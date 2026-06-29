# ==============================================================================
# 🧠 SISTEMA DE APRENDIZAJE — PASO 1: REGISTRO DE INTERACCIONES
# ==============================================================================
# La tabla 'ventas_aprendizaje' (y el resto de la arquitectura de aprendizaje:
# aprendizaje_global, insights_ia, calibracion_ia, cliente_memorias, ia_tasks...)
# ya existía en Supabase, con triggers e índices completos — pero nunca se
# conectó al flujo real del bot. Este archivo conecta la primera y más
# importante pieza: un registro de cada interacción, con los mismos datos que
# la IA YA calcula en cada respuesta.
#
# DECISIÓN DE DISEÑO CLAVE: esta función solo ESCRIBE — nunca se vuelve a leer
# dentro del prompt de Gemini. Eso significa que conectar esto NO le agrega
# ningún token ni latencia a la conversación en vivo. Se llama en paralelo
# (asyncio.gather) junto con actualizar_estado_crm y guardar_mensaje_chat, así
# que tampoco añade un viaje secuencial extra.
#
# Los campos de resultado de venta (cerro_venta, motivo_cierre, venta_monto,
# tiempo_cierre_horas, resultado_at) se dejan en NULL aquí a propósito — esos
# se llenarían DESPUÉS, cuando esa conversación específica termine en una
# venta real. Esa es una pieza futura (PASO 2+), no parte de esta conexión.
# ==============================================================================

import bleach
from typing import Optional
import config_and_schemas as config
from db_core_wrapper import async_db_execute, supabase

logger = config.logger

# Valores permitidos por las restricciones CHECK de la tabla — si la IA alguna
# vez devuelve algo fuera de esto (no debería, su propio esquema JSON ya lo
# limita), se cae a un valor seguro en vez de tronar el INSERT completo.
_EMOCIONES_VALIDAS = {"neutral", "feliz", "frustrado", "ansioso", "dudoso", "urgencia", "enojo", "duda", "entusiasmo"}
_TEMPERATURAS_VALIDAS = {"frio", "tibio", "caliente"}
_ACCIONES_TOOL_VALIDAS = {"ninguna", "aplicar_descuento"}


def _clamp01(valor, default: float = 0.0) -> float:
    try:
        return max(0.0, min(1.0, float(valor)))
    except (TypeError, ValueError):
        return default


def _texto_seguro(valor, limite: int) -> Optional[str]:
    txt = config.limpiar_texto(bleach.clean(str(valor or ""), tags=[], strip=True))[:limite]
    return txt or None


async def registrar_venta_aprendizaje(
    vendedor_id: str,
    telefono: str,
    mensaje_cliente: str,
    respuesta_ia: str,
    decision: dict,
    config_dict: dict = None,
    resumen_handoff: str = None,
) -> bool:
    """
    Registra una interacción del bot en 'ventas_aprendizaje'. Se diseñó para
    NUNCA tronar el pipeline del chat: cualquier error aquí se atrapa y se
    registra como advertencia — el cliente sigue recibiendo su respuesta
    normal sin importar si esto falla.
    """
    try:
        telefono = str(telefono or "").strip()
        vendedor_id = str(vendedor_id or "").strip()
        if not telefono or not vendedor_id or not isinstance(decision, dict):
            return False

        config_dict = config_dict or {}
        config_snapshot = {
            "negocio": str(config_dict.get("nombre_negocio", ""))[:200],
            "tono_ia": str(config_dict.get("tono_ia", ""))[:100],
            "giro": str(config_dict.get("giro", ""))[:100],
            "meta_venta": config_dict.get("meta_venta", 0),
        }

        perfil_actualizado = decision.get("perfil_actualizado")
        if not isinstance(perfil_actualizado, dict):
            perfil_actualizado = None

        try:
            lead_score = max(0, min(100, int(decision.get("lead_score", 0))))
        except (TypeError, ValueError):
            lead_score = 0

        emocion = str(decision.get("emocion_cliente", "")).strip().lower()
        if emocion not in _EMOCIONES_VALIDAS:
            emocion = None

        temperatura = str(decision.get("temperatura_lead", "")).strip().lower()
        if temperatura not in _TEMPERATURAS_VALIDAS:
            temperatura = None

        accion_tool = str(decision.get("accion_tool", "ninguna")).strip().lower()
        if accion_tool not in _ACCIONES_TOOL_VALIDAS:
            accion_tool = "ninguna"

        try:
            precio_oferta = max(0.0, float(decision.get("precio_oferta", 0) or 0))
        except (TypeError, ValueError):
            precio_oferta = 0.0

        payload = {
            "vendedor_id": vendedor_id,
            "telefono": telefono,
            "mensaje_cliente": _texto_seguro(mensaje_cliente, 5000) or "",
            "config_snapshot": config_snapshot,
            "respuesta_ia": _texto_seguro(respuesta_ia, 5000) or "",
            "intencion": _texto_seguro(decision.get("intencion"), 50),
            "lead_score": lead_score,
            "temperatura_lead": temperatura,
            "emocion_cliente": emocion,
            "objecion_detectada": _texto_seguro(decision.get("objecion_detectada"), 100),
            "producto_detectado": _texto_seguro(decision.get("producto_detectado"), 200),
            "categoria_preferida": _texto_seguro(decision.get("categoria_preferida"), 100),
            "cross_selling": _texto_seguro(decision.get("cross_selling"), 200),
            "upselling": _texto_seguro(decision.get("upselling"), 200),
            "probabilidad_cierre": _clamp01(decision.get("probabilidad_cierre", 0.0)),
            "precio_oferta": precio_oferta,
            "estrategia_venta": _texto_seguro(decision.get("estrategia_venta"), 100),
            "etapa_venta": _texto_seguro(decision.get("etapa_venta"), 50),
            "accion_tool": accion_tool,
            "sugerir_veltrix": bool(decision.get("sugerir_veltrix", False)),
            "requiere_seguimiento": bool(decision.get("requiere_seguimiento", False)),
            "tipo_seguimiento": _texto_seguro(decision.get("tipo_seguimiento"), 50),
            "nivel_prioridad": _texto_seguro(decision.get("nivel_prioridad"), 20),
            "confidence": _clamp01(decision.get("confidence", 0.0)),
            "perfil_actualizado": perfil_actualizado,
            "resumen_handoff": _texto_seguro(resumen_handoff, 3000) if resumen_handoff else None,
        }

        await async_db_execute(
            supabase.table("ventas_aprendizaje").insert(payload),
            timeout_seg=8.0,
            allow_retry=False,
        )
        return True

    except Exception as e:
        # 🛡️ No-crítico por diseño: el chat ya le respondió al cliente antes
        # de que esto corra. Un fallo aquí nunca debe verse del otro lado.
        logger.warning(f"⚠️ [APRENDIZAJE] No se pudo registrar interacción (no afecta el chat en vivo): {e}")
        return False
