# ==============================================================================
# 🚀 MÓDULO: db_api_endpoints.py (AAA ENTERPRISE GOLD STANDARD - COMPLETAMENTE CORREGIDO v2.8)
# ==============================================================================
# Godot 4.6 Ready • Orquestador IA, Auth B2B, CRM Base, Móvil e Inventario
# ==============================================================================

import asyncio, re, time, uuid, bleach, hashlib, jwt, os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
from cachetools import TTLCache

from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks, Header
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

# ==========================================================
# 🔌 IMPORTACIONES ESTRUCTURADAS (SSOT)
# ==========================================================
from config_and_schemas import (
    logger, get_lock, mensajes_procesados_meta, procesados_recientemente,
    JWT_SECRET, DUMMY_HASH, pwd_context, LoginUpdate, LeadAction, EstadoUpdate,
    BorrarRequest, NotasUpdate, NuevoArticulo, VentaItem,
    MobileMessageRequest, sanitizar_nombre_columna, ReordenarColumnasAction,
    ColumnaAction, RenombrarColumnaAction, BorrarColumnaAction, BotConfigUpdate, RESERVAS_TEMPORALES_ULTIMA_UNIDAD,
    RAWG_API_KEY, CambiarPasswordRequest
)
from ai_security_utils import verificar_sesion_b2b, get_http_client
from db_core_wrapper import async_db_execute, supabase
from db_chat import guardar_mensaje_chat, obtener_historial_chat
from db_crm_logic import actualizar_estado_crm

# CONSTANTE DE CRM GLOBAL
FILA_PAPELERA = "Papelera"

# 🔧 FIX ESTRUCTURA: bloques fijos compartidos. Antes vivían como listas locales duplicadas dentro de cargar_todo;
# ahora son la única fuente de verdad, usada también por /api/reordenar_columnas para validar que estos bloques
# nunca lleguen alterados desde el cliente.
COLUMNAS_FIJAS_IZQ = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
COLUMNAS_FIJAS_DER = ["Por Entregar", "Vendidos", FILA_PAPELERA]

# ⚠️ NOTA DE LIMPIEZA: existía una ruta /api/mover_prospecto duplicada (hacía exactamente lo mismo que
# /api/actualizar_estado, sin que nada en Godot la llamara) — se eliminó. /api/actualizar_estado es ahora la
# única ruta para mover una tarjeta de columna.

# ==========================================================
# 🛡️ HELPERS AAA, SCHEMAS Y CACHÉ
# ==========================================================
ventas_idempotencia_lock = asyncio.Lock()

# TTLCaches explícitos para evitar Memory Leaks y Bloqueos Infinitos
ventas_procesadas_idempotencia = TTLCache(maxsize=10000, ttl=86400)      # 24 Horas
LOGIN_RATE_LIMIT = TTLCache(maxsize=100000, ttl=300)                     # 5 Minutos
RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(maxsize=100000, ttl=60)            # 1 Minuto

class CampanaMasivaRequest(BaseModel):
    columna_origen: str
    mensaje: str

def now_ts() -> float: return time.time()
def limpiar_texto(texto: str) -> str: return bleach.clean(str(texto), tags=[], strip=True).strip()

def normalizar_telefono(tel: Any) -> str:
    if not tel: return ""
    limpio = re.sub(r"\D", "", str(tel))
    return limpio if len(limpio) >= 10 else ""

def _telefono_canonico_dedup(tel: Any) -> str:
    """Solo para agrupar/deduplicar prospectos duplicados por formato de teléfono. Colapsa variantes mexicanas
    con/sin el '1' extra que WhatsApp inserta tras el código de país 52 (ej. 524491142598 y 5214491142598 deben
    tratarse como el mismo cliente). No usar para escribir en BD."""
    limpio = re.sub(r"\D", "", str(tel or ""))
    if len(limpio) < 10: return ""
    if limpio.startswith("52") and not limpio.startswith("521") and len(limpio) == 12:
        limpio = "521" + limpio[2:]
    return limpio

def enmascarar_telefono(tel: str) -> str:
    if not tel or len(tel) < 6: return "unknown"
    return f"{tel[:4]}******{tel[-2:]}"

def obtener_trace_id(x_trace_id: Optional[str] = Header(None)) -> str:
    if x_trace_id:
        limpio = re.sub(r'[^a-zA-Z0-9_-]', '', str(x_trace_id))
        if limpio: return limpio[:64]
    return uuid.uuid4().hex[:16]

async def migrar_password_usuario(user_id: str, nuevo_hash: str):
    # FIX FASE 1: allow_retry=False por ser operación de mutación crítica (UPDATE)
    await async_db_execute(supabase.table('usuarios_veltrix').update({"password": nuevo_hash}).eq('id', user_id), allow_retry=False)

router = APIRouter()

# ==========================================================
# 🏢 CONFIGURACIÓN DINÁMICA DE TENANTS (reemplaza el diccionario hardcodeado
# CONFIGURACION_SHOWROOMS que vivía en main.py — ese venía marcado como
# "pendiente a migrar a Supabase" desde hace tiempo. Mientras siguiera ahí,
# dar de alta un giro nuevo (alarmas, terrenos, etc.) requería editar código
# y volver a desplegar; y aunque se hubiera dado de alta, los nombres de
# campo no coincidían con los que espera analizar_intencion_venta_ia
# (giro_comercial vs giro, objetivo_ventas_diario vs meta_venta,
# permitir_descuentos_ia vs permitir_desc, max_descuento_ia vs desc_max) —
# es decir, ni siquiera el tenant ya configurado recibía sus datos reales.
# ==========================================================
CONFIG_BOT_CACHE = TTLCache(maxsize=1000, ttl=120)  # 2 min: evita pegarle a Supabase en cada mensaje entrante

async def obtener_config_bot_por_phone_id(phone_number_id: str) -> Optional[dict]:
    """Busca la fila de configuracion_bot por meta_phone_id (columna única) y la
    traduce a las llaves exactas que espera analizar_intencion_venta_ia. Agregar
    un tenant nuevo ahora es una fila en Supabase, no una edición de código."""
    cacheado = CONFIG_BOT_CACHE.get(phone_number_id)
    if cacheado is not None:
        return cacheado
    try:
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_number_id).limit(1)),
            timeout=5.0
        )
        if not res.data:
            return None
        fila = res.data[0]
        config_dict = {
            "vendedor_id": fila.get("vendedor_id") or "",
            "nombre_negocio": fila.get("nombre_negocio") or "Veltrix Store",
            "giro": fila.get("giro") or "productos y servicios",
            "tono_ia": fila.get("tono_ia") or "Persuasivo, profesional y honesto",
            "meta_venta": fila.get("meta_venta_diaria") or 0,
            "permitir_desc": fila.get("permitir_descuento") if fila.get("permitir_descuento") is not None else True,
            "desc_max": fila.get("descuento_max_pct") or 0,
            "horario_atencion": fila.get("horario_atencion") or "",
            "link_pago": fila.get("link_pago") or "",
            "datos_pago_texto": fila.get("datos_pago_texto") or "",
            "texto_entrega": fila.get("texto_entrega") or "",
            "promo_veltrix_permitido": fila.get("promo_veltrix_permitido") if fila.get("promo_veltrix_permitido") is not None else False,
            "promo_veltrix_max_diario": fila.get("promo_veltrix_max_diario") or 5,
            # meta_token: si el tenant ya tiene su propio token en BD, se usa ese (necesario para
            # que cada negocio use SU PROPIO número de WhatsApp); si no, cae al env var global
            # (válido mientras solo exista un tenant real, como ahora).
            "meta_token": fila.get("meta_token") or os.getenv("WHATSAPP_TOKEN", "").strip(),
            "meta_phone_id": fila.get("meta_phone_id") or phone_number_id,
        }
        CONFIG_BOT_CACHE[phone_number_id] = config_dict
        return config_dict
    except Exception as e:
        logger.exception(f"❌ [CONFIG TENANT] Fallo consultando configuracion_bot para phone_id={phone_number_id}: {e}")
        return None

# ==========================================================
# ⏳ RESERVA TEMPORAL DE ÚLTIMA UNIDAD (Doberman ya NO descuenta inventario)
# ==========================================================
# El inventario real NUNCA se descuenta solo — eso lo hace un humano a mano
# en el Visor (botón "Vender", que ya manda la info a ventas/métricas
# correctamente). Lo único que se automatiza es evitar que el bot le
# prometa la ÚLTIMA unidad de un producto a dos clientes distintos mientras
# el primero completa su pago: si detecta intención de COMPRA sobre un
# producto con stock == 1, crea un "hold" de 1 hora. Mientras dure, CUALQUIER
# conversación ve ese producto como agotado (ver db_rag_scraper.py). Si pasa
# la hora sin que un humano lo haya descontado de verdad, expira solo y el
# inventario real vuelve a mandar.
async def evaluar_reserva_ultima_unidad(vendedor_id: str, nombre_producto: str, cliente: str, telefono: str) -> Optional[dict]:
    try:
        nombre_limpio = limpiar_texto(nombre_producto)[:120]
        if not nombre_limpio:
            return None

        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').select('id, nombre, stock')
                .ilike('nombre', f'%{nombre_limpio}%')
                .eq('vendedor_id', vendedor_id)
                .gt('stock', 0)
            ),
            timeout=8.0
        )
        candidatos = res.data or []
        # Misma cautela que antes: si hay ambigüedad (0 o 2+ coincidencias),
        # no se crea ningún hold — equivocarse de producto es peor que no
        # bloquearlo a tiempo.
        if len(candidatos) != 1 or int(candidatos[0].get('stock', 0)) != 1:
            return None

        item = candidatos[0]
        llave_reserva = f"{vendedor_id}:{item['id']}"
        if llave_reserva in RESERVAS_TEMPORALES_ULTIMA_UNIDAD:
            return None  # ya hay un hold activo para este artículo, no lo dupliquemos

        RESERVAS_TEMPORALES_ULTIMA_UNIDAD[llave_reserva] = {
            "cliente": cliente, "telefono": telefono, "ts": now_ts(), "nombre_item": item.get('nombre')
        }
        logger.info(f"⏳ [RESERVA] Última unidad de '{item.get('nombre')}' bloqueada 1h para {enmascarar_telefono(telefono)} (vendedor={vendedor_id}).")
        return {"nombre_item": item.get('nombre'), "id_item": item['id']}

    except Exception as e:
        logger.exception(f"❌ [RESERVA] Fallo evaluando reserva de última unidad: {e}")
        return None

# ==========================================================
# 🤖 0. ORQUESTADOR MAESTRO IA (FLATTENED & HARDENED)
# ==========================================================
async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict, media_dict: dict = None, id_mensaje_meta: str = None):
    from ai_gemini_core import analizar_intencion_venta_ia, validar_respuesta_ia, generar_resumen_handoff_ia
    from ai_security_utils import detectar_prompt_injection
    from ai_auditor_scraper import auditar_comprobante_ia
    from db_rag_scraper import obtener_contexto_inventario_rag
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async, disparar_whatsapp_imagen_async, enviar_alerta_whatsapp_admin

    trace_id = obtener_trace_id()
    inicio_pipeline = now_ts()
    cliente = limpiar_texto(str(cliente or "Cliente"))[:120]
    telefono = normalizar_telefono(telefono)
    texto_entrante = limpiar_texto(str(texto_entrante or ""))[:12000]
    vendedor_id = str(config.get("vendedor_id", "")).strip()
    if not telefono or not vendedor_id:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Inputs inválidos. Abortando pipeline multi-tenant.")
        return
    logger.info(f"🧠 [TRACE:{trace_id}] Inicio Pipeline IA | Tenant={vendedor_id} | Tel={enmascarar_telefono(telefono)}")

    try:
        if id_mensaje_meta:
            async with await get_lock(f"meta_{id_mensaje_meta}"):
                if id_mensaje_meta in mensajes_procesados_meta: return
                mensajes_procesados_meta[id_mensaje_meta] = True

        lock_hash = hashlib.sha256(f"{vendedor_id}:{telefono}".encode()).hexdigest()
        async with await get_lock(lock_hash):
            if detectar_prompt_injection(texto_entrante):
                logger.warning(f"🚨 [TRACE:{trace_id}] Injection detectada y bloqueada en tiempo real.")
                await disparar_whatsapp_dinamico_async(telefono, "Solicitud bloqueada por políticas de seguridad.", config.get("meta_token", ""), config.get("meta_phone_id", ""))
                return

            spam_hash = hashlib.sha256(f"{telefono}:{texto_entrante.lower()}".encode()).hexdigest()
            if spam_hash in procesados_recientemente: return
            procesados_recientemente[spam_hash] = True

            perfil_cliente_previo = {}
            try:
                res_p = await asyncio.wait_for(
                    async_db_execute(supabase.table('prospectos').select('perfil_psicologico').eq('telefono', telefono).eq('vendedor_id', vendedor_id).limit(1)),
                    timeout=5.0
                )
                if res_p.data: perfil_cliente_previo = res_p.data[0].get('perfil_psicologico', {}) or {}
            except Exception as e:
                logger.warning(f"⚠️ [TRACE:{trace_id}] Error consultando perfil: {e}")

            contexto_task = asyncio.create_task(obtener_contexto_inventario_rag(vendedor_id, texto_entrante))
            historial_task = asyncio.create_task(obtener_historial_chat(telefono, vendedor_id, limite=10))
            resultados = await asyncio.gather(contexto_task, historial_task, return_exceptions=True)
            contexto = resultados[0] if not isinstance(resultados[0], Exception) else ""
            historial = resultados[1] if not isinstance(resultados[1], Exception) else []

            # 🆕 AUDITORÍA DE COMPROBANTES (Doberman): ya existía construida, nunca estaba
            # conectada. Si llega una imagen, se analiza primero aquí con reglas estrictas
            # de antifraude (fecha, monto, señales de edición). Si el análisis sale bien,
            # se le pasa al cerebro de ventas como texto (fuente única de verdad) y se deja
            # de adjuntar la imagen cruda a esa llamada, para que no haya dos IAs opinando
            # cosas distintas sobre la misma imagen. Si la auditoría falla o tarda más de
            # 30s, se sigue exactamente el comportamiento anterior: la imagen cruda va
            # directo al cerebro de ventas, sin bloquear ni colgar el pipeline.
            #
            # 🛡️ FIX (hallazgo real): antes se descartaba la imagen cruda SIEMPRE que se
            # auditaba, sin importar el resultado. Si un cliente mandaba la portada de un
            # juego preguntando "¿tienes este?", el Doberman correctamente decía "esto no
            # es un comprobante" — pero la imagen YA se había descartado, así que el
            # cerebro de ventas nunca llegaba a VER la portada para identificar el juego.
            # Ahora solo se descarta la imagen cuando de verdad parece un intento de pago
            # (parece_comprobante=True), válido o no — si el propio Doberman dice que ni
            # siquiera se parece a un comprobante, se libera la imagen al cerebro de
            # ventas para que pueda usarla con su propósito real.
            analisis_comprobante = None
            media_dict_venta = media_dict
            if media_dict and str(media_dict.get("mime_type", "")).startswith("image/"):
                try:
                    analisis_comprobante = await asyncio.wait_for(
                        auditar_comprobante_ia(
                            b64_img_data=media_dict.get("data", b""),
                            mime_type=media_dict.get("mime_type", "image/jpeg"),
                            nombre_negocio=config.get("nombre_negocio", "Veltrix Store"),
                            historial_chat=historial,
                        ),
                        timeout=30.0
                    )
                    if bool(analisis_comprobante.get("parece_comprobante", True)):
                        media_dict_venta = None
                    else:
                        # No parece intento de pago (ej. portada de un producto) — se
                        # libera la imagen para que el cerebro de ventas la use, y el
                        # análisis del Doberman se descarta (no aporta nada aquí).
                        media_dict_venta = media_dict
                        analisis_comprobante = None
                except Exception as e:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Auditoría de comprobante falló, se sigue sin ella: {e}")
                    analisis_comprobante = None
                    media_dict_venta = media_dict

            # 🔧 FIX BUG ARGUMENTOS: faltaba 'telefono' — sin él, todo se recorría un lugar (telefono recibía el
            # dict de perfil, perfil_cliente_previo recibía media_dict, y el media_dict real nunca llegaba —
            # siempre None). Esto rompía la memoria persistente del cliente Y el análisis de imágenes/audios.
            # 🔧 FIX TIMEOUT: 25.0s afuera competía contra el reintento interno de consultar_gemini_json (2
            # intentos, hasta 26s cada uno ≈ 52s en el peor caso) — el de afuera casi siempre ganaba la carrera
            # y mataba el mecanismo de reintentos antes de que corriera ni un solo intento.
            decision = await asyncio.wait_for(
                analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, telefono, perfil_cliente_previo, media_dict_venta, analisis_comprobante),
                timeout=60.0
            )
            decision = validar_respuesta_ia(decision)
            respuesta_final = decision.get("respuesta", "Lo siento, tengo intermitencias en este momento.")
            producto_detectado = decision.get("producto_detectado", "")
            intencion_ia = decision.get("intencion", "HUMANO")
            perfil_actualizado = {
                **perfil_cliente_previo,
                "emocion_actual": decision.get("emocion_cliente"), "temperatura": decision.get("temperatura_lead"),
                "ultimo_interes": producto_detectado, "ultima_intencion": intencion_ia
            }

            # ⏳ RESERVA TEMPORAL DE ÚLTIMA UNIDAD: ver función arriba. Solo entra en
            # juego cuando la intención es COMPRA y queda exactamente 1 unidad — para
            # 2+ no se hace nada especial, sigue el flujo normal de abajo.
            reserva_resultado = None
            if intencion_ia == "COMPRA" and producto_detectado:
                reserva_resultado = await evaluar_reserva_ultima_unidad(vendedor_id, producto_detectado, cliente, telefono)

            # 🔧 FIX ILUMINACIÓN: el default era "blanco" (= ya leído), y se aplicaba
            # a CUALQUIER intención que no fuera exactamente una de las 4 de abajo
            # (cotización, regateo, saludo, pedido especial, mayoreo, pago recibido...).
            # Eso apagaba visualmente un mensaje que nadie había visto todavía — "ya
            # leído" es una decisión que le corresponde exclusivamente a Godot (vía
            # cache_leidos, cuando un humano abre el chat), nunca al backend. El
            # default correcto para cualquier mensaje nuevo sin clasificación especial
            # es "oro" (pendiente de revisar).
            nueva_columna, iluminacion = columna_actual, "oro"

            if reserva_resultado:
                # 🆕 Última unidad bloqueada temporalmente para este cliente — color y
                # columna distintos a todo lo demás porque hay una ventana de 1 hora
                # en la que esa pieza está "congelada" para que nadie más se la lleve.
                nueva_columna, iluminacion = "Vendidos", "rojo_prioridad"
                resumen = await generar_resumen_handoff_ia(cliente, "COMPRA", historial)
                aviso_admin = (
                    f"⏳ Solo queda 1 '{reserva_resultado.get('nombre_item')}' — bloqueado "
                    f"1 hora para este cliente mientras completa el pago. Actualiza el "
                    f"inventario en el Visor en cuanto confirmes que el dinero llegó.\n\n{resumen}"
                )
                await enviar_alerta_whatsapp_admin(cliente, telefono, "COMPRA", aviso_admin, config)
            elif intencion_ia == "INTERES_VELTRIX":
                # 🆕 Lead de venta cruzada para Veltrix Engine (no confundir con soporte de
                # Fantasy Games) — el bot ya le respondió la parte de info/demo solo, pero el
                # cierre real de una suscripción B2B se beneficia de seguimiento humano. Aviso
                # con encabezado propio para que nunca se mezcle a simple vista con quejas o
                # soporte normal de clientes de videojuegos.
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                aviso_admin = f"🚀 LEAD DE VELTRIX ENGINE (no es soporte de Fantasy Games) — {cliente} ({enmascarar_telefono(telefono)}) preguntó por el chatbot. El bot ya le dio info/demo; dale seguimiento para cerrar la suscripción."
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, aviso_admin, config)
            elif intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO", "PAGO_RECIBIDO"]:
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            elif intencion_ia == "COMPRA":
                nueva_columna, iluminacion = "Por Entregar", "verde_exito"

            resultados_gather = await asyncio.gather(
                actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, producto_detectado, perfil_ia=perfil_actualizado, nombre=cliente, mensaje=respuesta_final),
                guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final),
                return_exceptions=True
            )
            for r in resultados_gather:
                if isinstance(r, Exception): logger.error(f"❌ [TRACE:{trace_id}] Tarea asíncrona fallida en CRM/Chat: {r}")

            url_imagen = None
            if producto_detectado:
                try:
                    # 🛡️ FIX: antes la búsqueda exigía coincidencia literal exacta del
                    # texto que extrajo la IA contra el nombre real en inventario — una
                    # sola diferencia de puntuación (ej. "Batman: Arkham Knight" vs
                    # "Batman Arkham Knight", con/sin dos puntos) rompía el match por
                    # completo y el cliente se quedaba sin la foto aunque SÍ existiera.
                    # Se quita puntuación común antes de buscar, para ser más tolerante.
                    producto_normalizado = re.sub(r'[:\-,.]', ' ', producto_detectado)
                    producto_normalizado = re.sub(r'\s+', ' ', producto_normalizado).strip()
                    res_juego = await async_db_execute(
                        supabase.table('inventario').select('nombre, url_portada').ilike('nombre', f'%{producto_normalizado}%')
                        .eq('vendedor_id', vendedor_id).order('stock', desc=True).limit(1)
                    )
                    if res_juego.data and res_juego.data[0].get('url_portada'):
                        url_imagen = res_juego.data[0]['url_portada']
                    # 🆕 FIX: log de diagnóstico — antes, si esto fallaba, no había
                    # ninguna forma de saber por qué sin adivinar. Ahora queda visible
                    # en los logs de Render exactamente qué se buscó y qué se encontró.
                    logger.info(f"🖼️ [TRACE:{trace_id}] Búsqueda de portada — detectado='{producto_detectado}' normalizado='{producto_normalizado}' encontrado={'sí' if url_imagen else 'NO'}")
                except Exception as e:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Fallo al buscar portada: {e}")

            if url_imagen:
                await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))
            else:
                await disparar_whatsapp_dinamico_async(telefono, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))
            logger.info(f"✅ [TRACE:{trace_id}] Pipeline IA completado en {now_ts() - inicio_pipeline:.2f}s")

    except asyncio.TimeoutError:
        logger.error(f"⏱️ [TRACE:{trace_id}] Timeout global del Orquestador IA (Superó 60s).")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] CRÍTICO: Error en el pipeline IA: {e}")

# ==========================================================
# 📊 1. DASHBOARD STATS (TELEMETRÍA DE NEGOCIO VIA RPC)
# ==========================================================
@router.get("/stats")
async def get_dashboard_stats(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📊 [TRACE:{trace_id}] Solicitando Stats B2B (Vía RPC) para {vendedor_id}")
    try:
        # FIX FASE 1: allow_retry=False para ejecuciones RPC complejas
        res_rpc = await asyncio.wait_for(async_db_execute(supabase.rpc('get_tenant_stats', {'p_vendedor_id': vendedor_id}), allow_retry=False), timeout=5.0)
        stats_data = res_rpc.data[0] if res_rpc.data else {"total_leads": 0, "leads_nuevos": 0, "pendientes": 0}
        return {"status": "success", "data": stats_data}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout en base de datos al calcular stats.")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo crítico al procesar stats vía RPC: {e}")
        raise HTTPException(status_code=500, detail="Error recuperando stats nativos.")

# ==========================================================
# 📋 2. LISTADO DE LEADS
# ==========================================================
@router.get("/leads")
async def get_leads(fila: Optional[str] = None, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📋 [TRACE:{trace_id}] Solicitando Leads. Fila: {fila}")
    try:
        query = supabase.table("prospectos").select("*").eq("vendedor_id", vendedor_id)
        if fila: query = query.eq("fila", fila)
        res = await asyncio.wait_for(async_db_execute(query.order("ultima_interaccion_ia", desc=True).limit(50)), timeout=8.0)
        return {"status": "success", "data": res.data}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error recuperando leads: {e}")
        raise HTTPException(status_code=500, detail="Error recuperando leads.")

# ==========================================================
# ⚙️ 3. ACCIÓN MANUAL (CRM CONTROL)
# ==========================================================
@router.post("/leads/accion")
async def ejecutar_accion_lead(payload: LeadAction, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Acción Manual '{payload.accion}' sobre Lead {payload.lead_id}")
    try:
        res_check = await asyncio.wait_for(async_db_execute(supabase.table("prospectos").select("telefono").eq("id", payload.lead_id).eq("vendedor_id", vendedor_id)), timeout=5.0)
        if not res_check.data: raise HTTPException(status_code=404, detail="Lead no encontrado.")
        if payload.accion == "mover_fila":
            await actualizar_estado_crm(telefono=res_check.data[0]['telefono'], vendedor_id=vendedor_id, columna=payload.valor, iluminacion="blanco", juego="")
        return {"status": "success", "msg": "Acción ejecutada."}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error ejecutando acción: {e}")
        raise HTTPException(status_code=500, detail="Error ejecutando acción.")

# ==========================================================
# 🔐 9. AUTENTICACIÓN Y LOGIN B2B (AAA ENTERPRISE)
# ==========================================================
@router.post("/api/login")
async def login_b2b(datos: LoginUpdate, request: Request, background_tasks: BackgroundTasks, trace_id: str = Depends(obtener_trace_id)):
    ip_cliente = request.client.host if request.client else "127.0.0.1"
    email_normalizado = datos.email.lower().strip()
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email_normalizado):
        raise HTTPException(status_code=401, detail="Credenciales inválidas.")

    llave_limite = f"{ip_cliente}:{email_normalizado}"
    async with await get_lock(f"login_lock_{llave_limite}"):
        intentos_previos = LOGIN_RATE_LIMIT.get(llave_limite, 0)
        if intentos_previos >= 5:
            logger.warning(f"🚨 [TRACE:{trace_id}] IP bloqueada temporalmente: {llave_limite}")
            raise HTTPException(status_code=429, detail="Demasiados intentos. Intenta en 5 min.")

    logger.info(f"🔑 [TRACE:{trace_id}] Autenticando B2B: {email_normalizado} desde {ip_cliente}")
    try:
        res = await asyncio.wait_for(async_db_execute(supabase.table('usuarios_veltrix').select('*').eq('email', email_normalizado).limit(1)), timeout=10.0)
        usuario_existe = bool(res.data)
        usuario = res.data[0] if usuario_existe else {}
        password_guardada = str(usuario.get('password', DUMMY_HASH))
        password_valida, es_legacy = False, False

        if usuario_existe:
            if password_guardada.startswith('$2b$'):
                password_valida = await run_in_threadpool(pwd_context.verify, datos.password, password_guardada)
            else:
                es_legacy = True
                password_valida = (datos.password == password_guardada)
        else:
            await run_in_threadpool(pwd_context.verify, datos.password, DUMMY_HASH)

        if not password_valida:
            async with await get_lock(f"login_lock_{llave_limite}"):
                LOGIN_RATE_LIMIT[llave_limite] = LOGIN_RATE_LIMIT.get(llave_limite, 0) + 1
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")

        if es_legacy:
            logger.warning(f"🚨 [TRACE:{trace_id}] Migrando cuenta legacy: {email_normalizado}")
            nuevo_hash = await run_in_threadpool(pwd_context.hash, datos.password)
            background_tasks.add_task(migrar_password_usuario, usuario['id'], nuevo_hash)

        if str(usuario.get('estado', '')).lower().strip() != 'activo':
            raise HTTPException(status_code=401, detail="Cuenta no activa.")
        if not usuario.get('suscripcion_activa', False):
            raise HTTPException(status_code=402, detail="Suscripción inactiva.")

        async with await get_lock(f"login_lock_{llave_limite}"):
            LOGIN_RATE_LIMIT.pop(llave_limite, None)

        # FIX FASE 4: CRÍTICO MULTI-TENANT. Rompemos el fallback por defecto a 'V-001'.
        vendedor_id = usuario.get('vendedor_id')
        if not vendedor_id or not str(vendedor_id).strip():
            logger.critical(f"🚨 [SECURITY ALERT] El usuario {email_normalizado} carece de vendedor_id. Bloqueando acceso instantáneamente.")
            raise HTTPException(status_code=500, detail="Estructura de aislamiento corrupta. Contacte soporte.")

        vendedor_id = str(vendedor_id).strip()
        # 🆕 Se manda el giro real del tenant en el login — el Visor lo
        # necesita para saber si debe mostrar las listas/etiquetas de
        # videojuegos o el modo genérico para cualquier otro giro. Se
        # consulta una sola vez aquí, en vez de cada vez que se abre el
        # Visor — ya existe esta misma consulta en /api/descargar_plantilla,
        # se reusa el mismo patrón.
        giro_final = "videojuegos"
        try:
            res_giro = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('giro').eq('vendedor_id', vendedor_id).limit(1)), timeout=5.0)
            giro_crudo = res_giro.data[0].get('giro') if res_giro.data else None
            # 🛡️ FIX: alineado con /api/descargar_plantilla — ahí mismo, un
            # giro vacío/sin configurar cae a "videojuegos" por compatibilidad
            # con clientes existentes (todos eran de ese giro antes de esta
            # función). Usar un default distinto aquí ("general") haría que
            # el mismo tenant viera el Visor en modo genérico pero la
            # plantilla CSV en modo videojuegos — inconsistente.
            giro_str = str(giro_crudo or '').lower().strip()
            giro_final = giro_str if giro_str != "" else "videojuegos"
            if giro_str == "":
                # 🆕 Aviso visible — si un cliente nunca tuvo su giro
                # configurado, esto se cae a "videojuegos" por compatibilidad
                # silenciosa, pero conviene que quede una pista clara en los
                # logs en vez de que nadie se entere hasta que el cliente
                # se queje de ver términos de videojuegos sin venir al caso.
                logger.warning(f"⚠️ [TRACE:{trace_id}] El tenant {vendedor_id} no tiene 'giro' configurado en configuracion_bot — usando 'videojuegos' por default. Si NO es un cliente de videojuegos, hace falta configurarlo.")
        except Exception as e:
            logger.warning(f"⚠️ [TRACE:{trace_id}] No se pudo obtener el giro al hacer login (no bloqueante): {e}")
        ahora = datetime.now(timezone.utc)
        # FIX FASE 3: Modificado algoritmo a HS512 para total paridad con el validador central de seguridad
        token_jwt = jwt.encode({
            "sub": vendedor_id, "email": usuario['email'], "jti": str(uuid.uuid4()),
            "iss": "veltrix-engine", "aud": "veltrix-clients",
            "iat": ahora, "nbf": ahora, "exp": ahora + timedelta(hours=8)
        }, JWT_SECRET, algorithm="HS512")

        logger.info(f"✅ [TRACE:{trace_id}] Tenant [{vendedor_id}] verificado con éxito. Despachando token HS512 hacia Godot 4.6.")
        return {
            "status": "ok", "access_token": token_jwt, "token_type": "bearer",
            "datos": {"vendedor_id": vendedor_id, "email": usuario['email'], "nombre": usuario.get('nombre_contacto', 'Vendedor'), "rol": usuario.get('rol', 'vendedor'), "giro": giro_final}
        }
    except HTTPException: raise
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Servicio temporalmente no disponible.")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error CRÍTICO en login: {e}")
        raise HTTPException(status_code=500, detail="Error interno de autenticación.")

# ==========================================================
# 🌐 10. RUTAS CRM (CARGA Y ACTUALIZACIÓN B2B)
# ==========================================================
@router.get("/api/cargar_todo")
async def cargar_todo(limit: int = 200, offset: int = 0, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Sincronizando Tablero Kanban (Modo Ligero).")
    try:
        offset_seguro, limit_seguro = max(0, offset), min(limit, 300)
        columnas_izq, columnas_der = COLUMNAS_FIJAS_IZQ, COLUMNAS_FIJAS_DER

        # 🔧 FIX ORDEN: se agrega 'orden' al select y se ordena por él, para que un reordenamiento guardado vía
        # /api/reordenar_columnas se refleje aquí al recargar (antes no existía ningún ORDER BY, el orden visible
        # dependía por accidente del id de inserción).
        res_cols = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').select('nombre_columna, orden').eq('vendedor_id', str(_sesion)).order('orden')),
            timeout=10.0
        )
        columnas_custom = [
            sanitizar_nombre_columna(r['nombre_columna']) for r in (res_cols.data or [])
            if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)]
        ]

        res_prospectos = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').select('id, nombre, telefono, fila, ultima_interaccion_ia, ultimo_msj, estado_iluminacion')
                .eq('vendedor_id', str(_sesion)).order('ultima_interaccion_ia', desc=True).range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=12.0
        )

        ultimos = {}
        for registro in (res_prospectos.data or []):
            # 🔧 FIX DUPLICADOS: usamos la clave canónica (colapsa variantes mexicanas con/sin el "1") en vez de
            # normalizar_telefono crudo, para que un mismo cliente con dos formatos de teléfono en BD nunca
            # aparezca como dos prospectos peleándose por la misma tarjeta en Godot. Como la consulta ya viene
            # ordenada por actividad más reciente primero, la primera fila que gane esta clave es siempre la
            # más reciente/activa.
            key_identificador = _telefono_canonico_dedup(registro.get('telefono', '')) or str(registro.get('id', ''))
            if key_identificador not in ultimos:
                registro["ultimo_msj"] = bleach.clean(str(registro.get("ultimo_msj") or ""), tags=[], strip=True)
                ultimos[key_identificador] = registro

        # 🔬 DIAGNÓSTICO TEMPORAL: imprime el teléfono CRUDO (repr, sin transformar) de cada prospecto justo
        # antes de devolverlo, para confirmar de una vez por todas si cargar_todo ya manda el valor mal, o si
        # se daña después, del lado de Godot. Quitar una vez resuelto.
        for _reg in ultimos.values():
            logger.info(f"🔬 [DIAG TELEFONO] nombre={_reg.get('nombre')!r} telefono_crudo={_reg.get('telefono')!r} tipo={type(_reg.get('telefono')).__name__}")

        return {"columnas": columnas_izq + columnas_custom + columnas_der, "prospectos": list(ultimos.values())}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en cargar_todo: {e}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar tarjetas del embudo.")

@router.get("/api/perfil_cliente")
async def obtener_perfil_cliente(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Consultando perfil profundo de {enmascarar_telefono(telefono)}")
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Parámetro telefónico inválido.")
        res = await asyncio.wait_for(
            async_db_execute(supabase.table("prospectos").select("id, notas, etiquetas, fila, perfil_psicologico").eq("telefono", tel_norm).eq("vendedor_id", str(vendedor_id)).limit(1)),
            timeout=5.0
        )
        return {"status": "ok", "datos": res.data[0] if res.data else {}}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en perfil_cliente: {e}")
        raise HTTPException(status_code=500, detail="Fallo interno en consulta de perfil.")

@router.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Obteniendo Layout de Columnas")
    try:
        # 🔧 FIX ORDEN: igual que en cargar_todo, se ordena por 'orden'.
        res = await asyncio.wait_for(async_db_execute(supabase.table("configuracion").select("nombre_columna, orden").eq("vendedor_id", str(vendedor_id)).order('orden')), timeout=5.0)
        return {"status": "ok", "columnas": [sanitizar_nombre_columna(item["nombre_columna"]) for item in (res.data or [])]}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo recuperando columnas: {e}")
        raise HTTPException(status_code=500, detail="Error al solicitar columnas configuradas.")

@router.post("/api/reordenar_columnas")
async def reordenar_columnas(datos: ReordenarColumnasAction, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    """Guarda el nuevo orden de las columnas dinámicas (incluye "+"). Blindaje no negociable: los bloques fijos
    de izquierda y derecha deben llegar EXACTOS en su posición obligatoria, o se rechaza con 400 sin guardar
    nada. Esta ruta nunca toca la tabla 'prospectos' — solo 'configuracion.orden' — así que no existe forma de
    que mueva tarjetas."""
    logger.info(f"🎮 [TRACE:{trace_id}] Guardando nuevo orden de columnas para {vendedor_id}")
    try:
        columnas = datos.columnas
        n_izq, n_der = len(COLUMNAS_FIJAS_IZQ), len(COLUMNAS_FIJAS_DER)
        if len(columnas) < n_izq + n_der:
            raise HTTPException(status_code=400, detail="Estructura de columnas inválida: faltan columnas obligatorias.")

        bloque_izq = [c.upper() for c in columnas[:n_izq]]
        if bloque_izq != [c.upper() for c in COLUMNAS_FIJAS_IZQ]:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Intento de alterar bloque fijo izquierdo rechazado. Tenant={vendedor_id}")
            raise HTTPException(status_code=400, detail="Las columnas fijas de la izquierda no están en su posición obligatoria.")

        bloque_der = [c.upper() for c in columnas[-n_der:]]
        if bloque_der != [c.upper() for c in COLUMNAS_FIJAS_DER]:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Intento de alterar bloque fijo derecho rechazado. Tenant={vendedor_id}")
            raise HTTPException(status_code=400, detail="Las columnas fijas de la derecha no están en su posición obligatoria.")

        zona_dinamica = columnas[n_izq:-n_der]  # incluye "+" siempre, más cualquier columna real creada por el usuario

        # Persistimos el orden completo: fijas (siempre igual) + dinámicas (lo que de verdad cambió).
        # FIX FASE 1 (mismo patrón del resto del archivo): allow_retry=False por ser mutación.
        for idx, nombre_col in enumerate(COLUMNAS_FIJAS_IZQ):
            await asyncio.wait_for(
                async_db_execute(supabase.table("configuracion").update({"orden": idx}).eq("vendedor_id", str(vendedor_id)).ilike("nombre_columna", nombre_col), allow_retry=False),
                timeout=8.0
            )
        for idx, nombre_col in enumerate(zona_dinamica):
            nombre_seguro = sanitizar_nombre_columna(nombre_col, permitir_reservadas=True)
            await asyncio.wait_for(
                async_db_execute(supabase.table("configuracion").update({"orden": n_izq + idx}).eq("vendedor_id", str(vendedor_id)).eq("nombre_columna", nombre_seguro), allow_retry=False),
                timeout=8.0
            )
        for idx, nombre_col in enumerate(COLUMNAS_FIJAS_DER):
            await asyncio.wait_for(
                async_db_execute(supabase.table("configuracion").update({"orden": n_izq + len(zona_dinamica) + idx}).eq("vendedor_id", str(vendedor_id)).ilike("nombre_columna", nombre_col), allow_retry=False),
                timeout=8.0
            )

        logger.info(f"✅ [TRACE:{trace_id}] Orden de columnas guardado para {vendedor_id}")
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error guardando orden de columnas: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar el nuevo orden de columnas.")

# ==========================================================
# 🏗️ 10B. CREACIÓN Y RENOMBRADO DE COLUMNAS DINÁMICAS
# Godot ya llamaba a estas dos rutas (al renombrar el "+" inicial, y al
# renombrar cualquier otra columna dinámica) pero nunca existieron en el
# backend — por eso /api/crear_columna tiraba error. Sin /api/crear_columna,
# además, una columna nueva nunca llegaba a existir como fila real en
# 'configuracion': /api/reordenar_columnas solo hace UPDATE, así que sobre
# una columna que nunca se insertó, ese UPDATE no encuentra nada y no hace
# nada — la columna se ve bien en la sesión actual pero no sobrevive a un
# refresco, porque nunca se guardó de verdad.
# ==========================================================
@router.post("/api/crear_columna")
async def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Creando columna dinámica '{datos.nombre}' para {_sesion}")
    try:
        nombre_seguro = sanitizar_nombre_columna(datos.nombre, permitir_reservadas=True)
        existente = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').select('id').eq('vendedor_id', str(_sesion)).eq('nombre_columna', nombre_seguro).limit(1)),
            timeout=5.0
        )
        if existente.data:
            return {"status": "ok", "msg": "La columna ya existía."}
        # FIX FASE 1: allow_retry=False por mutación (INSERT)
        await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').insert({"nombre_columna": nombre_seguro, "vendedor_id": str(_sesion), "orden": 999}), allow_retry=False),
            timeout=8.0
        )
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo creando columna: {e}")
        raise HTTPException(status_code=500, detail="Error al crear la columna.")

@router.post("/api/renombrar_columna")
async def renombrar_columna(datos: RenombrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Renombrando columna '{datos.viejo_nombre}' -> '{datos.nuevo_nombre}' para {_sesion}")
    try:
        viejo_seguro = sanitizar_nombre_columna(datos.viejo_nombre, permitir_reservadas=True)
        nuevo_seguro = sanitizar_nombre_columna(datos.nuevo_nombre, permitir_reservadas=True)
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').update({"nombre_columna": nuevo_seguro}).eq('vendedor_id', str(_sesion)).eq('nombre_columna', viejo_seguro), allow_retry=False),
            timeout=8.0
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Columna original no encontrada.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo renombrando columna: {e}")
        raise HTTPException(status_code=500, detail="Error al renombrar la columna.")

# ==========================================================
# 🗑️ 10C. BORRADO DE COLUMNAS DINÁMICAS
# Godot ya llamaba a esta ruta (_borrar_columna_en_nube) pero nunca existió
# en el backend — cualquier intento de borrar una columna fallaba en
# silencio (el callback de Godot ni siquiera revisaba el código de
# respuesta). Como la fila nunca se borraba de 'configuracion', el siguiente
# ciclo de sync la recreaba de inmediato, dando la sensación de que "ya ni
# siquiera se borran".
# ==========================================================
@router.post("/api/borrar_columna")
async def borrar_columna(datos: BorrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Borrando columna '{datos.nombre_columna}' para {_sesion}")
    try:
        nombre_seguro = sanitizar_nombre_columna(datos.nombre_columna, permitir_reservadas=True)

        # 🛡️ Blindaje 1: las columnas fijas (izquierda/derecha) nunca se pueden
        # borrar — Godot ya bloquea esto visualmente (son columnas sin botón de
        # borrar), pero esto es la fuente de verdad real, no solo confianza en
        # el cliente.
        columnas_fijas_upper = [c.upper() for c in (COLUMNAS_FIJAS_IZQ + COLUMNAS_FIJAS_DER)]
        if nombre_seguro.upper() in columnas_fijas_upper:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Intento de borrar columna fija '{nombre_seguro}' rechazado.")
            raise HTTPException(status_code=400, detail="Las columnas fijas del sistema no se pueden eliminar.")

        # 🛡️ Blindaje 2: igual que Godot ya valida visualmente, pero verificado
        # aquí también — si hay prospectos en esta columna, no se borra. Esto
        # cubre la carrera donde una tarjeta se mueve a la columna justo entre
        # la validación visual de Godot y esta petición.
        res_check = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').select('id').eq('vendedor_id', str(_sesion)).eq('fila', nombre_seguro).limit(1)),
            timeout=5.0
        )
        if res_check.data:
            raise HTTPException(status_code=400, detail="La columna tiene prospectos asignados. Muévelos antes de eliminarla.")

        # FIX FASE 1: allow_retry=False por mutación crítica (DELETE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').delete().eq('vendedor_id', str(_sesion)).eq('nombre_columna', nombre_seguro), allow_retry=False),
            timeout=8.0
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Columna no encontrada.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo borrando columna: {e}")
        raise HTTPException(status_code=500, detail="Error al eliminar la columna.")

# ==========================================================
# 📱 11. ENDPOINTS MÓVILES (APP ASESORES Y GODOT)
# ==========================================================
@router.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, limit: int = 50, offset: int = 0, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Solicitando historial móvil para Tel={enmascarar_telefono(telefono)}")
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: return {"status": "ok", "historial": []}
        offset_seguro, limit_seguro = max(0, offset), min(limit, 100)
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("mensajes_chat").select("mensaje, autor, created_at, wamid").eq("vendedor_id", str(vendedor_id)).eq("telefono", tel_norm)
                .order("created_at", desc=True).range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=8.0
        )
        historial_formateado = [
            {
                "contenido": bleach.clean(str(m.get("mensaje") or ""), tags=[], strip=True),
                "es_mio": str(m.get("autor", "")).upper() in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"],
                "fecha": str(m.get("created_at", "")),
                # 🆕 FIX: se regresa el mismo id que Mobile mandó al enviar (si
                # lo mandó) — sin esto, Mobile nunca podía reconocer su propia
                # burbuja optimista y el mensaje se duplicaba en pantalla.
                "client_msg_id": str(m.get("wamid") or "")
            } for m in reversed(res.data or [])
        ]
        # 🛡️ FIX #1 (segunda parte — tras reinicio completo de la app): el
        # tablero (/api/cargar_todo) es "Modo Ligero" y NUNCA incluye
        # notas/etiquetas — eso significa que toda tarjeta CREADA DE CERO
        # (siempre que se reinicia Veltrix PC) arranca con esos campos
        # vacíos en memoria, sin importar lo que ya esté guardado en
        # Supabase. El primer fix solo evitó que un refresco periódico
        # BORRARA un valor ya cargado — pero nunca le dio al chat una forma
        # de cargar el valor real si nunca llegó a existir en memoria. Como
        # el chat YA llama a este endpoint al abrir, se le agrega el perfil
        # completo aquí — así el chat siempre tiene el dato correcto sin
        # depender de qué tan fresca esté la tarjeta del tablero.
        res_perfil = await asyncio.wait_for(
            async_db_execute(supabase.table("prospectos").select("notas, etiquetas, fila").eq("vendedor_id", str(vendedor_id)).eq("telefono", tel_norm).limit(1)),
            timeout=5.0
        )
        perfil = res_perfil.data[0] if res_perfil.data else {}
        return {
            "status": "ok", "historial": historial_formateado,
            "notas": str(perfil.get("notas") or ""),
            "etiquetas": str(perfil.get("etiquetas") or ""),
            "fila": str(perfil.get("fila") or "Bandeja Nueva")
        }
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error recuperando chat_history móvil: {e}")
        raise HTTPException(status_code=500, detail="Error al recuperar logs de conversación.")

@router.post("/api/mobile/send_message")
async def send_mobile_message(data: MobileMessageRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async
    tel_norm = normalizar_telefono(data.to)
    mensaje_limpio = str(data.msg).strip()
    logger.info(f"🎮 [TRACE:{trace_id}] Handoff humano: Despachando mensaje saliente hacia {enmascarar_telefono(tel_norm)}")
    if not tel_norm or not mensaje_limpio: raise HTTPException(status_code=400, detail="Datos incompletos.")
    if len(mensaje_limpio) > 4096: raise HTTPException(status_code=413, detail="Mensaje demasiado largo.")

    llave_outbound = f"{vendedor_id}:{tel_norm}"
    async with await get_lock(f"mobile_outbound_{llave_outbound}"):
        envios_recientes = RATE_LIMIT_MOBILE_OUTBOUND.get(llave_outbound, 0)
        if envios_recientes > 10:
            logger.warning(f"🚨 [TRACE:{trace_id}] Límite outbound excedido para {llave_outbound}")
            raise HTTPException(status_code=429, detail="Límite masivo excedido. Espera un momento.")
        RATE_LIMIT_MOBILE_OUTBOUND[llave_outbound] = envios_recientes + 1

    try:
        res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', str(vendedor_id)).limit(1)), timeout=5.0)
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración de Meta no encontrada en este tenant.")
        config = res_conf.data[0]

        # FIX FASE 4: TRAZABILIDAD ANTES QUE EFECTO SECUNDARIO. Guardamos en base de datos primero para
        # garantizar auditoría; si la API externa cae o responde lento, el mensaje humano no queda huérfano.
        # 🆕 FIX: se reusa el mismo campo 'wamid' que ya existe en mensajes_chat
        # (sirve como "id de mensaje externo" sin importar si viene de Meta o
        # de Mobile) — esto es lo que permite que el historial pueda regresarle
        # a Mobile el mismo client_msg_id que mandó, para que reconozca su
        # propia burbuja optimista y no la duplique.
        await guardar_mensaje_chat(tel_norm, str(vendedor_id), 'ASESOR', mensaje_limpio, wamid=data.client_msg_id)
        await actualizar_estado_crm(tel_norm, str(vendedor_id), "En Conversacion", "azul", "", mensaje=mensaje_limpio)

        # Efecto secundario (Llamada HTTP externa a la API de Meta)
        await disparar_whatsapp_dinamico_async(tel_norm, mensaje_limpio, config.get('meta_token'), config.get('meta_phone_id'))
        return {"status": "ok", "message": "Enviado"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error retransmitiendo handoff manual: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al despachar el mensaje.")

# ==========================================================
# 📷 11B. ENVÍO DE FOTOS DESDE MOBILE (CÁMARA / GALERÍA)
# Esta ruta nunca existió, ni antes ni después de la migración — Mobile ya
# mandaba fotos en base64 a /api/mobile/send_media, pero el backend no tenía
# nada ahí. disparar_whatsapp_imagen_async (ya existente) solo manda por URL,
# no por base64 crudo — así que se sube la foto a Supabase Storage (mismo
# bucket que ya usa el inventario) para conseguir una URL, y de ahí se reusa
# esa función tal cual, sin tocar nada del lado de Meta.
# ==========================================================
class MobileMediaRequest(BaseModel):
    to: str
    image_base64: str = Field(min_length=1)
    vendedor_id: str = ""

@router.post("/api/mobile/send_media")
async def send_mobile_media(data: MobileMediaRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    import base64, io
    from PIL import Image
    from ai_whatsapp_media import disparar_whatsapp_imagen_async

    tel_norm = normalizar_telefono(data.to)
    logger.info(f"📷 [TRACE:{trace_id}] Foto desde Mobile hacia {enmascarar_telefono(tel_norm)}")
    if not tel_norm: raise HTTPException(status_code=400, detail="Teléfono inválido.")

    try:
        img_bytes = base64.b64decode(data.image_base64, validate=False)
    except Exception:
        raise HTTPException(status_code=400, detail="Imagen en base64 inválida.")
    if len(img_bytes) < 32 or len(img_bytes) > 8_000_000:
        raise HTTPException(status_code=413, detail="Imagen vacía o demasiado grande.")

    try:
        # Misma validación estricta que el resto del proyecto: verificar +
        # decodificar por completo antes de confiar en los bytes recibidos.
        img_verificar = Image.open(io.BytesIO(img_bytes))
        img_verificar.verify()
        img = Image.open(io.BytesIO(img_bytes))
        img.load()
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        buffer_salida = io.BytesIO()
        img.save(buffer_salida, format="JPEG", quality=80)
        bytes_finales = buffer_salida.getvalue()
    except Exception as e:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Imagen de Mobile corrupta o ilegible: {e}")
        raise HTTPException(status_code=400, detail="La imagen está corrupta o no se pudo procesar.")

    try:
        nombre_archivo = f"chat/chat_{_sesion}_{tel_norm}_{int(now_ts())}.jpg"
        # 🛡️ FIX URGENTE: "inventario_media" nunca existió como bucket real
        # en Supabase Storage — los buckets reales son "portadas" (para
        # fotos de producto/portadas de juego) y "multimedia" (para esto:
        # imágenes mandadas dentro del chat). Toda subida hacia
        # "inventario_media" fallaría con "Bucket not found" en producción.
        def _upload():
            return supabase.storage.from_("multimedia").upload(
                nombre_archivo, bytes_finales, file_options={"content-type": "image/jpeg", "upsert": "true"}
            )
        await asyncio.wait_for(asyncio.to_thread(_upload), timeout=15.0)
        url_publica = supabase.storage.from_("multimedia").get_public_url(nombre_archivo)
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo subiendo foto de Mobile a Storage: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar la imagen.")

    res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
    if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración de Meta no encontrada en este tenant.")
    config_tenant = res_conf.data[0]

    exito = await disparar_whatsapp_imagen_async(tel_norm, url_publica, "", config_tenant.get('meta_token'), config_tenant.get('meta_phone_id'))
    if not exito:
        raise HTTPException(status_code=500, detail="La imagen se guardó pero falló el envío por WhatsApp.")

    await guardar_mensaje_chat(tel_norm, str(_sesion), 'ASESOR', "[Imagen enviada desde Mobile]")
    return {"status": "ok"}

# ==========================================================
# 📷 11C. FOTO DE PRODUCTO (MANUAL — CUALQUIER GIRO)
# Antes la ÚNICA forma de poner url_portada era el flujo de RAWG en
# PanelVideojuegos (búsqueda automática de portadas de videojuegos) — sin
# equivalente para ningún otro giro. Esta ruta permite subir una foto
# directo desde el Visor y asociarla al producto, sin importar el giro.
# Reusa la misma validación/redimensión de imagen que /api/mobile/send_media,
# pero sin disparar nada por WhatsApp — solo sube y guarda la URL.
# ==========================================================
class SubirFotoProductoRequest(BaseModel):
    item_id: int
    image_base64: str = Field(min_length=1)

@router.post("/api/subir_foto_producto")
async def subir_foto_producto(data: SubirFotoProductoRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    import base64, io
    from PIL import Image

    logger.info(f"📷 [TRACE:{trace_id}] Subiendo foto manual para producto {data.item_id} de {_sesion}")

    # 🛡️ Se valida que el producto exista Y pertenezca a este tenant ANTES
    # de subir nada a Storage — evita gastar el upload si el item_id es
    # inválido o de otro vendedor.
    res_check = await asyncio.wait_for(async_db_execute(supabase.table('inventario').select('id').eq('id', data.item_id).eq('vendedor_id', str(_sesion)).limit(1)), timeout=10.0)
    if not res_check.data:
        raise HTTPException(status_code=404, detail="Producto no encontrado o no pertenece a tu cuenta.")

    try:
        img_bytes = base64.b64decode(data.image_base64, validate=False)
    except Exception:
        raise HTTPException(status_code=400, detail="Imagen en base64 inválida.")
    if len(img_bytes) < 32 or len(img_bytes) > 8_000_000:
        raise HTTPException(status_code=413, detail="Imagen vacía o demasiado grande.")

    try:
        img_verificar = Image.open(io.BytesIO(img_bytes))
        img_verificar.verify()
        img = Image.open(io.BytesIO(img_bytes))
        img.load()
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        buffer_salida = io.BytesIO()
        img.save(buffer_salida, format="JPEG", quality=80)
        bytes_finales = buffer_salida.getvalue()
    except Exception as e:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Imagen de producto corrupta o ilegible: {e}")
        raise HTTPException(status_code=400, detail="La imagen está corrupta o no se pudo procesar.")

    try:
        nombre_archivo = f"portadas/producto_{_sesion}_{data.item_id}_{int(now_ts())}.jpg"
        # 🛡️ FIX URGENTE: "inventario_media" nunca existió como bucket real —
        # los buckets reales en Supabase Storage son "portadas" y
        # "multimedia" (confirmado por captura). Toda subida hacia
        # "inventario_media" fallaría con "Bucket not found" en producción.
        def _upload():
            return supabase.storage.from_("portadas").upload(
                nombre_archivo, bytes_finales, file_options={"content-type": "image/jpeg", "upsert": "true"}
            )
        await asyncio.wait_for(asyncio.to_thread(_upload), timeout=15.0)
        url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo subiendo foto de producto a Storage: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar la imagen.")

    try:
        await asyncio.wait_for(async_db_execute(supabase.table('inventario').update({'url_portada': url_publica}).eq('id', data.item_id).eq('vendedor_id', str(_sesion)), allow_retry=False), timeout=10.0)
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Foto subida pero no se pudo asociar al producto: {e}")
        raise HTTPException(status_code=500, detail="La imagen se subió pero no se pudo guardar en el producto. Intenta de nuevo.")

    logger.info(f"✅ [TRACE:{trace_id}] Foto de producto {data.item_id} actualizada con éxito.")
    return {"status": "ok", "url_portada": url_publica}

@router.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Compilando Dashboard Móvil para {vendedor_id}")
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        ventas_res = await asyncio.wait_for(async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio)), timeout=10.0)
        total_hoy = sum((float(v.get("monto") or 0.0) for v in (ventas_res.data or [])))

        prospectos_res = await asyncio.wait_for(
            async_db_execute(
                # 🛡️ FIX REAL: faltaba 'estado_iluminacion' — Mobile nunca
                # recibía este campo, así que no tenía forma de saber si un
                # prospecto pedía atención humana, tenía oferta especial,
                # etc. Por eso un cliente podía verse en "Requiere Asistencia"
                # en la PC y en "Bandeja Nueva" en Mobile al mismo tiempo —
                # cada plataforma estaba calculando su propio estado por su
                # cuenta, en vez de usar la misma verdad del servidor.
                supabase.table("prospectos").select("id, nombre, telefono, fila, ultima_interaccion_ia, ultimo_msj, estado_iluminacion")
                .eq("vendedor_id", str(vendedor_id)).order("ultima_interaccion_ia", desc=True).limit(50)
            ),
            timeout=8.0
        )
        prospectos_limpios = [
            {
                "id": p.get("id"), "nombre": bleach.clean(p.get("nombre") or "Cliente", tags=[], strip=True),
                "telefono": normalizar_telefono(p.get("telefono", "")),
                # 🛡️ FIX REAL (encontrado en logs de producción): esta función
                # existe para evitar que alguien CREE una columna nueva con el
                # mismo nombre que una columna fija — pero aquí solo se está
                # MOSTRANDO un valor que ya es legítimamente una columna fija
                # (Bandeja Nueva, Requiere Asistencia, etc.). Sin
                # permitir_reservadas=True, CUALQUIER prospecto en "Requiere
                # Asistencia", "Por Entregar" o "Envíos Masivos" se forzaba a
                # "Bandeja Nueva" SOLO en Mobile — exactamente la causa del
                # mismatch real entre PC y Mobile que se vio en producción.
                "fila": sanitizar_nombre_columna(p.get("fila") or "Bandeja Nueva", permitir_reservadas=True),
                "ultima_interaccion_ia": p.get("ultima_interaccion_ia") or "", "ultimo_msj": bleach.clean(p.get("ultimo_msj") or "", tags=[], strip=True),
                "estado_iluminacion": str(p.get("estado_iluminacion") or "blanco")
            } for p in (prospectos_res.data or [])
        ]
        return {"status": "ok", "vendedor": vendedor_id, "ventas_hoy": total_hoy, "prospectos": prospectos_limpios}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error en mobile_dashboard: {e}")
        raise HTTPException(status_code=500, detail="Error interno al compilar dashboard móvil.")

# ==========================================================
# 🚀 12. GESTIÓN DE TARJETAS CRM (MOVIMIENTOS Y BORRADOS)
# ==========================================================
@router.post("/api/actualizar_estado")
async def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Forzando Estado CRM a: {datos.nueva_fila}")
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Identificador obligatorio.")
        col_segura = sanitizar_nombre_columna(datos.nueva_fila, permitir_reservadas=True)

        # 🔍 DIAGNÓSTICO TEMPORAL: confirmamos si el filtro SÍ encuentra la fila antes de intentar el UPDATE,
        # para aislar si el problema es de coincidencia (telefono/vendedor_id) o de escritura (ej. una política
        # RLS de Supabase bloqueando el UPDATE en silencio).
        logger.info(f"🔬 [DIAG TELEFONO RECIBIDO] datos.telefono_crudo={datos.telefono!r} tel_norm={tel_norm!r}")
        diag = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').select('id, telefono, vendedor_id, fila').eq('vendedor_id', str(_sesion)).eq('telefono', tel_norm)),
            timeout=8.0
        )
        logger.info(f"🔍 [DIAG ACTUALIZAR_ESTADO] SELECT previo encontró: {diag.data}")

        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update({'fila': col_segura}).eq('vendedor_id', str(_sesion)).eq('telefono', tel_norm), allow_retry=False),
            timeout=8.0
        )
        logger.info(f"🔍 [DIAG ACTUALIZAR_ESTADO] UPDATE devolvió: {resultado.data}")
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Registro no encontrado.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo actualizando tarjeta: {e}")
        raise HTTPException(status_code=500, detail="Error transaccional.")

@router.post("/api/borrar_prospecto")
async def borrar_prospecto(datos: BorrarRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Soft Delete ({FILA_PAPELERA}) para: '{datos.nombre}'")
    try:
        nombre_prospecto = datos.nombre.strip()
        if not nombre_prospecto: raise HTTPException(status_code=400, detail="Nombre requerido.")
        # 🔧 FIX: Godot manda el NOMBRE del cliente en este campo, no un ID numérico — comparar contra la
        # columna 'id' (bigint) siempre fallaba con un error de tipo en Postgres. Filtramos por 'nombre' en
        # su lugar. FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update({'fila': FILA_PAPELERA}).eq('vendedor_id', str(_sesion)).eq('nombre', nombre_prospecto), allow_retry=False),
            timeout=8.0
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Prospecto no encontrado para archivar.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo al archivar prospecto: {e}")
        raise HTTPException(status_code=500, detail="Error en base de datos al archivar.")

@router.post("/api/borrar_permanente")
async def borrar_permanente(datos: BorrarRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.warning(f"💀 🎮 [TRACE:{trace_id}] Hard Delete ejecutado para ID: '{datos.nombre}'")
    try:
        res_admin = await asyncio.wait_for(async_db_execute(supabase.table('usuarios_veltrix').select('rol').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
        if not res_admin.data or str(res_admin.data[0].get('rol', '')).lower() != 'admin':
            logger.warning(f"🚨 [TRACE:{trace_id}] Intento de Hard Delete bloqueado. Requiere privilegios de Administrador.")
            raise HTTPException(status_code=403, detail="Operación denegada. Se requieren privilegios de Administrador.")

        nombre_prospecto = datos.nombre.strip()
        if not nombre_prospecto: raise HTTPException(status_code=400, detail="Nombre requerido.")
        # 🔧 FIX: mismo problema que borrar_prospecto — Godot manda el nombre, no un ID numérico. Filtramos por
        # 'nombre' en vez de 'id'. FIX FASE 1: allow_retry=False por destrucción crítica de datos (DELETE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').delete().eq('vendedor_id', str(_sesion)).eq('nombre', nombre_prospecto), allow_retry=False),
            timeout=8.0
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="El prospecto no existe.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en hard delete: {e}")
        raise HTTPException(status_code=500, detail="Fallo en la base de datos al eliminar.")

# ==========================================================
# 🤖 14B. CONFIGURACIÓN DEL ASISTENTE (BOT_CONFIG)
# Rutas que el panel "Configurador del Asistente B2B" de Godot ya llamaba
# (/api/bot_config GET y POST) pero que nunca existieron en el backend —
# cualquier intento de cargar o guardar siempre devolvía 404.
# ==========================================================
@router.get("/api/bot_config")
async def obtener_bot_config(vendedor_id: str = "", _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    # 🛡️ El 'vendedor_id' de la URL es solo informativo para logs — la fuente
    # de verdad de identidad SIEMPRE es _sesion (el JWT validado), nunca lo
    # que mande el cliente en el query string.
    logger.info(f"🎮 [TRACE:{trace_id}] Cargando configuración del asistente para {_sesion}")
    try:
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('link_pago, texto_entrega, admin_phone, bot_activo').eq('vendedor_id', str(_sesion)).limit(1)),
            timeout=8.0
        )
        datos = res.data[0] if res.data else {}
        return {"status": "ok", "datos": datos}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo cargando bot_config: {e}")
        raise HTTPException(status_code=500, detail="Error al cargar la configuración del asistente.")

@router.post("/api/bot_config")
async def actualizar_bot_config(datos: BotConfigUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Guardando configuración del asistente para {_sesion}")
    try:
        payload = {
            "link_pago": bleach.clean(datos.link_pago.strip(), tags=[], strip=True)[:500],
            "texto_entrega": bleach.clean(datos.texto_entrega.strip(), tags=[], strip=True)[:5000],
            "admin_phone": re.sub(r"\D", "", datos.admin_phone)[:20],
            "bot_activo": datos.bot_activo,
        }
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').update(payload).eq('vendedor_id', str(_sesion)), allow_retry=False),
            timeout=8.0
        )
        if not resultado.data:
            # No existía fila previa para este tenant (caso poco común, pero
            # posible si configuracion_bot nunca se sembró) — la creamos.
            payload_insert = dict(payload)
            payload_insert["vendedor_id"] = str(_sesion)
            await asyncio.wait_for(
                async_db_execute(supabase.table('configuracion_bot').insert(payload_insert), allow_retry=False),
                timeout=8.0
            )
        return {"status": "ok", "success": True}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo guardando bot_config: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar la configuración del asistente.")

# ==========================================================
# 🔑 CAMBIAR CONTRASEÑA
# Antes no existía ninguna forma de que el cliente cambiara su propia
# contraseña — la única manera de tenerla era que el dueño de Veltrix la
# conociera en texto plano para crear el hash al dar de alta la cuenta.
# ==========================================================
@router.post("/api/cambiar_password")
async def cambiar_password(datos: CambiarPasswordRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🔑 [TRACE:{trace_id}] Solicitud de cambio de contraseña para {_sesion}")
    try:
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('password').eq('vendedor_id', str(_sesion)).limit(1)),
            timeout=10.0
        )
        if not res.data:
            raise HTTPException(status_code=404, detail="Cuenta no encontrada.")
        password_guardada = str(res.data[0].get('password', DUMMY_HASH))

        # Mismo patrón de verificación que el login — soporta el caso legacy
        # (contraseña antigua en texto plano, sin hashear todavía) además
        # del caso normal con bcrypt.
        if password_guardada.startswith('$2b$'):
            password_valida = await run_in_threadpool(pwd_context.verify, datos.password_actual, password_guardada)
        else:
            password_valida = (datos.password_actual == password_guardada)

        if not password_valida:
            raise HTTPException(status_code=401, detail="La contraseña actual no es correcta.")
        if datos.password_nueva == datos.password_actual:
            raise HTTPException(status_code=400, detail="La contraseña nueva debe ser diferente a la actual.")

        nuevo_hash = await run_in_threadpool(pwd_context.hash, datos.password_nueva)
        await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').update({'password': nuevo_hash}).eq('vendedor_id', str(_sesion)), allow_retry=False),
            timeout=10.0
        )
        logger.info(f"✅ [TRACE:{trace_id}] Contraseña actualizada correctamente para {_sesion}")
        return {"status": "ok", "detail": "Contraseña actualizada correctamente."}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en cambiar_password para {_sesion}: {e}")
        raise HTTPException(status_code=500, detail="Error al cambiar la contraseña.")

@router.post("/api/actualizar_notas")
# 🛡️ FIX: el backend tenía esta ruta en inglés ("notes") mientras Godot
# siempre pidió la versión en español ("notas") — esto explica el error de
# "Error al Guardar" al editar notas de un cliente que vimos hace rato.
async def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Número obligatorio.")
        update_data = {
            "notas": bleach.clean(datos.notas or "", tags=[], strip=True)[:5000],
            "etiquetas": bleach.clean(datos.etiquetas or "", tags=[], strip=True)[:5000],
            "nombre": bleach.clean(datos.nombre or "Cliente", tags=[], strip=True)[:120]
        }
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update(update_data).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion)), allow_retry=False),
            timeout=8.0
        )
        if res and res.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Tarjeta no localizada.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error inyectando notas CRM: {e}")
        raise HTTPException(status_code=500, detail="Error al sincronizar apuntes.")

# ==========================================================
# 📦 13. INVENTARIO Y STOCK (TRANSACCIONES ATÓMICAS)
# ==========================================================
@router.post("/api/actualizar_stock")
async def actualizar_stock(item: VentaItem, request: Request, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Procesando Venta Atómica de {item.nombre_producto}")
    try:
        vid_str = str(_sesion)
        if not item.id: raise HTTPException(400, "ID requerido.")
        idempotency_key = request.headers.get("x-idempotency-key")
        async with ventas_idempotencia_lock:
            if idempotency_key and idempotency_key in ventas_procesadas_idempotencia:
                return ventas_procesadas_idempotencia[idempotency_key]

        res_inv = await asyncio.wait_for(async_db_execute(supabase.table("inventario").select("id, nombre, stock, precio").eq("id", item.id).eq("vendedor_id", vid_str).limit(1)), timeout=10.0)
        if not res_inv.data: raise HTTPException(status_code=404, detail="Artículo no localizado.")

        db_item = res_inv.data[0]
        stock_actual = int(db_item.get("stock", 0))
        precio_venta = float(db_item.get("precio", 0.0))
        cantidad_descontar = max(1, item.cantidad_vendida) if item.cantidad_vendida is not None else 1
        if cantidad_descontar > stock_actual:
            raise HTTPException(status_code=400, detail=f"Stock insuficiente. Solicitado: {cantidad_descontar}, Disponible: {stock_actual}")
        nuevo_stock_seguro = stock_actual - cantidad_descontar

        # Optimistic Locking. FIX FASE 1: allow_retry=False por mutación concurrente crítica
        res_update = await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").update({"stock": nuevo_stock_seguro}).eq("id", item.id).eq("stock", stock_actual), allow_retry=False),
            timeout=10.0
        )
        if not res_update.data: raise HTTPException(status_code=409, detail="Colisión de inventario: El stock cambió en otra transacción paralela.")

        transaccion_id = str(uuid.uuid4())
        try:
            # FIX FASE 1: allow_retry=False por inserción financiera (INSERT)
            await asyncio.wait_for(
                async_db_execute(
                    supabase.table("ventas").insert({
                        "vendedor_id": vid_str, "nombre_producto": db_item.get("nombre", item.nombre_producto),
                        "monto": precio_venta * cantidad_descontar, "cantidad": cantidad_descontar,
                        "stock_anterior": stock_actual, "stock_nuevo": nuevo_stock_seguro,
                        "tx_uuid": transaccion_id, "created_at": datetime.now(timezone.utc).isoformat()
                    }),
                    allow_retry=False
                ),
                timeout=10.0
            )
        except Exception as e:
            logger.critical(f"🚨 [TRACE:{trace_id}] Fallo al insertar historial de venta. Ejecutando ROLLBACK de stock seguro. Error: {e}")
            await asyncio.wait_for(async_db_execute(supabase.table("inventario").update({"stock": stock_actual}).eq("id", item.id), allow_retry=False), timeout=10.0)
            raise HTTPException(status_code=500, detail="Fallo transaccional al registrar la venta. Stock revertido a su estado original.")

        respuesta_exitosa = {"status": "ok", "nuevo_stock": nuevo_stock_seguro, "tx_id": transaccion_id}
        async with ventas_idempotencia_lock:
            if idempotency_key: ventas_procesadas_idempotencia[idempotency_key] = respuesta_exitosa
        return respuesta_exitosa
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en transacción de venta: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al procesar la venta.")

# ==========================================================
# 📦 13B. INVENTARIO: CARGA, EDICIÓN Y BORRADO (VISOR)
# ==========================================================
class EditarItemVisorRequest(BaseModel):
    # 🛡️ FIX: estaba tipado como 'Any' — Pydantic nunca limpiaba el float que
    # GDScript manda siempre para cualquier número que venga de un JSON
    # parseado (1510.0 en vez de 1510), y Postgres rechazaba la consulta
    # contra la columna 'id' (int8/bigint) con un error de tipo. VentaItem ya
    # usaba 'int' aquí y por eso /api/actualizar_stock nunca tuvo este
    # problema — Pydantic limpia automáticamente un float sin parte
    # fraccionaria real cuando el campo está tipado como int.
    id: Optional[int] = None
    nombre: Optional[str] = ""
    consola: Optional[str] = ""
    precio: float = 0.0
    stock: int = 0
    # 🆕 Campos nuevos editables desde el Visor en Modo Avanzado/Agotados —
    # todos Optional y sin default forzado: si el Visor no los manda (ej.
    # editando desde Modo Básico, que no tiene estas columnas), la ruta no
    # debe tocarlos en absoluto, no sobreescribirlos con un valor vacío.
    estado_general: Optional[str] = None
    costo: Optional[float] = None
    genero: Optional[str] = None
    precio_min_inmediato: Optional[float] = None
    precio_min_24h: Optional[float] = None
    precio_min_72h: Optional[float] = None
    descripcion_detallada: Optional[str] = None
    tipo_producto: Optional[str] = None

class BorrarItemRequest(BaseModel):
    id: Optional[int] = None
    nombre: Optional[str] = ""
    consola: Optional[str] = ""

@router.post("/api/guardar_inventario")
async def guardar_inventario(datos: NuevoArticulo, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    """
    🆕 Esta ruta nunca existió en el backend, a pesar de que PanelVideojuegos.gd
    y PanelGenerico.gd ya le pegan al guardar o actualizar un artículo — cualquier
    intento desde esos paneles regresaba 404. El schema NuevoArticulo ya estaba
    importado en este archivo pero jamás se usaba en ninguna ruta, lo que sugiere
    que esta conexión se quedó pendiente.

    Decisión de diseño: se actualiza un registro EXISTENTE solo si 'id_catalogo'
    coincide con uno ya guardado para este vendedor — nunca por nombre (dos
    condiciones/consolas distintas del mismo título son artículos físicos
    distintos, no la misma fila). Si no hay coincidencia (o no se mandó
    id_catalogo, como en PanelGenerico), se inserta como artículo nuevo.
    """
    logger.info(f"📦 [TRACE:{trace_id}] Guardando inventario '{datos.nombre}' para {_sesion}")
    try:
        costo_final = datos.costo if datos.costo > 0 else datos.precio_compra
        campos = {
            "nombre": bleach.clean(datos.nombre.strip(), tags=[], strip=True)[:200],
            "categoria": bleach.clean(datos.categoria.strip(), tags=[], strip=True)[:100] if datos.categoria.strip() else "General",
            "tipo_producto": bleach.clean(datos.tipo_producto.strip(), tags=[], strip=True)[:100] if datos.tipo_producto.strip() else None,
            "genero": bleach.clean(datos.genero.strip(), tags=[], strip=True)[:100] if datos.genero.strip() else None,
            "estado_general": bleach.clean(datos.estado_general.strip(), tags=[], strip=True)[:100] if datos.estado_general.strip() else None,
            "precio": datos.precio,
            "costo": costo_final,
            "stock": datos.stock,
            "precio_minimo_bot": int(datos.precio_minimo_bot),
            # 🛡️ FIX (causa probable del 422 persistente en TODAS las altas):
            # esta columna es int8 en Supabase, pero el esquema de Pydantic
            # la tipa como float — siempre se mandaba con decimales (0.0,
            # etc.) sin importar lo que el usuario llenara en el formulario,
            # lo cual explica que el error fuera 100% consistente. Mismo
            # patrón que ya se corrigió para 'id' en editar/borrar.
            "codigo_barras": bleach.clean(datos.codigo_barras.strip(), tags=[], strip=True)[:100] if datos.codigo_barras.strip() else None,
            "url_portada": datos.url_portada.strip()[:500] if datos.url_portada.strip() else None,
            "descripcion_detallada": bleach.clean(datos.descripcion_detallada.strip(), tags=[], strip=True)[:2000],
            "atributos_extra": datos.atributos_extra or {},
        }
        # 🆕 Igual que en edición: solo se incluyen si de verdad se mandaron.
        if datos.precio_min_inmediato is not None:
            campos["precio_min_inmediato"] = datos.precio_min_inmediato
        if datos.precio_min_24h is not None:
            campos["precio_min_24h"] = datos.precio_min_24h
        if datos.precio_min_72h is not None:
            campos["precio_min_72h"] = datos.precio_min_72h
        if datos.id_catalogo and datos.id_catalogo.strip():
            campos["id_catalogo"] = datos.id_catalogo.strip()[:100]

        existente = None
        if datos.id_catalogo and datos.id_catalogo.strip():
            res_check = await asyncio.wait_for(
                async_db_execute(supabase.table('inventario').select('id').eq('vendedor_id', str(_sesion)).eq('id_catalogo', datos.id_catalogo.strip()).limit(1)),
                timeout=5.0
            )
            existente = res_check.data[0] if res_check.data else None

        if existente:
            # FIX FASE 1: allow_retry=False por mutación (UPDATE)
            resultado = await asyncio.wait_for(
                async_db_execute(supabase.table('inventario').update(campos).eq('id', existente['id']).eq('vendedor_id', str(_sesion)), allow_retry=False),
                timeout=10.0
            )
            return {"status": "ok", "accion": "actualizado", "id": existente['id']}
        else:
            await _verificar_cupo_inventario(str(_sesion), 1, trace_id)
            campos["vendedor_id"] = str(_sesion)
            resultado = await asyncio.wait_for(
                async_db_execute(supabase.table('inventario').insert(campos), allow_retry=False),
                timeout=10.0
            )
            nuevo_id = resultado.data[0]['id'] if resultado.data else None
            return {"status": "ok", "accion": "creado", "id": nuevo_id}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en guardar_inventario: {e}")
        raise HTTPException(status_code=500, detail="Error interno al guardar el artículo.")

@router.get("/api/cargar_inventario")
async def cargar_inventario(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📦 [TRACE:{trace_id}] Cargando inventario para {vendedor_id}")
    try:
        # 🛡️ FIX: estaba en .limit(500) — un cliente con más productos que
        # eso JAMÁS vería el resto en el Visor (ni en la lista, ni en la
        # detección de duplicados, ni en las listas dinámicas de Tipo/
        # Consola/Género), sin ningún aviso de que faltaban. Se sube a 5000
        # (~5MB de respuesta con todos los campos nuevos, dentro del límite
        # que ya valida el cliente Godot). Si algún cliente llega a superar
        # esto, lo correcto es paginación real, no subir el número otra vez.
        res = await asyncio.wait_for(async_db_execute(supabase.table("inventario").select("*").eq("vendedor_id", str(vendedor_id)).order("nombre").limit(5000)), timeout=12.0)
        items = res.data or []
        # Si algún ítem trae datos extra anidados (ej. de importación CSV), los exponemos también en el nivel
        # superior sin pisar columnas reales.
        for it in items:
            extra = it.get("atributos_extra")
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in it or it.get(k) in (None, ""): it[k] = v
        return {"status": "ok", "inventario": items}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en cargar_inventario: {e}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar inventario.")

@router.post("/api/editar_item_visor")
async def editar_item_visor(item: EditarItemVisorRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"✏️ [TRACE:{trace_id}] Editando ítem id={item.id} para {vendedor_id}")
    try:
        if not item.id: raise HTTPException(status_code=400, detail="ID requerido.")
        campos: dict = {"precio": item.precio, "stock": max(0, item.stock)}
        if item.nombre and item.nombre.strip(): campos["nombre"] = bleach.clean(item.nombre.strip(), tags=[], strip=True)
        if item.consola and item.consola.strip():
            # En Supabase la columna se llama "categoria", no "consola"
            campos["categoria"] = bleach.clean(item.consola.strip(), tags=[], strip=True)
        # 🆕 Campos nuevos editables desde Modo Avanzado/Agotados — se
        # incluyen SOLO si de verdad se mandaron (no None). Esto es crítico
        # para el borrado suave (que solo manda id/nombre/consola/precio/
        # stock=0): sin este guard, esos campos llegarían como None y
        # borrarían por accidente el estado/costo/género/descuentos que el
        # producto ya tenía guardados.
        if item.estado_general is not None and item.estado_general.strip():
            campos["estado_general"] = bleach.clean(item.estado_general.strip(), tags=[], strip=True)[:100]
        if item.costo is not None:
            campos["costo"] = item.costo
        if item.genero is not None:
            campos["genero"] = bleach.clean(item.genero.strip(), tags=[], strip=True)[:100] if item.genero.strip() else None
        if item.precio_min_inmediato is not None:
            campos["precio_min_inmediato"] = item.precio_min_inmediato
        if item.precio_min_24h is not None:
            campos["precio_min_24h"] = item.precio_min_24h
        if item.precio_min_72h is not None:
            campos["precio_min_72h"] = item.precio_min_72h
        if item.descripcion_detallada is not None:
            campos["descripcion_detallada"] = bleach.clean(item.descripcion_detallada.strip(), tags=[], strip=True)[:2000]
        if item.tipo_producto is not None:
            campos["tipo_producto"] = bleach.clean(item.tipo_producto.strip(), tags=[], strip=True)[:100] if item.tipo_producto.strip() else None

        res = await asyncio.wait_for(async_db_execute(supabase.table("inventario").update(campos).eq("id", item.id).eq("vendedor_id", str(vendedor_id)), allow_retry=False), timeout=10.0)
        return {"status": "ok", "updated": len(res.data) if res.data else 0}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en editar_item_visor: {e}")
        raise HTTPException(status_code=500, detail="Error interno al editar ítem.")

@router.post("/api/borrar_item")
async def borrar_item(item: BorrarItemRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🗑️ [TRACE:{trace_id}] Borrando ítem id={item.id} para {vendedor_id}")
    try:
        if not item.id: raise HTTPException(status_code=400, detail="ID requerido.")
        res = await asyncio.wait_for(async_db_execute(supabase.table("inventario").delete().eq("id", item.id).eq("vendedor_id", str(vendedor_id)), allow_retry=False), timeout=10.0)
        return {"status": "ok", "deleted": len(res.data) if res.data else 0}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en borrar_item: {e}")
        raise HTTPException(status_code=500, detail="Error interno al eliminar ítem.")

# ==========================================================
# 📢 14. ENVÍOS MASIVOS (MARKETING AUTOMATION - BATCHING AAA)
# ==========================================================
def procesar_en_lotes(lista, n):
    for i in range(0, len(lista), n): yield lista[i:i + n]

async def background_enviar_campana(prospectos: list, mensaje: str, meta_token: str, meta_phone_id: str, trace_id: str):
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async
    logger.info(f"🚀 [TRACE:{trace_id}] Iniciando envío masivo a {len(prospectos)} prospectos (En Lotes).")
    sem = asyncio.Semaphore(10)

    async def enviar_con_semaforo(p):
        async with sem:
            try:
                await disparar_whatsapp_dinamico_async(p['telefono'], mensaje, meta_token, meta_phone_id)
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"❌ [TRACE:{trace_id}] Fallo envío a {enmascarar_telefono(p['telefono'])}: {str(e)}")

    for lote in procesar_en_lotes(prospectos, 50):
        tareas = [asyncio.create_task(enviar_con_semaforo(p)) for p in lote]
        await asyncio.gather(*tareas)
        await asyncio.sleep(1.0)
    logger.info(f"✅ [TRACE:{trace_id}] Campaña masiva completada exitosamente.")

@router.post("/api/mensaje_masivo")
async def ejecutar_campana_masiva(datos: CampanaMasivaRequest, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    try:
        vendedor_id = str(_sesion)
        res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', vendedor_id).limit(1)), timeout=5.0)
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración de Meta no encontrada para este tenant.")
        config = res_conf.data[0]
        meta_token, meta_phone = config.get('meta_token'), config.get('meta_phone_id')
        if not meta_token or not meta_phone: raise HTTPException(status_code=400, detail="Las credenciales de Meta están incompletas.")

        fila_a_buscar = datos.columna_origen
        res_prospectos = await async_db_execute(supabase.table('prospectos').select('telefono, nombre').eq('vendedor_id', vendedor_id).eq('fila', fila_a_buscar))
        prospectos_data = res_prospectos.data or []
        if not prospectos_data: return {"status": "ok", "msg": "No hay prospectos en la columna especificada."}

        background_tasks.add_task(background_enviar_campana, prospectos_data, datos.mensaje, meta_token, meta_phone, trace_id)
        return {"status": "ok", "msg": "Campaña encolada en background", "total_objetivos": len(prospectos_data)}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en encolamiento de ejecución masiva: {e}")
        raise HTTPException(status_code=500, detail="Fallo campaña masiva.")

# ==========================================================
# 🔍 16B. BÚSQUEDA EN CATÁLOGO MAESTRO (autocompletado de PanelVideojuegos.gd)
# Esta ruta nunca existió — el autocompletado mientras se escribe en
# PanelVideojuegos siempre regresaba 404 en silencio.
# ==========================================================
@router.get("/api/buscar_maestro")
async def buscar_maestro(q: str, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🔍 [TRACE:{trace_id}] Buscando en catálogo maestro: '{q}'")
    try:
        termino = limpiar_texto(q)[:100]
        if len(termino) < 2:
            return {"status": "ok", "resultados": []}
        # 🛡️ Nombres de columna inferidos del lado de Godot (PanelVideojuegos.gd
        # ya espera exactamente estos campos) — verifica que coincidan con el
        # esquema real de 'catalogo_maestro' si algo no aparece bien poblado.
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('catalogo_maestro')
                .select('id, nombre, consola, url_portada_oficial, precio_nuevo, precio_cib, precio_incompleto, precio_suelto')
                .ilike('nombre', f'%{termino}%').limit(10)
            ),
            timeout=8.0
        )
        return {"status": "ok", "resultados": res.data or []}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en buscar_maestro: {e}")
        raise HTTPException(status_code=500, detail="Error al buscar en el catálogo maestro.")

# ==========================================================
# 🔍 16B-2. PROXY DE BÚSQUEDA RAWG (portadas de videojuegos)
# 🛡️ FIX SEGURIDAD: la API key de RAWG vivía hardcodeada directo en
# PanelVideojuegos.gd — cualquiera que descompilara el ejecutable de
# Veltrix PC podía extraerla y usarla por su cuenta (agotando la cuota, o
# arriesgando que RAWG la bloquee para todos los clientes de videojuegos
# a la vez). Ahora Godot le pide esto a esta ruta, y la key vive solo
# aquí, como variable de entorno — nunca sale al cliente.
# ==========================================================
@router.get("/api/buscar_rawg")
async def buscar_rawg(q: str, platforms: str = "", _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🌐 [TRACE:{trace_id}] Buscando portada en RAWG: '{q}' (platforms={platforms})")
    if not RAWG_API_KEY:
        logger.warning(f"⚠️ [TRACE:{trace_id}] RAWG_API_KEY no configurada — se regresa lista vacía sin tronar.")
        return {"status": "ok", "results": []}
    try:
        termino = limpiar_texto(q)[:150]
        if len(termino) < 2:
            return {"status": "ok", "results": []}
        # Solo números y comas — son IDs de plataforma de RAWG, nunca texto libre.
        plataformas_limpias = re.sub(r"[^0-9,]", "", str(platforms))[:50]

        cliente = get_http_client()
        params = {"key": RAWG_API_KEY, "search": termino, "page_size": 10}
        if plataformas_limpias:
            params["platforms"] = plataformas_limpias

        resp = await asyncio.wait_for(cliente.get("https://api.rawg.io/api/games", params=params), timeout=10.0)
        if resp.status_code != 200:
            logger.warning(f"⚠️ [TRACE:{trace_id}] RAWG respondió código {resp.status_code} — se regresa lista vacía.")
            return {"status": "ok", "results": []}

        data = resp.json()
        # Solo se exponen los dos campos que el cliente realmente usa — no se
        # reenvía la respuesta completa de RAWG (ratings, metacritic, etc. no
        # hacen falta y solo aumentan el tamaño de la respuesta sin razón).
        resultados = [
            {"name": str(r.get("name", "")), "background_image": str(r.get("background_image", "") or "")}
            for r in (data.get("results") or [])[:10]
        ]
        return {"status": "ok", "results": resultados}
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ [TRACE:{trace_id}] RAWG tardó demasiado — se regresa lista vacía.")
        return {"status": "ok", "results": []}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo consultando RAWG: {e}")
        return {"status": "ok", "results": []}

# ==========================================================
# 🛒 16C-2. "VELTRIX STORE" COMUNITARIO — marketplace entre vendedores
# En vez de una vitrina aislada por cliente, el comprador elige un GIRO,
# y puede buscar por nombre (viendo resultados de TODOS los vendedores
# de ese giro, con ciudad/estado y WhatsApp de cada uno para decidir por
# cercanía/precio) o elegir un vendedor específico para ver su catálogo
# completo. Mismas protecciones de datos que la tienda individual: nunca
# costo, código de barras, ni stock exacto.
# ==========================================================
def _es_mismo_giro(giro_a: str, giro_b_buscado_es_videojuegos: bool) -> bool:
    g = str(giro_a or '').lower()
    return ("videojueg" in g or g == "") == giro_b_buscado_es_videojuegos

async def _vendedores_activos_por_giro(giro_buscado: str) -> list:
    """Regresa [{vendedor_id, nombre, ciudad, estado_ubicacion}] de cuentas activas que comparten el mismo giro (videojuegos vs. cualquier otro, igual que en el resto del sistema)."""
    es_vj_buscado = "videojueg" in giro_buscado.lower() or giro_buscado.strip() == ""
    # 🛡️ FIX: se agrega 'nombre_negocio' aquí mismo — vive en esta tabla
    # (configuracion_bot), no en usuarios_veltrix. Antes el público veía el
    # nombre de la PERSONA que abrió la cuenta (nombre_contacto), no el del
    # negocio.
    res_giros = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('vendedor_id, giro, nombre_negocio')), timeout=10.0)
    ids_match = [r['vendedor_id'] for r in (res_giros.data or []) if _es_mismo_giro(r.get('giro'), es_vj_buscado)]
    if not ids_match:
        return []
    nombres_negocio_por_id = {r['vendedor_id']: str(r.get('nombre_negocio') or '').strip() for r in (res_giros.data or [])}
    res_neg = await asyncio.wait_for(
        async_db_execute(supabase.table('usuarios_veltrix').select('vendedor_id, nombre_contacto, ciudad, estado_ubicacion, estado').in_('vendedor_id', ids_match)),
        timeout=10.0
    )
    return [
        {
            "vendedor_id": r['vendedor_id'],
            # nombre_negocio primero; si un tenant nunca lo configuró, cae al
            # nombre de contacto en vez de dejarlo vacío.
            "nombre": nombres_negocio_por_id.get(r['vendedor_id']) or str(r.get('nombre_contacto', '') or 'Tienda'),
            "ciudad": str(r.get('ciudad', '') or ''),
            "estado_ubicacion": str(r.get('estado_ubicacion', '') or '')
        }
        for r in (res_neg.data or []) if r.get('estado') == 'activo'
    ]

@router.get("/api/store/giros")
async def store_giros(trace_id: str = Depends(obtener_trace_id)):
    """Lista de giros que de verdad tienen al menos un vendedor activo — para llenar el selector de categorías."""
    try:
        res_giros = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('vendedor_id, giro')), timeout=10.0)
        res_activos = await asyncio.wait_for(async_db_execute(supabase.table('usuarios_veltrix').select('vendedor_id').eq('estado', 'activo')), timeout=10.0)
        ids_activos = {r['vendedor_id'] for r in (res_activos.data or [])}
        giros_vistos = set()
        for r in (res_giros.data or []):
            if r['vendedor_id'] not in ids_activos: continue
            g = str(r.get('giro') or '').lower().strip()
            giros_vistos.add('videojuegos' if ('videojueg' in g or g == '') else g)
        return {"status": "ok", "giros": sorted(giros_vistos)}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en store_giros: {e}")
        return {"status": "ok", "giros": []}

@router.get("/api/store/vendedores")
async def store_vendedores(giro: str, trace_id: str = Depends(obtener_trace_id)):
    """Lista de vendedores activos de un giro — para el selector de 'o elige un vendedor directamente'."""
    try:
        return {"status": "ok", "vendedores": await _vendedores_activos_por_giro(giro)}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en store_vendedores: {e}")
        return {"status": "ok", "vendedores": []}

_RATE_LIMIT_STORE_BUSQUEDA = TTLCache(maxsize=5000, ttl=60)

@router.get("/api/store/buscar")
async def store_buscar(giro: str, q: str, request: Request, trace_id: str = Depends(obtener_trace_id)):
    """Búsqueda cruzada entre TODOS los vendedores activos de un giro — el corazón del marketplace comunitario."""
    ip_cliente = request.client.host if request.client else "desconocido"
    conteo_actual = _RATE_LIMIT_STORE_BUSQUEDA.get(ip_cliente, 0)
    if conteo_actual > 60:
        raise HTTPException(status_code=429, detail="Demasiadas búsquedas. Intenta de nuevo en un momento.")
    _RATE_LIMIT_STORE_BUSQUEDA[ip_cliente] = conteo_actual + 1

    termino = limpiar_texto(q)[:100].strip()
    if len(termino) < 2:
        return {"status": "ok", "resultados": []}
    try:
        vendedores = await _vendedores_activos_por_giro(giro)
        if not vendedores:
            return {"status": "ok", "resultados": []}
        mapa_vendedores = {v['vendedor_id']: v for v in vendedores}

        res_inv = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario')
                .select('id, vendedor_id, nombre, categoria, tipo_producto, genero, estado_general, precio, precio_min_inmediato, url_portada, stock')
                .in_('vendedor_id', list(mapa_vendedores.keys()))
                .gt('stock', 0)
                .ilike('nombre', f'%{termino}%')
                .limit(100)
            ),
            timeout=15.0
        )

        # También se necesita el teléfono de cada vendedor para el enlace de WhatsApp.
        # 🛡️ FIX PRIVACIDAD: usaba 'admin_phone' — ese es el número PERSONAL
        # del dueño para alertas internas, nunca debió mostrarse a un
        # cliente final navegando la tienda. Se usa el número público
        # dedicado del bot.
        res_tel = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('vendedor_id, numero_bot_whatsapp').in_('vendedor_id', list(mapa_vendedores.keys()))),
            timeout=10.0
        )
        telefonos = {r['vendedor_id']: str(r.get('numero_bot_whatsapp', '') or '') for r in (res_tel.data or [])}

        resultados = []
        for item in (res_inv.data or []):
            vid = item.get('vendedor_id')
            v = mapa_vendedores.get(vid, {})
            precio = float(item.get('precio') or 0)
            precio_especial = item.get('precio_min_inmediato')
            tiene_oferta = precio_especial is not None and float(precio_especial) > 0 and float(precio_especial) < precio
            resultados.append({
                "nombre": str(item.get('nombre', '')),
                "categoria": str(item.get('categoria', '') or ''),
                "genero": str(item.get('genero', '') or ''),
                "estado": str(item.get('estado_general', '') or ''),
                "precio": precio,
                "precio_especial": float(precio_especial) if tiene_oferta else None,
                "foto": str(item.get('url_portada', '') or ''),
                "vendedor_id": vid,
                "vendedor_nombre": v.get('nombre', 'Tienda'),
                "vendedor_ciudad": v.get('ciudad', ''),
                "vendedor_estado": v.get('estado_ubicacion', ''),
                "vendedor_whatsapp": telefonos.get(vid, ''),
            })
        return {"status": "ok", "resultados": resultados}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en store_buscar: {e}")
        return {"status": "ok", "resultados": []}

# ==========================================================
# 🛒 16C. CATÁLOGO PÚBLICO ("Veltrix Store") — SIN AUTENTICACIÓN
# Pensado para clientes FINALES (no dueños de negocio) — para que puedan
# ver el inventario completo de un vendedor y comprar directo por
# WhatsApp, sin necesitar cuenta ni login. Es justo lo que ya se promete
# en la página de Veltrix Engine como "Veltrix Store: vende en segundos
# con un solo clic".
#
# 🛡️ MUY IMPORTANTE — esta ruta es pública por diseño, así que se filtra
# con cuidado lo que se expone: NUNCA costo, código de barras, notas
# internas, ni el conteo exacto de stock (solo "disponible: sí/no") —
# eso es información privada/competitiva del negocio, no del cliente final.
# ==========================================================
_RATE_LIMIT_STORE = TTLCache(maxsize=5000, ttl=60)  # máx peticiones por vendedor por minuto

@router.get("/api/store/{vendedor_id}")
async def catalogo_publico(vendedor_id: str, q: str = "", trace_id: str = Depends(obtener_trace_id)):
    vendedor_id = re.sub(r"[^A-Za-z0-9\-_]", "", vendedor_id)[:50]
    if not vendedor_id:
        raise HTTPException(status_code=404, detail="Tienda no encontrada.")

    # Límite simple por vendedor — esta ruta es pública (sin login), así que
    # es el único punto de abuso que un escáner/bot externo podría intentar.
    conteo_actual = _RATE_LIMIT_STORE.get(vendedor_id, 0)
    if conteo_actual > 120:
        raise HTTPException(status_code=429, detail="Demasiadas peticiones. Intenta de nuevo en un momento.")
    _RATE_LIMIT_STORE[vendedor_id] = conteo_actual + 1

    try:
        res_neg = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('nombre_contacto, estado').eq('vendedor_id', vendedor_id).limit(1)),
            timeout=5.0
        )
        if not res_neg.data or res_neg.data[0].get('estado') != 'activo':
            raise HTTPException(status_code=404, detail="Tienda no encontrada.")

        res_conf = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('giro, numero_bot_whatsapp, nombre_negocio').eq('vendedor_id', vendedor_id).limit(1)),
            timeout=5.0
        )
        # 🛡️ FIX: mostraba 'nombre_contacto' (la PERSONA que abrió la cuenta)
        # en vez de 'nombre_negocio' (que vive en configuracion_bot) — cae a
        # nombre_contacto solo si el tenant nunca configuró nombre_negocio.
        nombre_negocio = str((res_conf.data[0].get('nombre_negocio') if res_conf.data else None) or res_neg.data[0].get('nombre_contacto') or 'Tienda')
        giro = str((res_conf.data[0].get('giro') if res_conf.data else None) or '').lower()
        es_videojuegos = "videojueg" in giro or giro == ""
        # 🛡️ FIX PRIVACIDAD: usaba 'admin_phone' (número PERSONAL del dueño
        # para alertas internas) — un visitante cualquiera de la tienda
        # estaba viendo ese número en vez del número público del bot.
        telefono_whatsapp = str((res_conf.data[0].get('numero_bot_whatsapp') if res_conf.data else None) or '')

        query = supabase.table('inventario').select(
            'id, nombre, categoria, tipo_producto, genero, estado_general, precio, precio_min_inmediato, url_portada, stock'
        ).eq('vendedor_id', vendedor_id).gt('stock', 0)
        termino = limpiar_texto(q)[:100].strip()
        if termino:
            query = query.ilike('nombre', f'%{termino}%')
        res_inv = await asyncio.wait_for(async_db_execute(query.order('nombre').limit(1000)), timeout=15.0)

        productos = []
        for item in (res_inv.data or []):
            precio = float(item.get('precio') or 0)
            precio_especial = item.get('precio_min_inmediato')
            tiene_precio_especial = precio_especial is not None and float(precio_especial) > 0 and float(precio_especial) < precio
            productos.append({
                "id": item.get('id'),
                "nombre": str(item.get('nombre', '')),
                "categoria": str(item.get('categoria', '') or ''),
                "tipo_producto": str(item.get('tipo_producto', '') or ''),
                "genero": str(item.get('genero', '') or ''),
                "estado": str(item.get('estado_general', '') or ''),
                "precio": precio,
                "precio_especial": float(precio_especial) if tiene_precio_especial else None,
                "foto": str(item.get('url_portada', '') or ''),
                # Solo disponibilidad — nunca el conteo exacto.
                "disponible": True,
            })

        return {
            "status": "ok",
            "negocio": {
                "nombre": nombre_negocio,
                "giro_videojuegos": es_videojuegos,
                "etiqueta_item": "Juego" if es_videojuegos else "Producto",
                "etiqueta_variante": "Consola" if es_videojuegos else "Variante",
                "whatsapp": telefono_whatsapp,
            },
            "productos": productos,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en catalogo_publico para {vendedor_id}: {e}")
        raise HTTPException(status_code=500, detail="Error al cargar la tienda.")

# ==========================================================
# 💰 16C. MÉTRICAS FINANCIERAS (panel de finanzas en tiempo real)
# Tampoco existía — el botón de "Finanzas" en dashboard_main.gd siempre
# mostraba "Error al conectar con Finanzas".
# ==========================================================
@router.get("/api/metricas")
async def obtener_metricas_financieras(_sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"💰 [TRACE:{trace_id}] Calculando métricas financieras para {_sesion}")
    try:
        res_inv = await asyncio.wait_for(
            async_db_execute(supabase.table('inventario').select('stock, precio, costo, nombre').eq('vendedor_id', str(_sesion))),
            timeout=10.0
        )
        items = res_inv.data or []
        piezas = sum(int(it.get('stock', 0) or 0) for it in items)
        costo_inv = sum(float(it.get('costo', 0) or 0) * int(it.get('stock', 0) or 0) for it in items)
        valor = sum(float(it.get('precio', 0) or 0) * int(it.get('stock', 0) or 0) for it in items)
        ganancia_potencial = valor - costo_inv
        costo_por_nombre = {str(it.get('nombre', '')).strip().lower(): float(it.get('costo', 0) or 0) for it in items}

        res_ventas = await asyncio.wait_for(
            async_db_execute(supabase.table('ventas').select('monto, cantidad, nombre_producto').eq('vendedor_id', str(_sesion))),
            timeout=10.0
        )
        ventas = res_ventas.data or []
        ventas_totales = sum(float(v.get('monto', 0) or 0) for v in ventas)
        # 🛡️ Aproximación deliberada: la tabla 'ventas' no guarda el costo al
        # momento de cada venta, así que la ganancia real usa el costo ACTUAL
        # del artículo — puede no coincidir exacto si el costo cambió desde
        # que se vendió. Sería más preciso si 'ventas' guardara el costo al
        # momento de la transacción, pero eso es un cambio más grande.
        costo_de_lo_vendido = sum(
            costo_por_nombre.get(str(v.get('nombre_producto', '')).strip().lower(), 0.0) * int(v.get('cantidad', 1) or 1)
            for v in ventas
        )
        ganancia_real = ventas_totales - costo_de_lo_vendido

        return {
            "status": "ok", "piezas": piezas, "costo_inv": round(costo_inv, 2),
            "valor": round(valor, 2), "ganancia_potencial": round(ganancia_potencial, 2),
            "ventas_totales": round(ventas_totales, 2), "ganancia_real": round(ganancia_real, 2)
        }
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo calculando métricas: {e}")
        raise HTTPException(status_code=500, detail="Error al calcular métricas financieras.")

# ==========================================================
# 💀 16D. RESET COMPLETO (DESTRUCTIVO — exclusivo para Administrador)
# Tampoco existía. Borra TODO el inventario, prospectos y mensajes del
# tenant — irreversible. Mismo candado de rol que /api/borrar_permanente.
# ==========================================================
@router.post("/api/reset_limpio")
async def reset_limpio(_sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.warning(f"💀 [TRACE:{trace_id}] RESET COMPLETO solicitado para {_sesion}")
    try:
        res_admin = await asyncio.wait_for(async_db_execute(supabase.table('usuarios_veltrix').select('rol').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
        if not res_admin.data or str(res_admin.data[0].get('rol', '')).lower() != 'admin':
            logger.warning(f"🚨 [TRACE:{trace_id}] Intento de reset completo bloqueado. Requiere privilegios de Administrador.")
            raise HTTPException(status_code=403, detail="Operación denegada. Se requieren privilegios de Administrador.")

        # FIX FASE 1: allow_retry=False — destrucción masiva e irreversible (DELETE)
        await asyncio.wait_for(async_db_execute(supabase.table('inventario').delete().eq('vendedor_id', str(_sesion)), allow_retry=False), timeout=15.0)
        await asyncio.wait_for(async_db_execute(supabase.table('prospectos').delete().eq('vendedor_id', str(_sesion)), allow_retry=False), timeout=15.0)
        await asyncio.wait_for(async_db_execute(supabase.table('mensajes_chat').delete().eq('vendedor_id', str(_sesion)), allow_retry=False), timeout=15.0)
        # 🛡️ FIX: 'ventas' no se borraba — las métricas financieras (que leen
        # de esta tabla) seguirían mostrando ventas históricas después de un
        # "reset completo", contradiciendo la expectativa de "cero métricas".
        await asyncio.wait_for(async_db_execute(supabase.table('ventas').delete().eq('vendedor_id', str(_sesion)), allow_retry=False), timeout=15.0)

        logger.warning(f"💀 [TRACE:{trace_id}] RESET COMPLETO ejecutado para {_sesion} — inventario, prospectos y mensajes borrados permanentemente.")
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en reset_limpio: {e}")
        raise HTTPException(status_code=500, detail="Error al ejecutar el reset.")

# ==========================================================
# 📄 16. PLANTILLA CSV DE INVENTARIO (IMPORTAR / EXPORTAR)
# Ninguna de las dos rutas existía — ni en este backend modular ni en el
# monolito original — a pesar de que Godot ya las llama
# (_generar_plantilla_csv / _procesar_importacion_csv en dashboard_main.gd).
# Columnas oficiales por giro: videojuegos tiene su propio set; cualquier
# otro giro cae en un set genérico hasta que se definan sus columnas reales.
# ==========================================================
COLUMNAS_CSV_VIDEOJUEGOS = [
    # 🆕 "id": vacío para productos nuevos (el Alta de siempre); si tiene un
    # número (viene de exportar tu inventario real), la importación lo
    # toma como "actualiza este producto" en vez de crear uno duplicado.
    "id", "nombre", "tipo_producto", "plataforma", "genero", "estado_general", "descripcion_detallada",
    "precio_compra", "precio_venta", "cantidad", "codigo_barras",
    "precio_min_inmediato", "precio_min_24h", "precio_min_72h"
]
COLUMNAS_CSV_GENERICO = ["id", "nombre", "tipo_producto", "categoria", "precio_compra", "precio_venta", "cantidad", "descripcion"]

# 🛡️ FIX: la columna se llamaba "condicion" — ambiguo, sonaba a un valor fijo
# tipo "nuevo/usado" en vez de la nota libre que en realidad es ("rayado",
# "le falta el case", etc. — lo que el bot lee para describirle el estado
# real al cliente). Se renombra para que sea explícito.
EJEMPLO_CSV_VIDEOJUEGOS = ["", "Batman Arkham Knight", "Videojuegos", "PS4", "Acción", "Completo", "Disco con rayones leves, funciona perfecto", "300", "550", "1", "", "500", "450", "400"]
EJEMPLO_CSV_GENERICO = ["", "Producto de ejemplo", "Mercancía General", "Categoría A", "100", "200", "1", "Descripción breve"]

INSTRUCCIONES_CSV_VIDEOJUEGOS = [
    "DÉJALO VACÍO si es alta nueva. Con un número = actualiza ESE producto (viene de exportar tu inventario)",
    "Nombre del producto — OBLIGATORIO",
    "Tipo: Videojuegos, Accesorios, Reparaciones, etc. — tú decides las categorías de tu negocio",
    "Plataforma: PS5, PS4, Xbox, Switch, etc.",
    "Género: Acción, Aventura, RPG, Deportes, etc.",
    "Estado físico: Nuevo/Sellado, Completo CIB, Solo Disco, etc.",
    "Notas o defectos visibles — texto libre, ej. 'rayón leve en el disco'",
    "Costo de compra — número, sin signo de pesos",
    "Precio de venta — OBLIGATORIO, número mayor a cero",
    "Cantidad en stock — número entero. CERO está bien (queda guardado pero oculto hasta que le pongas stock)",
    "Código de barras si tiene uno — déjalo vacío si no",
    "Precio mínimo autorizado de inmediato — opcional, déjalo vacío si no aplica",
    "Precio mínimo autorizado a 24h — opcional",
    "Precio mínimo autorizado a 72h — opcional",
]
INSTRUCCIONES_CSV_GENERICO = [
    "DÉJALO VACÍO si es alta nueva. Con un número = actualiza ESE producto (viene de exportar tu inventario)",
    "Nombre del producto — OBLIGATORIO",
    "Tipo: tú decides las categorías de tu negocio (ej. Ropa, Accesorios, Servicios)",
    "Categoría / variante (ej. talla, color, modelo) — texto libre",
    "Costo de compra — número, sin signo de pesos",
    "Precio de venta — OBLIGATORIO, número mayor a cero",
    "Cantidad en stock — número entero. CERO está bien (queda guardado pero oculto hasta que le pongas stock)",
    "Descripción breve — texto libre",
]

@router.get("/api/descargar_plantilla")
async def descargar_plantilla(prellenada: bool = False, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from fastapi.responses import Response
    import csv, io as io_csv
    logger.info(f"📄 [TRACE:{trace_id}] Generando plantilla CSV de inventario para {_sesion} (prellenada={prellenada})")
    try:
        res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('giro').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
        # 🛡️ FIX: si 'giro' existe en la fila pero su valor es NULL, .get('giro', '')
        # regresa None (no el default ''), porque el default solo aplica cuando la
        # LLAVE no existe — no cuando existe con valor nulo. str(None) = "none", lo
        # que hacía que nunca detectara correctamente el giro "videojuegos".
        giro_crudo = res_conf.data[0].get('giro') if res_conf.data else None
        giro = str(giro_crudo or '').lower()

        es_videojuegos = "videojueg" in giro or giro == ""  # default a videojuegos si no hay giro configurado aún
        columnas = COLUMNAS_CSV_VIDEOJUEGOS if es_videojuegos else COLUMNAS_CSV_GENERICO
        instrucciones = INSTRUCCIONES_CSV_VIDEOJUEGOS if es_videojuegos else INSTRUCCIONES_CSV_GENERICO
        filas_datos = [EJEMPLO_CSV_VIDEOJUEGOS if es_videojuegos else EJEMPLO_CSV_GENERICO]

        # 🆕 PLANTILLA PRELLENADA — en vez de tabla propia de "catálogo
        # maestro" (que existe pero es 100% específica de videojuegos), se
        # usa el inventario REAL de otros tenants del MISMO giro como fuente
        # del catálogo compartido. Crece solo: en el momento que cualquier
        # tenant guarda un producto nuevo, ya está disponible aquí para los
        # demás — no hace falta ningún paso de "anexar" por separado.
        # 🛡️ Privacidad: SOLO se comparten nombre/categoría/tipo/género —
        # nunca precio, stock, costo, código de barras ni notas de otro
        # tenant. Todo eso es información competitiva/privada de cada quien.
        if prellenada:
            res_giros = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('vendedor_id, giro')), timeout=10.0)
            vendedores_mismo_giro = []
            for r in (res_giros.data or []):
                g = str(r.get('giro') or '').lower()
                es_vj_otro = "videojueg" in g or g == ""
                if es_vj_otro == es_videojuegos:
                    vendedores_mismo_giro.append(r['vendedor_id'])

            if vendedores_mismo_giro:
                res_catalogo = await asyncio.wait_for(
                    async_db_execute(supabase.table('inventario').select('nombre, categoria, tipo_producto, genero, estado_general').in_('vendedor_id', vendedores_mismo_giro).limit(5000)),
                    timeout=20.0
                )
                vistos = set()
                filas_catalogo = []
                for item in (res_catalogo.data or []):
                    nombre_c = str(item.get('nombre', '')).strip()
                    cat_c = str(item.get('categoria', '') or '').strip()
                    if not nombre_c: continue
                    llave = (nombre_c.lower(), cat_c.lower())
                    if llave in vistos: continue
                    vistos.add(llave)
                    if es_videojuegos:
                        filas_catalogo.append(["", nombre_c, str(item.get('tipo_producto', '') or ''), cat_c, str(item.get('genero', '') or ''), str(item.get('estado_general', '') or ''), "", "0", "0", "0", "", "", "", ""])
                    else:
                        filas_catalogo.append(["", nombre_c, str(item.get('tipo_producto', '') or ''), cat_c, "0", "0", "0", ""])
                if filas_catalogo:
                    filas_datos = filas_catalogo
                    logger.info(f"✅ [TRACE:{trace_id}] Plantilla prellenada: {len(filas_catalogo)} productos únicos de {len(vendedores_mismo_giro)} tenant(s) del mismo giro.")
                else:
                    logger.info(f"ℹ️ [TRACE:{trace_id}] Plantilla prellenada solicitada, pero el catálogo de este giro está vacío todavía — se manda la plantilla normal.")

        buffer = io_csv.StringIO()
        writer = csv.writer(buffer)
        # 🆕 Fila de instrucciones (fila 1), encabezados reales (fila 2), y
        # datos desde la fila 3. Un CSV no se puede "proteger" como un Excel
        # real, pero esto deja muy claro qué va en cada columna sin
        # necesidad de un archivo aparte.
        writer.writerow(instrucciones)
        writer.writerow(columnas)
        for fila in filas_datos:
            writer.writerow(fila)
        contenido = buffer.getvalue()

        nombre_archivo = "Plantilla_Veltrix_Prellenada.csv" if (prellenada and len(filas_datos) > 1) else "Plantilla_Veltrix_Importar.csv"
        return Response(
            content=contenido,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={nombre_archivo}"}
        )
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo generando plantilla CSV: {e}")
        raise HTTPException(status_code=500, detail="Error al generar la plantilla.")

# ==========================================================
# 📦 16B. EXPORTAR INVENTARIO REAL A CSV
# Antes solo existía la plantilla en blanco — no había forma de sacar el
# inventario YA CARGADO para respaldarlo o editarlo en bloque. Usa las
# mismas columnas que la plantilla/importación (mismo giro), para que el
# ciclo exportar → editar en Excel → reimportar funcione sin fricción.
# ==========================================================
# Traduce cada columna del CSV al nombre real de la columna en la tabla —
# espejo de COLUMNAS_OFICIALES en dashboard_main.gd, pero en la dirección
# inversa (de cara afuera hacia adentro de la base).
_MAPA_CSV_A_CAMPO_REAL = {
    "plataforma": "categoria", "categoria": "categoria",
    "tipo_producto": "tipo_producto",
    "genero": "genero",
    "estado_general": "estado_general",
    "descripcion_detallada": "descripcion_detallada", "descripcion": "descripcion_detallada",
    "precio_compra": "costo",
    "precio_venta": "precio",
    "cantidad": "stock",
    "codigo_barras": "codigo_barras",
    "precio_min_inmediato": "precio_min_inmediato",
    "precio_min_24h": "precio_min_24h",
    "precio_min_72h": "precio_min_72h",
}

@router.get("/api/exportar_inventario")
async def exportar_inventario(_sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from fastapi.responses import Response
    import csv, io as io_csv
    logger.info(f"📦 [TRACE:{trace_id}] Exportando inventario real para {_sesion}")
    try:
        res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('giro').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
        giro_crudo = res_conf.data[0].get('giro') if res_conf.data else None
        giro = str(giro_crudo or '').lower()
        es_videojuegos = "videojueg" in giro or giro == ""
        columnas = COLUMNAS_CSV_VIDEOJUEGOS if es_videojuegos else COLUMNAS_CSV_GENERICO
        instrucciones = INSTRUCCIONES_CSV_VIDEOJUEGOS if es_videojuegos else INSTRUCCIONES_CSV_GENERICO

        res = await asyncio.wait_for(async_db_execute(supabase.table('inventario').select('*').eq('vendedor_id', str(_sesion)).order('nombre').limit(5000)), timeout=15.0)
        items = res.data or []

        buffer = io_csv.StringIO()
        writer = csv.writer(buffer)
        # 🆕 Misma estructura de 3 filas que la plantilla: instrucciones,
        # encabezados, datos desde la fila 3 — así un archivo exportado se
        # puede reimportar directo sin que la fila 1 de instrucciones se
        # confunda con un producto real.
        writer.writerow(instrucciones)
        writer.writerow(columnas)
        for item in items:
            fila = []
            for col in columnas:
                if col == "nombre":
                    fila.append(str(item.get("nombre", "") or ""))
                    continue
                campo_real = _MAPA_CSV_A_CAMPO_REAL.get(col, col)
                valor = item.get(campo_real)
                fila.append("" if valor is None else str(valor))
            writer.writerow(fila)
        contenido = buffer.getvalue()

        logger.info(f"✅ [TRACE:{trace_id}] Exportados {len(items)} productos para {_sesion}.")
        return Response(
            content=contenido,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Mi_Inventario_Veltrix.csv"}
        )
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo exportando inventario CSV: {e}")
        raise HTTPException(status_code=500, detail="Error al exportar el inventario.")

class ImportarInventarioRequest(BaseModel):
    vendedor_id: str = ""
    inventario: List[dict] = Field(default_factory=list)

# ==========================================================
# 🆕 TOPE DE INVENTARIO POR PAQUETE DE PRECIO
# Sin esto, nada impedía que una cuenta acumulara inventario sin límite —
# incluyendo abuso intencional (muchas peticiones pequeñas para esquivar
# el límite por lote) o simplemente un cliente creciendo más allá de lo
# que paga. El límite se guarda en usuarios_veltrix.limite_inventario
# (default 1000), pensado para ligarse a los 3 paquetes de precio.
# ==========================================================
async def _verificar_cupo_inventario(vendedor_id: str, cuantos_nuevos: int, trace_id: str) -> None:
    res_limite = await asyncio.wait_for(
        async_db_execute(supabase.table('usuarios_veltrix').select('limite_inventario').eq('vendedor_id', vendedor_id).limit(1)),
        timeout=5.0
    )
    limite = int(res_limite.data[0].get('limite_inventario') or 1000) if res_limite.data else 1000
    # Se cuenta con select('id') + len() en vez de count='exact' — más lento
    # para inventarios enormes, pero garantizado a funcionar sin depender de
    # un detalle de versión del cliente de Supabase que no se puede probar
    # en este entorno.
    res_actuales = await asyncio.wait_for(
        async_db_execute(supabase.table('inventario').select('id').eq('vendedor_id', vendedor_id)),
        timeout=10.0
    )
    actuales = len(res_actuales.data or [])
    if actuales + cuantos_nuevos > limite:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Tope de inventario alcanzado para {vendedor_id}: {actuales} actuales + {cuantos_nuevos} nuevos > límite {limite}.")
        raise HTTPException(
            status_code=403,
            detail=f"Tu plan permite hasta {limite} productos. Tienes {actuales} y esto agregaría {cuantos_nuevos} más. Elimina productos sin uso, o contacta a soporte para ampliar tu plan."
        )

def _safe_float_importacion(valor) -> float:
    try:
        if valor is None or str(valor).strip() == "": return 0.0
        limpio = str(valor).replace("$", "").replace(",", "").strip()
        return float(limpio)
    except Exception:
        return 0.0

def _safe_float_opcional(valor) -> Optional[float]:
    if valor is None or str(valor).strip() == "": return None
    resultado = _safe_float_importacion(valor)
    return resultado if resultado > 0 else None

@router.post("/api/importar_inventario")
async def importar_inventario(datos: ImportarInventarioRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from rapidfuzz import process, fuzz
    logger.info(f"📄 [TRACE:{trace_id}] Importando {len(datos.inventario)} artículos para {_sesion}")
    try:
        if not datos.inventario:
            raise HTTPException(status_code=400, detail="El lote de inventario está vacío.")
        if len(datos.inventario) > 1000:
            raise HTTPException(status_code=413, detail="Demasiados artículos en un solo lote (máximo 1000).")

        # 🆕 BLINDAJE COMPLETO + ACTUALIZAR EN VEZ DE DUPLICAR — antes esta
        # ruta no tenía NINGUNA de las protecciones del Alta manual, y
        # siempre creaba productos nuevos sin importar si la fila ya
        # existía. Ahora: exportar inventario → editar precio/cantidad en
        # Excel → reimportar actualiza el producto original (vía su "id"),
        # en vez de crear un duplicado.
        vid_str = str(_sesion)
        res_existente = await asyncio.wait_for(
            async_db_execute(supabase.table('inventario').select('id, nombre, codigo_barras').eq('vendedor_id', vid_str)),
            timeout=15.0
        )
        existentes_por_id = {}
        for r in (res_existente.data or []):
            rid = r.get('id')
            if rid is not None:
                existentes_por_id[int(rid)] = r
        # 🛡️ FIX: esta lista se actualiza DENTRO del ciclo (ver abajo) — si no,
        # dos filas DEL MISMO CSV (ej. "SIREN" y "siren") nunca se comparaban
        # entre sí, solo contra lo que ya estaba en la base. Ahora compara
        # contra TODO lo visto hasta ese punto, sea de la base o del mismo lote.
        nombres_acumulados = [str(r.get('nombre', '')) for r in (res_existente.data or []) if r.get('nombre')]
        codigos_existentes_global = {str(r['codigo_barras']).strip() for r in (res_existente.data or []) if r.get('codigo_barras')}

        filas_para_insertar = []
        filas_para_actualizar = []       # [{"id": int, "campos": {...}}]
        advertencias_similares = []      # posibles duplicados por nombre parecido (solo aplica a productos NUEVOS) — NO bloquea, solo se reporta
        omitidos_codigo_duplicado = []   # código de barras repetido (en BD o dentro del mismo CSV) — SÍ se omite, porque insertarlo tronaría el lote completo
        ids_no_reconocidos = []          # la fila trae un "id" pero no es de este vendedor o no existe — se trata como producto nuevo, pero se avisa por si fue un error de captura
        codigos_vistos_en_lote = set()

        for idx, item in enumerate(datos.inventario):
            fila_num = idx + 2  # +2: fila 1 es la cabecera del CSV, así el número coincide con lo que el usuario ve en Excel/Sheets
            # 🆕 Capitalización — mismo criterio que el Alta manual del Visor,
            # para que "siren" importado por CSV y "Siren" capturado a mano no
            # se vuelvan productos distintos por la diferencia de mayúsculas.
            nombre = bleach.clean(str(item.get("nombre", "")).strip(), tags=[], strip=True)[:200].title()
            if not nombre:
                continue

            # 🆕 ¿Esta fila trae un "id" que de verdad es de este vendedor?
            # Si sí, es una actualización del producto existente, no un alta.
            id_crudo = str(item.get("id", "")).strip()
            es_actualizacion = False
            id_objetivo = None
            registro_existente = None
            if id_crudo != "":
                try:
                    id_candidato = int(float(id_crudo))  # tolera "123" y "123.0" (Excel a veces guarda enteros como flotantes)
                except (ValueError, TypeError):
                    id_candidato = None
                if id_candidato is not None and id_candidato in existentes_por_id:
                    es_actualizacion = True
                    id_objetivo = id_candidato
                    registro_existente = existentes_por_id[id_candidato]
                elif id_candidato is not None:
                    ids_no_reconocidos.append({"fila": fila_num, "nombre": nombre, "id": id_candidato})

            codigo = bleach.clean(str(item.get("codigo_barras", "")).strip(), tags=[], strip=True)[:100] or None
            if codigo:
                # Si es una actualización y el código es el MISMO que ya
                # tenía ese producto, no es un conflicto — es su propio dato.
                codigo_previo = str(registro_existente.get('codigo_barras', '') or '').strip() if registro_existente else None
                es_su_propio_codigo = es_actualizacion and codigo == codigo_previo
                if not es_su_propio_codigo and (codigo in codigos_existentes_global or codigo in codigos_vistos_en_lote):
                    omitidos_codigo_duplicado.append({"fila": fila_num, "nombre": nombre, "codigo_barras": codigo})
                    continue  # esta fila específica se omite — el resto del lote sigue su curso normal
                codigos_vistos_en_lote.add(codigo)

            # 🆕 Mismo umbral y librería (rapidfuzz) que ya usa el RAG del bot.
            # Solo aplica a productos NUEVOS — un producto que se está
            # actualizando lógicamente "se parece" a sí mismo, eso no aporta
            # nada como advertencia.
            if not es_actualizacion:
                if nombres_acumulados:
                    match = process.extractOne(nombre, nombres_acumulados, scorer=fuzz.WRatio)
                    if match and match[1] >= 80:
                        advertencias_similares.append({"fila": fila_num, "nombre_importado": nombre, "parecido_a_existente": match[0], "similitud": round(match[1], 1)})
                nombres_acumulados.append(nombre)

            campos = {
                "nombre": nombre,
                "categoria": bleach.clean(str(item.get("categoria", "")).strip(), tags=[], strip=True)[:100] or "General",
                "tipo_producto": bleach.clean(str(item.get("tipo_producto", "")).strip(), tags=[], strip=True)[:100] or None,
                "genero": bleach.clean(str(item.get("genero", "")).strip(), tags=[], strip=True)[:100] or None,
                "estado_general": bleach.clean(str(item.get("estado_general", "")).strip(), tags=[], strip=True)[:100] or None,
                "descripcion_detallada": bleach.clean(str(item.get("descripcion_detallada", "")).strip(), tags=[], strip=True)[:2000],
                "precio": _safe_float_importacion(item.get("precio")),
                "costo": _safe_float_importacion(item.get("costo")),
                "stock": max(0, int(_safe_float_importacion(item.get("stock")))),
                "codigo_barras": codigo,
                "precio_min_inmediato": _safe_float_opcional(item.get("precio_min_inmediato")),
                "precio_min_24h": _safe_float_opcional(item.get("precio_min_24h")),
                "precio_min_72h": _safe_float_opcional(item.get("precio_min_72h")),
                "atributos_extra": item.get("atributos_extra", {}) or {},
            }

            if es_actualizacion:
                filas_para_actualizar.append({"id": id_objetivo, "campos": campos})
            else:
                campos["vendedor_id"] = vid_str
                filas_para_insertar.append(campos)

        if not filas_para_insertar and not filas_para_actualizar:
            raise HTTPException(status_code=400, detail="Ningún artículo tenía nombre válido, o todos chocaban por código de barras duplicado.")

        insertados = 0
        actualizados = 0

        if filas_para_insertar:
            # 🆕 Tope de inventario — se cuenta SOLO lo que se va a INSERTAR
            # (filas nuevas), no lo que se va a actualizar (eso no crece el
            # total). Se revisa antes de insertar nada del lote completo.
            await _verificar_cupo_inventario(vid_str, len(filas_para_insertar), trace_id)
            # FIX FASE 1: allow_retry=False por inserción masiva (INSERT)
            resultado = await asyncio.wait_for(
                async_db_execute(supabase.table('inventario').insert(filas_para_insertar), allow_retry=False),
                timeout=20.0
            )
            insertados = len(resultado.data) if resultado.data else len(filas_para_insertar)

        if filas_para_actualizar:
            # 🛡️ Supabase no permite "actualizar muchas filas con valores
            # distintos cada una" en una sola llamada — cada producto se
            # actualiza por separado, pero en paralelo (no uno por uno en
            # serie) para que un lote de cientos de actualizaciones no tarde
            # minutos.
            async def _actualizar_uno(fila):
                return await async_db_execute(
                    supabase.table('inventario').update(fila["campos"]).eq('id', fila["id"]).eq('vendedor_id', vid_str),
                    allow_retry=False
                )
            resultados_update = await asyncio.wait_for(
                asyncio.gather(*[_actualizar_uno(f) for f in filas_para_actualizar], return_exceptions=True),
                timeout=30.0
            )
            for r in resultados_update:
                if isinstance(r, Exception):
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Falló la actualización de un producto durante importación: {r}")
                else:
                    actualizados += 1

        logger.info(f"✅ [TRACE:{trace_id}] Importación completa: {insertados} nuevos, {actualizados} actualizados, {len(omitidos_codigo_duplicado)} omitidos por código duplicado, {len(advertencias_similares)} posibles duplicados por nombre.")
        return {
            "status": "ok",
            "insertados": insertados,
            "actualizados": actualizados,
            "omitidos_codigo_barras_duplicado": omitidos_codigo_duplicado,
            "advertencias_posibles_duplicados": advertencias_similares,
            "ids_no_reconocidos": ids_no_reconocidos,
        }
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en importar_inventario: {e}")
        raise HTTPException(status_code=500, detail="Error al importar el inventario.")

# ==========================================================
# 💰 15. PRECIO DE MERCADO (PRICECHARTING) — recuperado del monolito
# ==========================================================
# Godot lo llama desde PanelVideojuegos.gd (_pedir_precios_sugeridos) al
# buscar un juego o cambiar su estado físico. Antes esta ruta no existía en
# absoluto en el backend modular — ver auditoría de ai_auditor_scraper.py
# para la lógica completa de búsqueda y parseo.
@router.get("/api/consultar_precio")
async def consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "", dias_inventario: int = 0, rareza: str = "comun", _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from ai_auditor_scraper import consultar_precio_pricecharting
    logger.info(f"🎮 [TRACE:{trace_id}] Consulta de precio de mercado: '{nombre}' ({consola}) para {_sesion}")
    try:
        # 🛡️ Igual que en /api/bot_config: el 'vendedor_id' del query string es
        # solo informativo para logs — la identidad real siempre es _sesion.
        return await consultar_precio_pricecharting(nombre, consola, str(_sesion), dias_inventario, rareza)
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en consultar_precio: {e}")
        raise HTTPException(status_code=500, detail="Error interno al consultar el precio de mercado.")
