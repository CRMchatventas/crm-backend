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
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

# ==========================================================
# 🔌 IMPORTACIONES ESTRUCTURADAS (SSOT)
# ==========================================================
from config_and_schemas import (
    logger, get_lock, mensajes_procesados_meta, procesados_recientemente,
    JWT_SECRET, DUMMY_HASH, pwd_context, LoginUpdate, LeadAction, EstadoUpdate,
    BorrarRequest, NotasUpdate, NuevoArticulo, VentaItem,
    MobileMessageRequest, sanitizar_nombre_columna, ReordenarColumnasAction,
    ColumnaAction, RenombrarColumnaAction, BorrarColumnaAction, BotConfigUpdate, RESERVAS_TEMPORALES_ULTIMA_UNIDAD
)
from ai_security_utils import verificar_sesion_b2b
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
            elif intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO", "PAGO_RECIBIDO"]:
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            elif intencion_ia == "COMPRA":
                nueva_columna, iluminacion = "Por Entregar", "verde_exito"

            resultados_gather = await asyncio.gather(
                actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, producto_detectado, perfil_ia=perfil_actualizado, nombre=cliente),
                guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final),
                return_exceptions=True
            )
            for r in resultados_gather:
                if isinstance(r, Exception): logger.error(f"❌ [TRACE:{trace_id}] Tarea asíncrona fallida en CRM/Chat: {r}")

            url_imagen = None
            if producto_detectado:
                try:
                    # FIX FASE 4: Se añade ordenamiento por relevancia y se mitiga el comodín masivo
                    res_juego = await async_db_execute(
                        supabase.table('inventario').select('url_portada').ilike('nombre', f'%{producto_detectado}%')
                        .eq('vendedor_id', vendedor_id).order('stock', desc=True).limit(1)
                    )
                    if res_juego.data and res_juego.data[0].get('url_portada'): url_imagen = res_juego.data[0]['url_portada']
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
            "datos": {"vendedor_id": vendedor_id, "email": usuario['email'], "nombre": usuario.get('nombre_contacto', 'Vendedor'), "rol": usuario.get('rol', 'vendedor')}
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
                supabase.table("mensajes_chat").select("mensaje, autor, created_at").eq("vendedor_id", str(vendedor_id)).eq("telefono", tel_norm)
                .order("created_at", desc=True).range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=8.0
        )
        historial_formateado = [
            {
                "contenido": bleach.clean(str(m.get("mensaje") or ""), tags=[], strip=True),
                "es_mio": str(m.get("autor", "")).upper() in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"],
                "fecha": str(m.get("created_at", ""))
            } for m in reversed(res.data or [])
        ]
        return {"status": "ok", "historial": historial_formateado}
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
        await guardar_mensaje_chat(tel_norm, str(vendedor_id), 'ASESOR', mensaje_limpio)
        await actualizar_estado_crm(tel_norm, str(vendedor_id), "En Conversacion", "azul", "")

        # Efecto secundario (Llamada HTTP externa a la API de Meta)
        await disparar_whatsapp_dinamico_async(tel_norm, mensaje_limpio, config.get('meta_token'), config.get('meta_phone_id'))
        return {"status": "ok", "message": "Enviado"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error retransmitiendo handoff manual: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al despachar el mensaje.")

@router.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Compilando Dashboard Móvil para {vendedor_id}")
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        ventas_res = await asyncio.wait_for(async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio)), timeout=10.0)
        total_hoy = sum((float(v.get("monto") or 0.0) for v in (ventas_res.data or [])))

        prospectos_res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("prospectos").select("id, nombre, telefono, fila, ultima_interaccion_ia, ultimo_msj")
                .eq("vendedor_id", str(vendedor_id)).order("ultima_interaccion_ia", desc=True).limit(50)
            ),
            timeout=8.0
        )
        prospectos_limpios = [
            {
                "id": p.get("id"), "nombre": bleach.clean(p.get("nombre") or "Cliente", tags=[], strip=True),
                "telefono": normalizar_telefono(p.get("telefono", "")), "fila": sanitizar_nombre_columna(p.get("fila") or "Bandeja Nueva"),
                "ultima_interaccion_ia": p.get("ultima_interaccion_ia") or "", "ultimo_msj": bleach.clean(p.get("ultimo_msj") or "", tags=[], strip=True)
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
    id: Any
    nombre: Optional[str] = ""
    consola: Optional[str] = ""
    precio: float = 0.0
    stock: int = 0

class BorrarItemRequest(BaseModel):
    id: Any
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
            "genero": bleach.clean(datos.genero.strip(), tags=[], strip=True)[:100] if datos.genero.strip() else None,
            "estado_general": bleach.clean(datos.estado_general.strip(), tags=[], strip=True)[:100] if datos.estado_general.strip() else None,
            "precio": datos.precio,
            "costo": costo_final,
            "stock": datos.stock,
            "precio_minimo_bot": datos.precio_minimo_bot,
            "codigo_barras": bleach.clean(datos.codigo_barras.strip(), tags=[], strip=True)[:100] if datos.codigo_barras.strip() else None,
            "url_portada": datos.url_portada.strip()[:500] if datos.url_portada.strip() else None,
            "descripcion_detallada": bleach.clean(datos.descripcion_detallada.strip(), tags=[], strip=True)[:2000],
            "atributos_extra": datos.atributos_extra or {},
        }
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
        res = await asyncio.wait_for(async_db_execute(supabase.table("inventario").select("*").eq("vendedor_id", str(vendedor_id)).order("nombre").limit(500)), timeout=12.0)
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
    "nombre", "plataforma", "genero", "estado_general", "condicion",
    "precio_compra", "precio_venta", "cantidad", "codigo_barras",
    "precio_min_inmediato", "precio_min_24h", "precio_min_72h"
]
COLUMNAS_CSV_GENERICO = ["nombre", "categoria", "precio_compra", "precio_venta", "cantidad", "descripcion"]

EJEMPLO_CSV_VIDEOJUEGOS = ["Batman Arkham Knight", "PS4", "Acción", "Completo", "Excelente estado", "300", "550", "1", "", "500", "450", "400"]
EJEMPLO_CSV_GENERICO = ["Producto de ejemplo", "General", "100", "200", "1", "Descripción breve"]

@router.get("/api/descargar_plantilla")
async def descargar_plantilla(_sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from fastapi.responses import Response
    import csv, io as io_csv
    logger.info(f"📄 [TRACE:{trace_id}] Generando plantilla CSV de inventario para {_sesion}")
    try:
        res_conf = await asyncio.wait_for(async_db_execute(supabase.table('configuracion_bot').select('giro').eq('vendedor_id', str(_sesion)).limit(1)), timeout=5.0)
        giro = str(res_conf.data[0].get('giro', '')).lower() if res_conf.data else ""

        es_videojuegos = "videojueg" in giro or giro == ""  # default a videojuegos si no hay giro configurado aún
        columnas = COLUMNAS_CSV_VIDEOJUEGOS if es_videojuegos else COLUMNAS_CSV_GENERICO
        ejemplo = EJEMPLO_CSV_VIDEOJUEGOS if es_videojuegos else EJEMPLO_CSV_GENERICO

        buffer = io_csv.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(columnas)
        writer.writerow(ejemplo)
        contenido = buffer.getvalue()

        return Response(
            content=contenido,
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=Plantilla_Veltrix_Importar.csv"}
        )
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo generando plantilla CSV: {e}")
        raise HTTPException(status_code=500, detail="Error al generar la plantilla.")

class ImportarInventarioRequest(BaseModel):
    vendedor_id: str = ""
    inventario: List[dict] = Field(default_factory=list)

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
    logger.info(f"📄 [TRACE:{trace_id}] Importando {len(datos.inventario)} artículos para {_sesion}")
    try:
        if not datos.inventario:
            raise HTTPException(status_code=400, detail="El lote de inventario está vacío.")
        if len(datos.inventario) > 1000:
            raise HTTPException(status_code=413, detail="Demasiados artículos en un solo lote (máximo 1000).")

        filas_finales = []
        for item in datos.inventario:
            nombre = bleach.clean(str(item.get("nombre", "")).strip(), tags=[], strip=True)[:200]
            if not nombre:
                continue
            filas_finales.append({
                "vendedor_id": str(_sesion),
                "nombre": nombre,
                "categoria": bleach.clean(str(item.get("categoria", "")).strip(), tags=[], strip=True)[:100] or "General",
                "genero": bleach.clean(str(item.get("genero", "")).strip(), tags=[], strip=True)[:100] or None,
                "estado_general": bleach.clean(str(item.get("estado_general", "")).strip(), tags=[], strip=True)[:100] or None,
                "descripcion_detallada": bleach.clean(str(item.get("descripcion_detallada", "")).strip(), tags=[], strip=True)[:2000],
                "precio": _safe_float_importacion(item.get("precio")),
                "costo": _safe_float_importacion(item.get("costo")),
                "stock": max(0, int(_safe_float_importacion(item.get("stock")))),
                "codigo_barras": bleach.clean(str(item.get("codigo_barras", "")).strip(), tags=[], strip=True)[:100] or None,
                "precio_min_inmediato": _safe_float_opcional(item.get("precio_min_inmediato")),
                "precio_min_24h": _safe_float_opcional(item.get("precio_min_24h")),
                "precio_min_72h": _safe_float_opcional(item.get("precio_min_72h")),
                "atributos_extra": item.get("atributos_extra", {}) or {},
            })

        if not filas_finales:
            raise HTTPException(status_code=400, detail="Ningún artículo tenía nombre válido.")

        # FIX FASE 1: allow_retry=False por inserción masiva (INSERT)
        resultado = await asyncio.wait_for(
            async_db_execute(supabase.table('inventario').insert(filas_finales), allow_retry=False),
            timeout=20.0
        )
        return {"status": "ok", "insertados": len(resultado.data) if resultado.data else len(filas_finales)}
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
