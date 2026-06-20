# ==============================================================================
# 🚀 MÓDULO: main.py (ENTRYPOINT - VELTRIX ENGINE MULTI-TENANT 20.5.AAA)
# ==============================================================================
# FIX 20.2 → 20.3: ver historial previo (router include, procesar_respuesta_bot,
# columna_actual real, nombre_cliente desde payload, gestionar_historial_db eliminado).
#
# FIX 20.3 → 20.4 (CRÍTICO):
# Meta solo espera ~5s por el 200 OK del webhook; si tarda más lo marca
# fallido y reintenta, y tras 5 fallos seguidos desactiva el webhook.
# El pipeline de IA (Gemini + CRM + WhatsApp) tarda 10-13s+, así que NUNCA
# puede correr antes de responder. Ahora el webhook responde casi de
# inmediato y todo el trabajo lento se agenda con BackgroundTasks para
# correr DESPUÉS de responder. Es agnóstico de giro: no toca nada
# específico de videojuegos, alarmas o terrenos.
#
# FIX 20.4 → 20.5 (CRÍTICO — MULTIGIRO REAL):
# CONFIGURACION_SHOWROOMS era un diccionario de Python hardcodeado (estaba
# literalmente comentado como "Pendiente a migrar a Supabase"). Dar de alta
# un tenant nuevo requería editar código y redesplegar — y aunque se diera de
# alta, sus llaves (giro_comercial, objetivo_ventas_diario,
# permitir_descuentos_ia, max_descuento_ia) no coincidían con lo que
# analizar_intencion_venta_ia espera (giro, meta_venta, permitir_desc,
# desc_max), así que ni el tenant ya configurado recibía sus datos reales.
# Ahora se consulta configuracion_bot por meta_phone_id en tiempo real
# (con caché de 2 min). Dar de alta un negocio nuevo es una fila en
# Supabase, no una edición de código.
# ==============================================================================


import os
import uvicorn
from fastapi import FastAPI, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse

# 🔌 1. Seguridad y Configuración Centralizada
import config_and_schemas as config
from ai_security_utils import (
    lifespan,
    configurar_middlewares_seguridad,
    validar_firma_meta
)

# 🔌 2. Media y acceso a BD (solo lo que main.py necesita directamente)
from ai_whatsapp_media import descargar_media_whatsapp_async
from db_core_wrapper import async_db_execute, supabase
from db_chat import guardar_mensaje_chat

# 🔌 3. FIX CRÍTICO: lógica de negocio completa (CRM + IA + alertas),
#    la configuración dinámica de tenants, y el router con TODOS los
#    endpoints que usa Godot.
from db_api_endpoints import (
    procesar_respuesta_bot,
    obtener_config_bot_por_phone_id,
    router as db_router
)

logger = config.logger

# ==========================================================
# 🚀 INICIALIZACIÓN DE FASTAPI
# ==========================================================
app = FastAPI(
    title="Veltrix Engine Enterprise",
    version=config.SCHEMA_VERSION,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None
)

configurar_middlewares_seguridad(app)

# 🔗 FIX CRÍTICO #1: sin esto, Godot recibía 404 en TODOS los endpoints
# (/api/cargar_todo, /api/actualizar_estado, /api/login, /api/mover_prospecto,
# /api/mobile/*, /api/mensaje_masivo, /api/actualizar_stock, /stats, /leads...)
# porque el router de db_api_endpoints.py nunca estaba montado en la app.
app.include_router(db_router)


# ==========================================================
# 🆕 PROCESAMIENTO EN SEGUNDO PLANO (FIX CRÍTICO #4)
# Meta solo espera ~5s para considerar el webhook recibido; si tarda más,
# lo marca como fallido y reintenta (y tras 5 fallos seguidos, desactiva
# el webhook). El pipeline de IA tarda 10-13s+, así que NUNCA puede correr
# antes de responder. Esta función corre DESPUÉS de que ya respondimos.
# Es agnóstica de giro: no contiene nada específico de videojuegos.
# ==========================================================
async def _procesar_mensaje_en_segundo_plano(
    telefono_cliente: str, vendedor_id: str, texto_cliente: str,
    media_id: str, token_meta: str, config_bot: dict,
    nombre_cliente: str, wamid: str
):
    try:
        media_dict = None
        if media_id:
            logger.info(f"📥 [BG] Descargando multimedia (ID: {media_id})...")
            media_dict = await descargar_media_whatsapp_async(media_id, token_meta)

        try:
            res_col = await async_db_execute(
                supabase.table('prospectos').select('fila')
                .eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_id).limit(1)
            )
            columna_actual = (
                res_col.data[0].get('fila', 'Bandeja Nueva')
                if res_col.data else "Bandeja Nueva"
            )
        except Exception as e:
            logger.warning(f"⚠️ [BG] No se pudo leer columna actual, usando default: {e}")
            columna_actual = "Bandeja Nueva"

        await guardar_mensaje_chat(telefono_cliente, vendedor_id, "CLIENTE", texto_cliente, wamid)

        await procesar_respuesta_bot(
            cliente=nombre_cliente,
            telefono=telefono_cliente,
            texto_entrante=texto_cliente,
            columna_actual=columna_actual,
            config=config_bot,
            media_dict=media_dict,
            id_mensaje_meta=wamid
        )
    except Exception as e:
        logger.exception(f"❌ [BG] Error no controlado procesando mensaje en segundo plano: {e}")


@app.get("/")
async def root():
    return {"status": "Veltrix Engine Online", "motor_db": "Conectado", "version": config.SCHEMA_VERSION}


@app.get("/health")
async def health_check():
    return {"status": "ok", "memory_status": "stable"}


# ==========================================================
# 🔵 WEBHOOK DE META - VERIFICACIÓN (sin cambios)
# ==========================================================
@app.get("/webhook")
async def verificar_webhook_meta(request: Request):
    logger.info("🔵 [META WEBHOOK] Intento de verificación de Meta...")
    hub_mode = request.query_params.get("hub.mode")
    hub_challenge = request.query_params.get("hub.challenge")
    hub_verify_token = request.query_params.get("hub.verify_token")
    TOKEN_VERIFICACION_META = os.getenv("META_VERIFY_TOKEN", "").strip()

    if hub_mode == "subscribe" and hub_verify_token == TOKEN_VERIFICACION_META:
        logger.info("✅ [META WEBHOOK] Verificación exitosa. Webhook conectado.")
        return Response(content=hub_challenge, media_type="text/plain")

    logger.error("❌ [META WEBHOOK SECURITY] Fallo de verificación. El hub_verify_token no coincide.")
    return JSONResponse(content={"error": "Token de verificación inválido"}, status_code=403)


# ==========================================================
# 🟢 WEBHOOK DE META - RECEPCIÓN Y PROCESAMIENTO
# FIX 20.5: el routing de tenant ya no usa un diccionario hardcodeado —
# consulta configuracion_bot por meta_phone_id en tiempo real.
# ==========================================================
@app.post("/webhook")
async def recibir_mensajes_whatsapp(request: Request, background_tasks: BackgroundTasks):
    try:
        await validar_firma_meta(request)
    except Exception as e:
        logger.error(f"❌ [META WEBHOOK SECURITY] Firma criptográfica inválida o ausente: {e}")
        return JSONResponse(content={"status": "error", "detail": "Invalid signature"}, status_code=403)

    try:
        payload = await request.json()

        entradas = payload.get("entry", [])
        for entrada in entradas:
            cambios = entrada.get("changes", [])
            for cambio in cambios:
                valor = cambio.get("value", {})

                numero_destino_id = valor.get("metadata", {}).get("phone_number_id")
                if not numero_destino_id:
                    continue

                # 🔧 FIX 20.5: ya no se busca en un diccionario hardcodeado — se
                # consulta configuracion_bot por meta_phone_id (con caché de 2 min).
                config_bot = await obtener_config_bot_por_phone_id(numero_destino_id)
                if not config_bot:
                    logger.error(f"❌ [WEBHOOK ROUTING FATAL] Recibido mensaje para tenant NO configurado: {numero_destino_id}")
                    continue

                vendedor_id = config_bot["vendedor_id"]
                token_meta = config_bot.get("meta_token")

                if not token_meta:
                    logger.error(f"❌ [WEBHOOK CONFIG ERROR] Tenant {vendedor_id} no tiene un token de WhatsApp configurado.")
                    continue

                # 🆕 Nombre del cliente (Meta lo manda en contacts[0].profile.name)
                # Se usa en resúmenes de handoff y en la alerta al vendedor.
                contactos = valor.get("contacts", [])
                nombre_cliente = (
                    contactos[0].get("profile", {}).get("name", "Cliente")
                    if contactos else "Cliente"
                )

                mensajes = valor.get("messages", [])
                for mensaje_cliente in mensajes:

                    # Replay Protection usando WAMID (sin cambios)
                    wamid = mensaje_cliente.get("id")
                    if wamid:
                        if wamid in config.WEBHOOK_REPLAY_CACHE:
                            logger.info(f"♻️ [REPLAY PROTECTION] Mensaje duplicado de Meta ignorado (WAMID: {wamid[:8]}...).")
                            continue
                        config.WEBHOOK_REPLAY_CACHE[wamid] = True

                    telefono_cliente = mensaje_cliente.get("from")
                    tipo_mensaje = mensaje_cliente.get("type", "text")
                    texto_cliente, media_id = "", None

                    # 🧠 EXTRACCIÓN INTELIGENTE (sin cambios)
                    if tipo_mensaje == "text":
                        texto_cliente = mensaje_cliente.get("text", {}).get("body", "")
                    elif tipo_mensaje in ["image", "audio", "video", "document"]:
                        media_obj = mensaje_cliente.get(tipo_mensaje, {})
                        media_id = media_obj.get("id")
                        caption = media_obj.get("caption", "")

                        if tipo_mensaje == "image":
                            texto_cliente = caption if caption else "[El cliente envió una imagen/comprobante de pago. Analízala.]"
                        elif tipo_mensaje == "audio":
                            texto_cliente = "[El cliente envió una nota de voz. Escúchala y responde.]"
                        else:
                            texto_cliente = f"[El cliente envió un archivo {tipo_mensaje}.]"

                    elif tipo_mensaje == "interactive":
                        inter = mensaje_cliente.get("interactive", {})
                        if inter.get("type") == "button_reply":
                            texto_cliente = inter.get("button_reply", {}).get("title", "")
                        elif inter.get("type") == "list_reply":
                            texto_cliente = inter.get("list_reply", {}).get("title", "")

                    if not texto_cliente and not media_id:
                        continue

                    # 🆕 FIX CRÍTICO: se agenda en segundo plano en vez de
                    # esperarlo aquí. Meta ya recibe su 200 OK abajo casi
                    # de inmediato; la descarga de medios, lectura de CRM,
                    # IA, actualización de Kanban y alerta al vendedor
                    # corren después, sin que Meta tenga que esperarlas.
                    background_tasks.add_task(
                        _procesar_mensaje_en_segundo_plano,
                        telefono_cliente=telefono_cliente,
                        vendedor_id=vendedor_id,
                        texto_cliente=texto_cliente,
                        media_id=media_id,
                        token_meta=token_meta,
                        config_bot=config_bot,
                        nombre_cliente=nombre_cliente,
                        wamid=wamid
                    )

        return {"status": "ok"}

    except Exception as e:
        logger.exception(f"❌ [WEBHOOK ERROR FATAL] Excepción no controlada en el procesamiento del Webhook: {e}")
        return {"status": "ok"}


if __name__ == "__main__":
    puerto = int(os.environ.get("PORT", 8000))
    logger.info(f"🚀 Iniciando servidor Uvicorn en el puerto {puerto}")
    uvicorn.run("main:app", host="0.0.0.0", port=puerto, reload=config.MODO_LABORATORIO)

