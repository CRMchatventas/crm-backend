# ==============================================================================
# 🚀 MÓDULO: main.py (ENTRYPOINT - VELTRIX ENGINE MULTI-TENANT 20.3.AAA)
# ==============================================================================
# FIX 20.2 → 20.3:
# 1. app.include_router(db_router) — sin esto, TODOS los endpoints de Godot
#    (/api/cargar_todo, /api/actualizar_estado, /api/login, etc.) daban 404.
# 2. El webhook ahora delega a procesar_respuesta_bot() en vez de reimplementar
#    su propio pipeline incompleto. Esto restaura: actualizar_estado_crm
#    (las tarjetas ya se mueven de columna), enviar_alerta_whatsapp_admin
#    (el vendedor se entera de ventas/handoffs), detectar_prompt_injection,
#    y búsqueda de portada para enviar imagen junto con la respuesta.
# 3. Se agrega lectura de columna_actual real desde 'prospectos' antes de
#    procesar — antes se ignoraba por completo, lo que podía resetear
#    incorrectamente el flujo de remarketing en cada mensaje.
# 4. Se extrae nombre_cliente del payload de Meta (contacts[0].profile.name)
#    para los resúmenes de handoff y alertas al vendedor.
# 5. gestionar_historial_db() eliminado: duplicaba lo que ya hacen
#    guardar_mensaje_chat() y procesar_respuesta_bot() internamente.
# ==============================================================================

import os
import uvicorn
from fastapi import FastAPI, Request, Response
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

# 🔌 3. FIX CRÍTICO: lógica de negocio completa (CRM + IA + alertas)
#    y el router con TODOS los endpoints que usa Godot.
from db_api_endpoints import procesar_respuesta_bot, router as db_router

logger = config.logger

# ==========================================================
# ⚙️ CONFIGURACIÓN DE TUS SHOWROOMS (Pendiente a migrar a Supabase)
# Agrega aquí una entrada por cada número de Meta que conectes.
# Hoy: solo Fantasygames. Cuando tengas los números de alarmas y
# terrenos, copia el bloque y cambia phone_number_id + datos.
# ==========================================================
CONFIGURACION_SHOWROOMS = {
    "1100616133134501": {
        "vendedor_id": "V-FANTASY-001",
        "giro_comercial": "Videojuegos y Consolas Seminuevas",
        "nombre_negocio": "Fantasygames",
        "tono_ia": "Gamer, experto en ventas, muy persuasivo pero honesto. Experto en leer comprobantes de pago.",
        "objetivo_ventas_diario": 3000,
        "objetivo_veltrix_diario": 5,
        "permitir_descuentos_ia": True,
        "max_descuento_ia": 15,
        "whatsapp_token": os.getenv("WHATSAPP_TOKEN", "").strip(),
        # procesar_respuesta_bot lee meta_token / meta_phone_id de config:
        "meta_token": os.getenv("WHATSAPP_TOKEN", "").strip(),
        "meta_phone_id": "1100616133134501",
    },
    # 🟡 Plantilla para cuando tengas el número de Meta de Alarmas:
    # "TU_PHONE_NUMBER_ID_ALARMAS": {
    #     "vendedor_id": "V-ALARMAS-001",
    #     "giro_comercial": "Sistemas de Seguridad",
    #     "nombre_negocio": "TuNegocioAlarmas",
    #     "tono_ia": "Profesional, enfocado en seguridad y confianza.",
    #     "objetivo_ventas_diario": 3000,
    #     "objetivo_veltrix_diario": 5,
    #     "permitir_descuentos_ia": True,
    #     "max_descuento_ia": 10,
    #     "whatsapp_token": os.getenv("WHATSAPP_TOKEN_ALARMAS", "").strip(),
    #     "meta_token": os.getenv("WHATSAPP_TOKEN_ALARMAS", "").strip(),
    #     "meta_phone_id": "TU_PHONE_NUMBER_ID_ALARMAS",
    # },
}

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
# FIX CRÍTICO #2: ahora delega el pipeline completo a procesar_respuesta_bot()
# ==========================================================
@app.post("/webhook")
async def recibir_mensajes_whatsapp(request: Request):
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

                config_bot = CONFIGURACION_SHOWROOMS.get(numero_destino_id)
                if not config_bot:
                    logger.error(f"❌ [WEBHOOK ROUTING FATAL] Recibido mensaje para tenant NO configurado: {numero_destino_id}")
                    continue

                vendedor_id = config_bot["vendedor_id"]
                token_meta = config_bot.get("whatsapp_token") or config_bot.get("meta_token")

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

                    # 📥 DESCARGA MULTIMEDIA (sin cambios)
                    media_dict = None
                    if media_id:
                        logger.info(f"📥 Descargando multimedia (ID: {media_id})...")
                        media_dict = await descargar_media_whatsapp_async(media_id, token_meta)

                    # 🆕 Columna actual real del prospecto en el Kanban.
                    # Antes este dato se ignoraba; sin él, procesar_respuesta_bot
                    # no sabría si el cliente ya estaba en "Con Descuento" o
                    # "Requiere Asistencia", y podría tratarlo como nuevo siempre.
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
                        logger.warning(f"⚠️ [WEBHOOK] No se pudo leer columna actual, usando default: {e}")
                        columna_actual = "Bandeja Nueva"

                    # 🗄️ Guarda el mensaje entrante del cliente (dedup atómico por WAMID en BD)
                    await guardar_mensaje_chat(telefono_cliente, vendedor_id, "CLIENTE", texto_cliente, wamid)

                    # 🤖 FIX CRÍTICO: pipeline completo de negocio.
                    # Antes este bloque solo llamaba a Gemini y respondía.
                    # Ahora incluye: anti prompt-injection, actualización real
                    # del CRM (mueve la tarjeta de columna), alerta al teléfono
                    # personal del vendedor cuando hay venta o se necesita
                    # atención humana, y búsqueda de portada del producto.
                    await procesar_respuesta_bot(
                        cliente=nombre_cliente,
                        telefono=telefono_cliente,
                        texto_entrante=texto_cliente,
                        columna_actual=columna_actual,
                        config=config_bot,
                        media_dict=media_dict,
                        id_mensaje_meta=wamid
                    )

        return {"status": "ok"}

    except Exception as e:
        logger.exception(f"❌ [WEBHOOK ERROR FATAL] Excepción no controlada en el procesamiento del Webhook: {e}")
        return {"status": "ok"}


if __name__ == "__main__":
    puerto = int(os.environ.get("PORT", 8000))
    logger.info(f"🚀 Iniciando servidor Uvicorn en el puerto {puerto}")
    uvicorn.run("main:app", host="0.0.0.0", port=puerto, reload=config.MODO_LABORATORIO)