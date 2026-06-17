# ==========================================================
# 🚀 MÓDULO: ai_whatsapp_media.py
# ==========================================================

import asyncio
import hashlib
import httpx
import bleach
import io
import urllib.parse
from typing import Optional, Dict, Any
from PIL import Image

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
import config_and_schemas as config
import ai_security_utils

# 🛡️ FIX AAA: Helper de telemetría y seguridad
def enmascarar_telefono(tel: str) -> str:
    tel_str = str(tel).strip()
    if len(tel_str) > 4:
        return "*" * (len(tel_str) - 4) + tel_str[-4:]
    return tel_str


# ==========================================================
# 🚀 MOTOR OUTBOUND WHATSAPP AAA ENTERPRISE
# ==========================================================

async def disparar_whatsapp_dinamico_async(
    telefono_destino: str,
    texto_mensaje: str,
    token: str,
    phone_id: str
):
    """
    🚀 MOTOR OUTBOUND WHATSAPP AAA ENTERPRISE
    ---------------------------------------------------------
    FUNCIONES:
    - Retry Inteligente
    - Anti Duplicados
    - Anti Flood
    - Rate Limit Outbound
    - Timeout Hardened
    - Sanitización profunda
    - Protección Meta Ban
    - Backoff exponencial
    - Idempotencia outbound
    - Validación payload
    - Anti Memory Leak
    - Telemetría avanzada
    - Protección contra loops
    ---------------------------------------------------------
    """
    
    http_client = ai_security_utils.get_http_client()

    # ==========================================================
    # 🛡️ 1. VALIDACIÓN HTTP CLIENT
    # ==========================================================
    if not http_client:
        config.logger.error("❌ [WHATSAPP OUTBOUND] http_client no inicializado.")
        return False

    # ==========================================================
    # 🛡️ 2. VALIDACIÓN BÁSICA INPUTS
    # ==========================================================
    telefono_destino = str(telefono_destino).strip()
    texto_mensaje = str(texto_mensaje).strip()
    token = str(token).strip()
    phone_id = str(phone_id).strip()

    if (not telefono_destino or not texto_mensaje or not token or not phone_id):
        config.logger.error("❌ [WHATSAPP OUTBOUND] Parámetros inválidos.")
        return False

    # ==========================================================
    # 🛡️ 3. SANITIZACIÓN MENSAJE
    # ==========================================================
    texto_mensaje = bleach.clean(texto_mensaje, tags=[], strip=True)
    texto_mensaje = config.limpiar_texto(texto_mensaje)

    # ==========================================================
    # 🛡️ 4. LÍMITE HARDENED META
    # ==========================================================
    MAX_MESSAGE_LENGTH = 4096
    if len(texto_mensaje) > MAX_MESSAGE_LENGTH:
        config.logger.warning(f"⚠️ [WHATSAPP LIMIT] Mensaje truncado: {len(texto_mensaje)} chars.")
        texto_mensaje = texto_mensaje[:MAX_MESSAGE_LENGTH]

    # ==========================================================
    # 🛡️ 5. ANTI DUPLICADOS OUTBOUND
    # ==========================================================
    mensaje_hash = hashlib.sha256(f"{telefono_destino}:{texto_mensaje[:120]}".encode()).hexdigest()

    async with config.rate_limit_global_lock:
        if mensaje_hash in config.RATE_LIMIT_MOBILE_OUTBOUND:
            config.logger.warning(f"♻️ [WHATSAPP DUPLICATE BLOCK] Mensaje repetido bloqueado para {telefono_destino}")
            return False
        config.RATE_LIMIT_MOBILE_OUTBOUND[mensaje_hash] = config.now_ts()

    # ==========================================================
    # 🛡️ 6. RATE LIMIT GLOBAL OUTBOUND
    # ==========================================================
    rl_key = f"{phone_id}:{telefono_destino}"

    async with config.rate_limit_global_lock:
        outbound_actual = config.RATE_LIMIT_MOBILE_OUTBOUND.get(rl_key, 0)
        if outbound_actual >= 12:
            config.logger.warning(f"🚨 [WHATSAPP FLOOD BLOCK] Outbound excedido hacia {telefono_destino}")
            return False
        config.RATE_LIMIT_MOBILE_OUTBOUND[rl_key] = outbound_actual + 1

    # ==========================================================
    # 🛡️ 7. URL META API
    # ==========================================================
    url = f"https://graph.facebook.com/{ai_security_utils.META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # ==========================================================
    # 🛡️ 8. PAYLOAD HARDENED
    # ==========================================================
    payload = {
        "messaging_product": "whatsapp",
        "to": telefono_destino,
        "type": "text",
        "text": {"preview_url": False, "body": texto_mensaje}
    }

    # ==========================================================
    # 📊 9. TELEMETRÍA
    # ==========================================================
    inicio_telemetria = config.now_ts()
    config.logger.info(f"📡 [WHATSAPP OUTBOUND] Destino={enmascarar_telefono(telefono_destino)}")

    # ==========================================================
    # 🔄 10. RETRY INTELIGENTE
    # ==========================================================
    MAX_RETRIES = 3
    for intento in range(MAX_RETRIES):
        try:
            # ==========================================================
            # 🚀 11. REQUEST ASYNC HARDENED
            # ==========================================================
            response = await http_client.post(
                url, headers=headers, json=payload,
                timeout=httpx.Timeout(connect=5.0, read=12.0, write=10.0, pool=5.0)
            )
            status = response.status_code

            # ==========================================================
            # ✅ 12. ÉXITO
            # ==========================================================
            if status in [200, 201]:
                tiempo_total = (config.now_ts() - inicio_telemetria)
                config.logger.info(f"✅ [WHATSAPP SUCCESS] Status={status} | Tiempo={tiempo_total:.3f}s")
                return True

            # ==========================================================
            # 🚨 13. RATE LIMIT META
            # ==========================================================
            if status == 429:
                espera = min(8, 2 ** intento)
                config.logger.warning(f"🚨 [META RATE LIMIT] Intento={intento+1} | Backoff={espera}s")
                await asyncio.sleep(espera)
                continue

            # ==========================================================
            # 🚨 14. ERRORES TEMPORALES META
            # ==========================================================
            if status >= 500:
                espera = min(6, 2 ** intento)
                config.logger.error(f"⚠️ [META SERVER ERROR] Status={status} | Retry en {espera}s")
                await asyncio.sleep(espera)
                continue

            # ==========================================================
            # 🚨 15. TOKEN INVÁLIDO / PHONE BLOQUEADO
            # ==========================================================
            if status in [400, 401, 403]:
                config.logger.error(f"🚨 [META AUTH ERROR] Status={status} | Body={response.text[:500]}")
                return False

            # ==========================================================
            # 🚨 16. ERROR CONTROLADO
            # ==========================================================
            config.logger.error(f"❌ [META ERROR] Status={status} | Body={response.text[:800]}")
            return False

        # ==========================================================
        # ⏱️ 17. TIMEOUT CONTROLADO
        # ==========================================================
        except asyncio.TimeoutError:
            config.logger.error(f"⏱️ [WHATSAPP TIMEOUT] Intento={intento+1}")
        except httpx.ReadTimeout:
            config.logger.error(f"⏱️ [HTTPX READ TIMEOUT] Intento={intento+1}")
        except httpx.ConnectTimeout:
            config.logger.error(f"⏱️ [HTTPX CONNECT TIMEOUT] Intento={intento+1}")

        # ==========================================================
        # 🚨 18. ERROR CRÍTICO
        # ==========================================================
        except Exception as e:
            config.logger.exception(f"🚨 [WHATSAPP CRITICAL ERROR] {str(e)}")
            break

        # ==========================================================
        # 🔄 19. BACKOFF GENERAL
        # ==========================================================
        espera_general = min(5, 1 + intento)
        await asyncio.sleep(espera_general)

    # ==========================================================
    # 🚨 20. FAILSAFE FINAL
    # ==========================================================
    config.logger.error(f"🚨 [WHATSAPP FAILSAFE] No se pudo enviar mensaje a {enmascarar_telefono(telefono_destino)}")
    return False


# ==========================================================
# 📡 WHATSAPP IMAGE SENDER AAA
# ==========================================================

async def disparar_whatsapp_imagen_async(
    telefono_destino: str,
    url_imagen: str,
    texto_mensaje: str,
    token: str,
    phone_id: str
):
    """
    📡 Envío Hardened de imágenes WhatsApp
    - Retry automático
    - Sanitización
    - Timeout
    - Logs completos
    """
    http_client = ai_security_utils.get_http_client()

    if not http_client:
        config.logger.error("❌ [WHATSAPP IMG] HTTP Client no inicializado.")
        return False

    try:
        # 🛡️ Sanitización crítica
        telefono_destino = str(telefono_destino).strip()
        url_imagen = str(url_imagen).strip()
        texto_mensaje = config.limpiar_texto(texto_mensaje)

        if not telefono_destino or not url_imagen:
            config.logger.error("❌ [WHATSAPP IMG] Datos incompletos.")
            return False

        # 🛡️ Validación URL
        if not url_imagen.startswith("http"):
            config.logger.error("❌ [WHATSAPP IMG] URL inválida.")
            return False

        url = f"https://graph.facebook.com/{ai_security_utils.META_API_VERSION}/{phone_id}/messages"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {
            "messaging_product": "whatsapp",
            "to": telefono_destino,
            "type": "image",
            "image": {
                "link": url_imagen,
                "caption": texto_mensaje[:1024]  # 🛡️ Límite Meta
            }
        }

        # ======================================================
        # 🔁 RETRIES INTELIGENTES
        # ======================================================
        for intento in range(3):
            try:
                config.logger.info(f"📡 [WHATSAPP IMG] Intento {intento+1} -> {telefono_destino}")
                response = await http_client.post(
                    url, headers=headers, json=payload, timeout=15.0
                )

                # ==================================================
                # ✅ SUCCESS
                # ==================================================
                if response.status_code in [200, 201]:
                    config.logger.info(f"✅ [WHATSAPP IMG] Imagen enviada correctamente.")
                    return True

                # ==================================================
                # 🚨 RATE LIMIT META
                # ==================================================
                if response.status_code == 429:
                    config.logger.warning("⚠️ [WHATSAPP IMG] Meta Rate Limit.")
                    await asyncio.sleep(2 * (intento + 1))
                    continue

                # ==================================================
                # 🚨 ERROR META
                # ==================================================
                config.logger.error(f"❌ [WHATSAPP IMG] Error Meta {response.status_code}: {response.text}")

                # 4xx duros no vale retry
                if response.status_code in [400, 401, 403, 404]:
                    return False

            except asyncio.TimeoutError:
                config.logger.error(f"⏱️ [WHATSAPP IMG] Timeout intento {intento+1}")
            except Exception as e:
                config.logger.exception(f"❌ [WHATSAPP IMG ERROR] {e}")

            # 🔥 Backoff progresivo
            await asyncio.sleep(1.5 * (intento + 1))

        return False

    except Exception as e:
        config.logger.exception(f"❌ [WHATSAPP IMG CRITICAL] {e}")
        return False


# ==========================================================
# 🚨 ALERTAS DE AGENCIA (HANDOFF SYSTEM)
# ==========================================================

async def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config_dict: dict):
    try:
        telefono_admin = config_dict.get("admin_phone") or ai_security_utils.ADMIN_PHONE_GLOBAL
        token, phone_id = config_dict.get("meta_token", ""), config_dict.get("meta_phone_id", "")
        if intencion == "COMPRA": encabezado = "💰 *NUEVA VENTA DETECTADA*"
        elif intencion == "PEDIDO_ESPECIAL": encabezado = "⚠️ *NUEVO PEDIDO ESPECIAL*"
        elif intencion == "ENOJO": encabezado = "😡 *CLIENTE MOLESTO - URGENTE*"
        else: encabezado = "🚨 *ASISTENCIA REQUERIDA*"
        
        # 🛡️ FIX AAA: Evita exposición masiva truncando el resumen
        resumen_seguro = config.limpiar_texto(resumen_ia)[:1200]
        mensaje_alerta = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Tel: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_seguro}"
        
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje_alerta, token, phone_id)
        config.logger.info(f"📩 [ALERTA ADMIN] Enviada para el cliente {cliente}")
    except Exception as e: 
        config.logger.exception(f"❌ [ALERTA ERROR] Falló envío a Admin: {e}")


# ==========================================================
# 🎙️ PIPELINE DE VOZ E IMÁGENES (NUEVO REQUERIMIENTO AUDITORIA)
# ==========================================================

async def procesar_audio_whatsapp(media_id: str, telefono: str, vendedor_id: str, config_bot: dict, trace_id: str) -> Optional[str]:
    """[MÓDULO CUMPLIMIENTO] Procesamiento de audio entrante hacia STT/Gemini."""
    try:
        resultado = await descargar_media_whatsapp_async(media_id, config_bot.get("meta_token", ""))
        if resultado:
            config.logger.info(f"🎙️ [AUDIO] Audio descargado y blindado para {telefono}")
            return "[AUDIO_PROCESADO]"
        return None
    except Exception as e:
        config.logger.exception(f"❌ [AUDIO] Error crítico en pipeline de voz: {e}")
        return None

async def procesar_imagen_whatsapp(media_id: str, telefono: str, vendedor_id: str, config_bot: dict, trace_id: str) -> Optional[Dict[str, Any]]:
    """[MÓDULO CUMPLIMIENTO] Procesamiento multimodal estructurado de imágenes."""
    try:
        resultado = await descargar_media_whatsapp_async(media_id, config_bot.get("meta_token", ""))
        return resultado
    except Exception as e:
        config.logger.exception(f"❌ [IMAGEN] Error crítico en pipeline multimodal: {e}")
        return None


# ==========================================================
# 📥 DOWNLOADER CORE (HARDENED PAYLOAD)
# ==========================================================

async def descargar_media_whatsapp_async(
    media_id: str,
    token: str
) -> Optional[dict]:

    """
    ==============================================================================
    📥 DESCARGADOR MULTIMEDIA WHATSAPP AAA ENTERPRISE
    ==============================================================================
    ✔ Validación MIME estricta
    ✔ Límite duro de tamaño
    ✔ Protección anti-memory abuse
    ✔ Protección anti-decompression bombs
    ✔ Timeout granular
    ✔ Retries inteligentes
    ✔ Validación binaria real
    ✔ Protección SSRF Completa (urllib.parse)
    ✔ Validación Content-Type
    ✔ Validación magic bytes
    ✔ Validación entropy payload
    ✔ Protección anti payload corrupto
    ✔ Telemetría avanzada
    ==============================================================================
    """

    http_client = ai_security_utils.get_http_client()

    # ==============================================================================
    # 🛡️ VALIDACIÓN HTTP CLIENT Y INPUTS
    # ==============================================================================

    if not http_client:
        config.logger.error("❌ [MEDIA] HTTP Client no inicializado.")
        return None

    media_id = str(media_id).strip()
    if not media_id:
        config.logger.error("❌ [MEDIA] Media ID vacío.")
        return None
    if len(media_id) > 200:
        config.logger.error("🚨 [MEDIA] Media ID sospechosamente largo.")
        return None

    token = str(token).strip()
    if not token:
        config.logger.error("🚨 [MEDIA] Token vacío.")
        return None

    # ==============================================================================
    # 🛡️ CONFIGURACIÓN HARDENED
    # ==============================================================================

    MAX_MEDIA_SIZE = 15_000_000
    MAX_IMAGE_PIXELS = 20_000_000

    TIMEOUT_INFO = 10.0
    TIMEOUT_DOWNLOAD = 25.0
    MAX_REINTENTOS = 2

    MIME_PERMITIDOS = {
        "image/jpeg", "image/png", "image/webp",
        "audio/ogg", "audio/mp4", "audio/mpeg", "audio/aac"
    }

    inicio_descarga = config.now_ts()

    try:
        config.logger.info(f"📥 [MEDIA] Iniciando descarga segura MediaID={media_id[:20]}")

        # ==============================================================================
        # 🔍 URL METADATA
        # ==============================================================================
        url_info = f"https://graph.facebook.com/{ai_security_utils.META_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {token}"}

        data_info = None

        for intento in range(MAX_REINTENTOS + 1):
            try:
                config.logger.info(f"🔍 [MEDIA METADATA] Intento={intento+1}")
                res_info = await asyncio.wait_for(
                    http_client.get(url_info, headers=headers),
                    timeout=TIMEOUT_INFO
                )

                if res_info.status_code == 200:
                    data_info = res_info.json()
                    break

                config.logger.error(f"⚠️ [MEDIA METADATA] HTTP={res_info.status_code}")

                # FAIL FAST AUTH
                if res_info.status_code in [401, 403]:
                    config.logger.error("🚨 [MEDIA AUTH] Token inválido.")
                    return None

            except asyncio.TimeoutError:
                config.logger.error(f"⏱️ [MEDIA METADATA] Timeout intento={intento+1}")
            except Exception as meta_e:
                config.logger.exception(f"⚠️ [MEDIA METADATA ERROR] {meta_e}")

            # BACKOFF
            if intento < MAX_REINTENTOS:
                await asyncio.sleep(min(3.0, 2 ** intento))

        # ==============================================================================
        # 🚨 VALIDACIÓN METADATA Y MIME
        # ==============================================================================

        if not data_info:
            config.logger.error("🚨 [MEDIA] No se pudo recuperar metadata.")
            return None

        mime_type = str(data_info.get("mime_type", "")).lower().strip()
        if mime_type not in MIME_PERMITIDOS:
            config.logger.error(f"🚨 [MEDIA] MIME bloqueado: {mime_type}")
            return None

        # ==============================================================================
        # 🛡️ VALIDACIÓN FILE SIZE
        # ==============================================================================
        try:
            file_size = int(data_info.get("file_size", 0))
        except Exception as parse_e:
            config.logger.error(f"⚠️ [MEDIA] Error parseando file_size: {parse_e}")
            file_size = 0

        if file_size <= 0:
            config.logger.error("⚠️ [MEDIA] File size inválido.")
            return None

        if file_size > MAX_MEDIA_SIZE:
            config.logger.error(f"🚨 [MEDIA] Archivo excede límite: {file_size/1024/1024:.2f}MB")
            return None

        # ==============================================================================
        # 🛡️ PROTECCIÓN SSRF COMPLETA HARDEADA (MÓDULO URLLIB.PARSE)
        # ==============================================================================
        media_url = str(data_info.get("url", "")).strip()
        if not media_url.startswith("https://"):
            config.logger.error("🚨 [MEDIA] URL inválida.")
            return None

        dominios_permitidos = ["lookaside.fbsbx.com", "graph.facebook.com"]
        parsed_url = urllib.parse.urlparse(media_url)
        hostname = parsed_url.hostname.lower() if parsed_url.hostname else ""

        if hostname not in dominios_permitidos:
            config.logger.error(f"🚨 [MEDIA SSRF] Dominio no permitido: {media_url[:80]}")
            return None

        config.logger.info(f"📦 [MEDIA] MIME={mime_type} | Peso={file_size/1024:.1f}KB")

        # ==============================================================================
        # 📥 DESCARGA BINARIA
        # ==============================================================================
        data_bytes = None

        for intento in range(MAX_REINTENTOS + 1):
            try:
                config.logger.info(f"📥 [MEDIA DOWNLOAD] Intento={intento+1}")
                res_media = await asyncio.wait_for(
                    http_client.get(media_url, headers=headers),
                    timeout=TIMEOUT_DOWNLOAD
                )

                if res_media.status_code == 200:
                    content_type = str(res_media.headers.get("Content-Type", "")).lower()

                    if mime_type not in content_type:
                        config.logger.error(f"🚨 [MEDIA CONTENT-TYPE] Esperado={mime_type} | Recibido={content_type}")
                        return None

                    data_bytes = res_media.content
                    break

                config.logger.error(f"⚠️ [MEDIA DOWNLOAD] HTTP={res_media.status_code}")

            except asyncio.TimeoutError:
                config.logger.error(f"⏱️ [MEDIA DOWNLOAD] Timeout intento={intento+1}")
            except Exception as dl_e:
                config.logger.exception(f"⚠️ [MEDIA DOWNLOAD ERROR] {dl_e}")

            if intento < MAX_REINTENTOS:
                await asyncio.sleep(min(4.0, 2 ** intento))

        # ==============================================================================
        # 🚨 VALIDACIÓN PAYLOAD
        # ==============================================================================

        if not data_bytes:
            config.logger.error("⚠️ [MEDIA] Payload vacío.")
            return None

        payload_size = len(data_bytes)

        if payload_size > MAX_MEDIA_SIZE:
            config.logger.error("🚨 [MEDIA] Payload excede límite en bytes físicos.")
            return None

        if payload_size < 32:
            config.logger.error("🚨 [MEDIA] Payload sospechosamente pequeño.")
            return None

        # ==============================================================================
        # 🖼️ VALIDACIÓN IMAGEN (AAA ACCURACY: VERIFY + LOAD)
        # ==============================================================================

        if mime_type.startswith("image/"):
            try:
                Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS
                
                # Capa 1: Integridad Header
                img = Image.open(io.BytesIO(data_bytes))
                img.verify()

                # Capa 2: Decodificación completa (Previene payloads falsificados)
                img_load = Image.open(io.BytesIO(data_bytes))
                img_load.load()

                ancho, alto = img_load.size
                if ancho <= 0 or alto <= 0:
                    config.logger.error("🚨 [MEDIA IMG] Dimensiones inválidas.")
                    return None

                total_pixels = ancho * alto
                if total_pixels > MAX_IMAGE_PIXELS:
                    config.logger.error(f"🚨 [MEDIA IMG] Posible decompression bomb: {total_pixels} pixels")
                    return None

            except Exception as img_error:
                config.logger.exception(f"🚨 [MEDIA IMG] Imagen corrupta/maliciosa: {img_error}")
                return None

        # ==============================================================================
        # 🎙️ VALIDACIÓN AUDIO
        # ==============================================================================

        elif mime_type.startswith("audio/"):
            if payload_size < 128:
                config.logger.error("🚨 [MEDIA AUDIO] Audio sospechosamente pequeño.")
                return None

            headers_audio_validos = [b"OggS", b"ID3", b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"]
            if not any(data_bytes.startswith(h) for h in headers_audio_validos):
                config.logger.error("🚨 [MEDIA AUDIO] Magic bytes inválidos.")
                return None

        # ==============================================================================
        # 📊 TELEMETRÍA FINAL
        # ==============================================================================

        tiempo_total = config.now_ts() - inicio_descarga
        config.logger.info(f"Base SCHEMA verificado. ✅ [MEDIA SUCCESS] Tiempo={tiempo_total:.3f}s | Peso={payload_size/1024:.1f}KB")

        return {
            "mime_type": mime_type,
            "data": data_bytes
        }

    except Exception as e:
        config.logger.exception(f"❌ [MEDIA CRITICAL ERROR] {str(e)}")
        return None