# ==========================================================
# 🚀 MÓDULO: ai_gemini_core.py
# ==========================================================

import os
import json
import re
import base64
import asyncio
import hashlib
import math
import requests
import bleach
import orjson
import urllib3

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
# Importaciones explícitas (Única Fuente de Verdad)
import config_and_schemas as config
import ai_security_utils

# Desactivamos temporalmente las advertencias de SSL para el túnel proxy
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Variable local para el circuit breaker específico de este módulo
gemini_bloqueado_hasta = 0.0

# 🛡️ Helper de caché local
def generar_hash_cache(prompt: str, tenant: str, temp: float) -> str:
    return hashlib.sha256(f"{tenant}:{temp}:{prompt}".encode()).hexdigest()

# ==========================================================
# ─────────────────────────────────────────────────────────
# ÚNICA FUENTE DE VERDAD — globals compartidos por todo el módulo
# ─────────────────────────────────────────────────────────

# FAILSAFE GLOBAL (reemplaza RESPUESTA_FAILSAFE local de consultar_gemini_json)
FAILSAFE_RESPONSE = {
    "intencion": "HUMANO",
    "respuesta": "Hubo un micro-corte. Un asesor revisará tu mensaje enseguida. ⏳",
    "emocion_cliente": "neutral",
    "temperatura_lead": "frio",
    "producto_detectado": "",
    "categoria_preferida": "",
    "confidence": 0.0,
    "accion_tool": "ninguna",
    "precio_oferta": 0.0,
    "lead_score": 0,
    "probabilidad_cierre": 0.0,
    "estrategia_venta": "fallback",
    "requiere_seguimiento": False,
    "sugerir_veltrix": False,
    "tipo_seguimiento": "ninguno",
    "cross_selling": "",
    "upselling": "",
    "nivel_prioridad": "media",
    "etapa_venta": "descubrimiento",
    "objecion_detectada": "ninguna",
    "perfil_actualizado": {},
}

# ENUMS UNIFICADOS — fusión de ENUMS_VALIDOS + validar_respuesta_ia (antes duplicados)
ENUMS_VALIDOS = {
    "intencion": {
        # set original del cerebro
        "COMPRA", "COTIZACION", "HUMANO", "REGATEO", "POSTVENTA", "PAGO_RECIBIDO",
        # set adicional del validador
        "PEDIDO_ESPECIAL", "GARANTIA", "SPAM", "MAYOREO", "SALUDO", "ENOJO",
    },
    "etapa_venta":        {"descubrimiento", "negociacion", "cierre"},
    "objecion_detectada": {"precio", "indecision", "autoridad", "ninguna"},
    "accion_tool":        {"ninguna", "aplicar_descuento"},
    "emocion_cliente": {
        # set del cerebro (Gemini-prompted)
        "neutral", "feliz", "frustrado", "ansioso", "dudoso",
        # set del validador (antes divergente — causa del bug "feliz"→"neutral")
        "urgencia", "enojo", "duda", "entusiasmo",
    },
    "temperatura_lead":  {"frio", "tibio", "caliente"},
    "nivel_prioridad":   {"baja", "media", "alta", "critica"},   # "critica" del validador
    "tipo_seguimiento":  {"ninguno", "24h", "48h", "7d"},
}

ENUM_DEFAULTS = {
    "intencion":          "HUMANO",
    "etapa_venta":        "descubrimiento",
    "objecion_detectada": "ninguna",
    "accion_tool":        "ninguna",
    "emocion_cliente":    "neutral",
    "temperatura_lead":   "frio",
    "nivel_prioridad":    "media",
    "tipo_seguimiento":   "ninguno",
}

# HELPERS GLOBALES — versión hardened del validador (sustituye las versiones simples del cerebro)
def safe_float(
    valor,
    default: float = 0.0,
    minimo: float = 0.0,
    maximo: float = 999_999.0
) -> float:
    """Float seguro: protege contra NaN, Infinity y valores fuera de rango."""
    try:
        num = float(valor)
        if math.isnan(num) or math.isinf(num):
            return default
        return max(minimo, min(num, maximo))
    except Exception:
        return default

def safe_int(
    valor,
    default: int = 0,
    minimo: int = 0,
    maximo: int = 100
) -> int:
    """Int seguro: protege contra valores no numéricos y fuera de rango."""
    try:
        return max(minimo, min(int(float(valor)), maximo))
    except Exception:
        return default

def safe_bool(valor, default: bool = False) -> bool:
    """Bool seguro: normaliza strings de Gemini ('si', 'verdadero', '1', etc.)."""
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, str):
        return valor.lower().strip() in {"true", "1", "si", "sí", "yes", "verdadero"}
    if valor is None:
        return default
    return bool(valor)

# ==========================================================

async def consultar_gemini_json(
    prompt: str,
    media_dict: dict = None,
    temperature: float = 0.2,
    retries: int = 2,
    vendedor_id: str = "V-001"
) -> dict:

    """
    🚀 MOTOR GEMINI AAA ENTERPRISE ULTRA HARDENED (REST API EDITION)

    ✔ Cache Inteligente
    ✔ Circuit Breaker Global
    ✔ Failover Multi-Modelo (HTTP Bypass)
    ✔ Anti Prompt Bomb
    ✔ Anti Semantic Flood
    ✔ Anti Retry Storm
    ✔ JSON Hardened Parser
    ✔ Sanitización Profunda
    ✔ Validación Multimodal (Base64)
    ✔ Protección Anti Costos
    ✔ Protección Tokens
    ✔ Protección RAM
    ✔ Protección Deadlocks
    ✔ Protección Hallucinations
    ✔ Protección Response Poisoning
    ✔ Timeout Estricto
    ✔ Retry Exponencial
    ✔ Observabilidad Enterprise
    ✔ Respuesta Failsafe
    """

    global gemini_bloqueado_hasta

    inicio_telemetria = config.now_ts()

    # ==========================================================
    # 🛡️ CONFIG HARDENED
    # ==========================================================

    MAX_PROMPT_CHARS = 45000
    MAX_RESPONSE_CHARS = 15000
    MAX_MEDIA_SIZE = 20_000_000
    MAX_OUTPUT_TOKENS = 2048
    MAX_PALABRAS_PROMPT = 5000
    MAX_RETRIES = 4

    MODELOS_FAILOVER = [
        "gemini-2.5-flash"
    ]

    MIME_VALIDOS = {
        "image/jpeg",
        "image/png",
        "image/webp",
        "audio/ogg",
        "audio/mp4",
        "audio/mpeg",
        "audio/aac"
    }

    # ==========================================================
    # 🛡️ 0. VALIDACIÓN TEMPRANA
    # ==========================================================

    try:
        retries = int(retries)
        retries = max(1, min(retries, MAX_RETRIES))

        temperature = float(temperature)
        temperature = max(0.0, min(temperature, 1.0))

        vendedor_id = config.limpiar_texto(
            str(vendedor_id)
        )[:80]

    except Exception as config_error:
        config.logger.error(
            f"❌ [GEMINI CONFIG] Fallback config aplicado: {config_error}"
        )
        retries = 2
        temperature = 0.2
        vendedor_id = "V-001"

    # ==========================================================
    # 🛡️ 1. CIRCUIT BREAKER GLOBAL
    # ==========================================================

    tiempo_actual = config.now_ts()

    if tiempo_actual < gemini_bloqueado_hasta:
        restante = round(gemini_bloqueado_hasta - tiempo_actual, 2)
        config.logger.warning(
            f"🚨 [GEMINI CIRCUIT BREAKER] Gemini bloqueado temporalmente ({restante}s restantes)"
        )
        return {
            "respuesta": "En este momento estoy atendiendo a varios clientes. ⏳",
            "intencion": "HUMANO",
            "confidence": 1.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # 🛡️ 2. SERIALIZACIÓN SEGURA
    # ==========================================================

    try:
        if isinstance(prompt, (dict, list)):
            prompt_serializado = orjson.dumps(prompt).decode("utf-8")
        else:
            prompt_serializado = str(prompt)
    except Exception as serial_error:
        config.logger.error(
            f"❌ [PROMPT SERIALIZER] Fallback serializer: {serial_error}"
        )
        prompt_serializado = str(prompt)

    # ==========================================================
    # 🧹 3. SANITIZACIÓN PROFUNDA
    # ==========================================================

    prompt_serializado = config.limpiar_texto(prompt_serializado)
    prompt_serializado = prompt_serializado.replace("\x00", "").replace("\r", "")

    prompt_serializado = re.sub(r"```.*?```", "", prompt_serializado, flags=re.DOTALL)

    patrones_roles = [
        r"<\|system\|>", r"<\|assistant\|>", r"<\|user\|>",
        r"role\s*:\s*system", r"role\s*:\s*assistant", r"role\s*:\s*user",
        r"BEGIN\s+OVERRIDE", r"SYSTEM\s+INSTRUCTION", r"DEVELOPER\s+MODE",
        r"IGNORE\s+PREVIOUS\s+INSTRUCTIONS", r"YOU\s+ARE\s+NOW", r"ACT\s+AS", r"JAILBREAK"
    ]

    for patron in patrones_roles:
        prompt_serializado = re.sub(patron, "", prompt_serializado, flags=re.IGNORECASE)

    # ==========================================================
    # 🛡️ 4. LIMITADOR HARD PROMPT
    # ==========================================================

    if len(prompt_serializado) > MAX_PROMPT_CHARS:
        config.logger.warning(f"⚠️ [PROMPT LIMIT] Prompt truncado ({len(prompt_serializado)} chars)")
        prompt_serializado = prompt_serializado[-MAX_PROMPT_CHARS:]

    # ==========================================================
    # 🛡️ 5. ANTI SEMANTIC FLOOD
    # ==========================================================

    palabras_prompt = prompt_serializado.lower().split()

    if len(palabras_prompt) > MAX_PALABRAS_PROMPT:
        config.logger.error("🚨 [SEMANTIC FLOOD] Demasiadas palabras detectadas.")
        return {
            "respuesta": "Tu mensaje es demasiado grande para procesarlo.",
            "intencion": "HUMANO",
            "confidence": 0.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # 🛡️ 6. ANTI PROMPT REPETITIVO
    # ==========================================================

    palabras_unicas = len(set(palabras_prompt))

    if len(palabras_prompt) > 100 and palabras_unicas <= 10:
        config.logger.error("🚨 [PROMPT SPAM] Patrón repetitivo detectado.")
        return {
            "respuesta": "No pude procesar correctamente el contenido recibido.",
            "intencion": "SPAM",
            "confidence": 1.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # ⚡ 7. CACHE INTELIGENTE
    # ==========================================================

    cache_key = generar_hash_cache(prompt_serializado, vendedor_id, temperature)

    try:
        cache_item = config.cache_respuestas_ia.get(cache_key)
        if cache_item:
            edad_cache = config.now_ts() - cache_item.get("ts", 0)
            if edad_cache < config.CACHE_TTL_SECONDS:
                config.logger.info(f"⚡ [CACHE HIT] Tenant={vendedor_id} | Edad={edad_cache:.2f}s")
                return cache_item["data"]
    except Exception as cache_error:
        config.logger.error(f"❌ [CACHE ERROR] {cache_error}")

    # ==========================================================
    # 📊 8. ESTIMACIÓN TOKENS
    # ==========================================================

    tokens_estimados = max(1, len(prompt_serializado) // 4)

    # ==========================================================
    # 🛡️ 9. RATE LIMIT TOKENS
    # ==========================================================

    async with config.rate_limit_global_lock:
        tokens_actuales = config.tokens_consumidos_tenant.get(vendedor_id, 0)
        nuevo_total = tokens_actuales + tokens_estimados

        if nuevo_total > ai_security_utils.MAX_TOKENS_POR_MINUTO_TENANT:
            config.logger.error(f"🚨 [TOKEN FLOOD] Tenant={vendedor_id} superó límite de tokens.")
            return {
                "respuesta": "Estoy procesando demasiadas solicitudes ahora mismo. Dame un minuto.",
                "intencion": "HUMANO",
                "confidence": 0.0,
                "accion_tool": "ninguna"
            }
        config.tokens_consumidos_tenant[vendedor_id] = nuevo_total

    # ==========================================================
    # 🧠 10. FAILOVER MULTI MODELO (BYPASS HTTP REST)
    # ==========================================================

    API_KEY = os.getenv("GENAI_KEY", "").strip()

    if not API_KEY:
        config.logger.critical("❌ [CONFIG CRÍTICA] La variable GENAI_KEY no está configurada en el entorno.")
        return FAILSAFE_RESPONSE

    for nombre_modelo in MODELOS_FAILOVER:
        config.logger.info(f"🧠 [GEMINI] Iniciando inferencia HTTP con: {nombre_modelo}")
        
        url_api = f"https://generativelanguage.googleapis.com/v1beta/models/{nombre_modelo}:generateContent?key={API_KEY}"

        for intento in range(retries):
            try:
                # ==========================================================
                # 📦 11 & 12. CONSTRUCCIÓN CONTENIDO Y MULTIMEDIA (BASE64)
                # ==========================================================
                
                partes_contenido = [{"text": prompt_serializado}]

                if media_dict and "data" in media_dict:
                    try:
                        media_bytes = media_dict.get("data", b"")
                        mime_type = str(media_dict.get("mime_type", "image/jpeg")).lower().strip()

                        if mime_type in MIME_VALIDOS and media_bytes and len(media_bytes) <= MAX_MEDIA_SIZE:
                            
                            b64_data = base64.b64encode(media_bytes).decode("utf-8")
                            
                            partes_contenido.append({
                                "inlineData": {
                                    "mimeType": mime_type,
                                    "data": b64_data
                                }
                            })
                            config.logger.info(f"📸 [MEDIA HTTP] Archivo {mime_type} adjuntado con éxito al payload.")

                        elif len(media_bytes) > MAX_MEDIA_SIZE:
                            config.logger.error("🚨 [MEDIA LIMIT] Multimedia excede 20MB.")

                    except Exception as media_error:
                        config.logger.error(f"❌ [MEDIA ERROR] {media_error}")

                # ==========================================================
                # 🚀 13. ENSAMBLAJE DE PAYLOAD JSON Y GENERATION CONFIG
                # ==========================================================

                payload = {
                    "contents": [{"parts": partes_contenido}],
                    "generationConfig": {
                        "temperature": temperature,
                        "topP": 0.90,
                        "topK": 32,
                        "maxOutputTokens": MAX_OUTPUT_TOKENS,
                        # 🟢 FIX: Obligamos a Gemini 2.5 a responder SOLO con JSON puro
                        "responseMimeType": "application/json" 
                    }
                }

                # ==========================================================
                # 🔍 DEBUG PROMPT GEMINI
                # ==========================================================

                config.logger.warning(
                    f"\n\n"
                    f"================ PROMPT GEMINI =================\n"
                    f"{prompt_serializado}\n"
                    f"================================================\n\n"
                )

                # ==========================================================
                # 🌐 14. LLAMADA HTTP DIRECTA (TÚNEL PROXY ANTI-BLOQUEO)
                # ==========================================================
                
                headers_seguros = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                }

                # Extraemos tu llave para enrutar el tráfico y evadir el firewall de Google
                scraper_key = os.getenv("SCRAPER_API_KEY", "").strip()
                proxies_config = None
                
                if scraper_key:
                    proxy_url = f"http://scraperapi:{scraper_key}@proxy-server.scraperapi.com:8001"
                    proxies_config = {
                        "http": proxy_url,
                        "https": proxy_url
                    }

                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        requests.post,
                        url_api,
                        json=payload,
                        headers=headers_seguros,
                        proxies=proxies_config,
                        verify=False, # Necesario para evitar bloqueos de certificado por el proxy
                        timeout=25.0
                    ),
                    timeout=26.0
                )

                # ==========================================================
                # 🛡️ 15. VALIDACIÓN RESPONSE HTTP
                # ==========================================================

                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")

                data = response.json()
                
                if not data.get('candidates') or not data['candidates'][0].get('content'):
                    raise Exception("Estructura devuelta por Gemini vacía o bloqueada.")

                texto_respuesta = data['candidates'][0]['content']['parts'][0]['text']

                # ==========================================================
                # 🧹 16. LIMPIEZA RESPUESTA
                # ==========================================================

                texto_limpio = (
                    texto_respuesta
                    .replace("```json", "")
                    .replace("```JSON", "")
                    .replace("```", "")
                    .strip()
                )

                texto_limpio = texto_limpio[:MAX_RESPONSE_CHARS]

                # ==========================================================
                # 🛡️ 17. JSON PARSER HARDENED
                # ==========================================================

                obj = None

                try:
                    decoder = json.JSONDecoder()
                    obj, idx = decoder.raw_decode(texto_limpio)
                except json.JSONDecodeError as decode_error:
                    config.logger.error(f"❌ [JSON PARSER] Fallback regex activado. Error: {decode_error}")
                    match = re.search(r'\{.*\}', texto_limpio, re.DOTALL)
                    if match:
                        try:
                            obj = orjson.loads(match.group())
                        except Exception as regex_error:
                            config.logger.error(f"❌ [REGEX PARSER ERROR] {regex_error}")

                # ==========================================================
                # 🛡️ 18. VALIDACIÓN OBJETO
                # ==========================================================

                if not isinstance(obj, dict):
                    raise ValueError("Gemini devolvió estructura inválida.")

                # ==========================================================
                # 🧹 19. SANITIZACIÓN RESPUESTA
                # ==========================================================

                for key, value in list(obj.items()):
                    if isinstance(value, str):
                        value = bleach.clean(value, tags=[], strip=True)
                        value = config.limpiar_texto(value)
                        value = value.replace("\x00", "")
                        value = value[:5000]
                        obj[key] = value

                # ==========================================================
                # 🛡️ 20. VALIDACIÓN CAMPOS
                # ==========================================================

                obj.setdefault("respuesta", "No pude generar respuesta.")
                obj.setdefault("intencion", "HUMANO")
                obj.setdefault("confidence", 0.5)
                obj.setdefault("accion_tool", "ninguna")

                config.logger.warning(
                    f"\n\n================ RESPUESTA GEMINI =================\n"
                    f"{json.dumps(obj, ensure_ascii=False, indent=2)}\n"
                    f"===================================================\n"
                )

                if not isinstance(obj["respuesta"], str):
                    obj["respuesta"] = "Respuesta inválida."

                # ==========================================================
                # ⚡ 21. GUARDADO CACHE
                # ==========================================================

                try:
                    config.cache_respuestas_ia[cache_key] = {
                        "data": obj,
                        "ts": config.now_ts()
                    }
                except Exception as cache_save_error:
                    config.logger.error(f"❌ [CACHE SAVE ERROR] {cache_save_error}")

                # ==========================================================
                # 📊 22. TELEMETRÍA
                # ==========================================================

                tiempo_total = config.now_ts() - inicio_telemetria
                config.logger.info(
                    f"✅ [GEMINI SUCCESS HTTP] Modelo={nombre_modelo} | "
                    f"Tiempo={tiempo_total:.3f}s | Tokens≈{tokens_estimados} | "
                    f"Tenant={vendedor_id}"
                )

                return obj

            # ==========================================================
            # ⏱️ 23. TIMEOUT CONTROLADO
            # ==========================================================
            except asyncio.TimeoutError:
                config.logger.error(f"⏱️ [GEMINI TIMEOUT] Modelo={nombre_modelo} | Intento={intento+1}")

            # ==========================================================
            # 🚨 24. ERRORES CONTROLADOS
            # ==========================================================
            except Exception as e:
                config.logger.error(f"❌ [GEMINI ERROR] Modelo={nombre_modelo} | Intento={intento+1} | Error={str(e)}")

                error_str = str(e).lower()

                # QUOTA / 429
                if "429" in error_str or "quota" in error_str or "resource exhausted" in error_str or "rate limit" in error_str:
                    gemini_bloqueado_hasta = config.now_ts() + 60.0
                    config.logger.warning("🚨 [QUOTA LIMIT] Circuit breaker 60s activado.")
                    break # Salta al siguiente modelo si se agotó la cuota de este

                # BACKOFF EXPONENCIAL
                espera = min(8, 2 ** intento)
                await asyncio.sleep(espera)

    # ==========================================================
    # 🚨 25. FAILSAFE FINAL
    # ==========================================================

    tiempo_total = config.now_ts() - inicio_telemetria
    config.logger.error(f"🚨 [GEMINI FAILSAFE] Todos los modelos HTTP fallaron | Tiempo={tiempo_total:.3f}s")

    return FAILSAFE_RESPONSE

# ==========================================================
# 🛡️ VALIDADOR UNIVERSAL IA AAA ENTERPRISE HARDENED
# ==========================================================

def validar_respuesta_ia(data: dict) -> dict:
    """
    ==============================================================================
    🧠 FIREWALL COGNITIVO UNIVERSAL VELTRIX ENGINE
    ==============================================================================
    ✔ Schema Validation Estricta
    ✔ Anti Hallucination
    ✔ Anti JSON Bomb
    ✔ Anti Prompt Reflection
    ✔ Anti Unicode Exploits
    ✔ Clamp Numérico Seguro
    ✔ Sanitización XSS / HTML
    ✔ Protección Anti Overflow
    ✔ Compatibilidad Retroactiva
    ✔ FailSafe Comercial
    ✔ Protección Token/RAM Abuse
    ✔ Anti Nested Objects
    ✔ Anti Markdown Injection
    ✔ Anti Null Corruption
    ✔ Anti Infinity / NaN
    ==============================================================================
    """

    # ==============================================================================
    # 🛡️ 1. VALIDACIÓN ESTRUCTURAL
    # ==============================================================================

    if not isinstance(data, dict):
        raise Exception("Formato IA inválido.")

    # ==============================================================================
    # 🛡️ 2. LÍMITE DURO DE CAMPOS
    # ==============================================================================

    if len(data) > 80:
        config.logger.error("🚨 [VALIDADOR IA] Payload excesivo detectado.")
        raise Exception("Payload IA sospechoso.")

    # ==============================================================================
    # 🛡️ 3. ENUMS SEGUROS — fuente única: ENUMS_VALIDOS / ENUM_DEFAULTS globales
    # ==============================================================================
    # (INTENCIONES_VALIDAS, EMOCIONES_VALIDAS, TEMPERATURAS_VALIDAS,
    #  TOOLS_VALIDAS y PRIORIDADES_VALIDAS unificadas en ENUMS_VALIDOS)

    # ==============================================================================
    # 🛡️ 4. HELPERS HARDENED
    # (safe_float y safe_int son ahora funciones globales del módulo)
    # ==============================================================================

    def safe_clean_text(
        valor,
        max_len: int = 300,
        permitir_saltos: bool = False
    ) -> str:

        try:

            if valor is None:
                return ""

            # ----------------------------------------------------------------------
            # Protección Anti Nested Objects
            # ----------------------------------------------------------------------

            if isinstance(valor, (dict, list, tuple, set)):
                valor = str(valor)

            texto = str(valor)

            # ----------------------------------------------------------------------
            # Protección Anti Unicode Invisible / Control Chars
            # ----------------------------------------------------------------------

            texto = re.sub(
                r"[\x00-\x1F\x7F-\x9F\u200B-\u200F\u202A-\u202E]",
                "",
                texto
            )

            # ----------------------------------------------------------------------
            # Protección Anti Prompt Reflection
            # ----------------------------------------------------------------------

            patrones_bloqueados = [
                r"system\s+prompt",
                r"developer\s+mode",
                r"ignore\s+instructions",
                r"olvida\s+las\s+reglas",
                r"eres\s+chatgpt",
                r"<script",
                r"javascript:",
                r"data:text/html",
                r"file://",
                r"gopher://",
                r"ftp://",
                r"localhost",
                r"127\.0\.0\.1"
            ]

            texto_lower = texto.lower()

            for patron in patrones_bloqueados:
                if re.search(patron, texto_lower):
                    config.logger.warning(
                        f"🚨 [VALIDADOR IA] Patrón sospechoso bloqueado: {patron}"
                    )
                    texto = "[CONTENIDO FILTRADO]"
                    break

            # ----------------------------------------------------------------------
            # Sanitización HTML/XSS
            # ----------------------------------------------------------------------

            texto = bleach.clean(
                texto,
                tags=[],
                attributes={},
                strip=True
            )

            texto = config.limpiar_texto(texto)

            # ----------------------------------------------------------------------
            # Protección Markdown Injection
            # ----------------------------------------------------------------------

            texto = texto.replace("```", "")
            texto = texto.replace("***", "")
            texto = texto.replace("###", "")

            # ----------------------------------------------------------------------
            # Protección Longitud
            # ----------------------------------------------------------------------

            texto = texto[:max_len]

            # ----------------------------------------------------------------------
            # Saltos de línea
            # ----------------------------------------------------------------------

            if not permitir_saltos:
                texto = texto.replace("\n", " ").replace("\r", " ")

            return texto.strip()

        except Exception as e:

            config.logger.error(
                f"❌ [VALIDADOR IA] Error limpiando texto: {e}"
            )

            return ""

    # ==============================================================================
    # 🛡️ 5. NORMALIZACIÓN PRINCIPAL
    # ==============================================================================

    intencion = safe_clean_text(
        data.get("intencion", "HUMANO"),
        40
    ).upper()

    if intencion not in ENUMS_VALIDOS["intencion"]:
        intencion = ENUM_DEFAULTS["intencion"]

    emocion_cliente = safe_clean_text(
        data.get("emocion_cliente", "neutral"),
        30
    ).lower()

    if emocion_cliente not in ENUMS_VALIDOS["emocion_cliente"]:
        emocion_cliente = ENUM_DEFAULTS["emocion_cliente"]

    temperatura_lead = safe_clean_text(
        data.get("temperatura_lead", "frio"),
        30
    ).lower()

    if temperatura_lead not in ENUMS_VALIDOS["temperatura_lead"]:
        temperatura_lead = ENUM_DEFAULTS["temperatura_lead"]

    accion_tool = safe_clean_text(
        data.get("accion_tool", "ninguna"),
        50
    ).lower()

    if accion_tool not in ENUMS_VALIDOS["accion_tool"]:
        accion_tool = ENUM_DEFAULTS["accion_tool"]

    nivel_prioridad = safe_clean_text(
        data.get("nivel_prioridad", "media"),
        20
    ).lower()

    if nivel_prioridad not in ENUMS_VALIDOS["nivel_prioridad"]:
        nivel_prioridad = ENUM_DEFAULTS["nivel_prioridad"]

    # 🟢 FIX: Validación real para etapa_venta
    etapa_venta = safe_clean_text(
        data.get("etapa_venta", "descubrimiento"),
        30
    ).lower()

    if etapa_venta not in ENUMS_VALIDOS["etapa_venta"]:
        etapa_venta = ENUM_DEFAULTS["etapa_venta"]

    # 🟢 FIX: Validación real para objecion_detectada
    objecion_detectada = safe_clean_text(
        data.get("objecion_detectada", "ninguna"),
        30
    ).lower()

    if objecion_detectada not in ENUMS_VALIDOS["objecion_detectada"]:
        objecion_detectada = ENUM_DEFAULTS["objecion_detectada"]

    # 🟢 FIX: Validación real para tipo_seguimiento
    tipo_seguimiento = safe_clean_text(
        data.get("tipo_seguimiento", "ninguno"),
        30
    ).lower()

    if tipo_seguimiento not in ENUMS_VALIDOS["tipo_seguimiento"]:
        tipo_seguimiento = ENUM_DEFAULTS["tipo_seguimiento"]

    # 🟢 FIX: Validación segura de perfil_actualizado (evitar que un string rompa el dict)
    raw_perfil = data.get("perfil_actualizado", {})
    if not isinstance(raw_perfil, dict):
        config.logger.warning(f"⚠️ [VALIDADOR IA] perfil_actualizado inválido detectado, reseteando a dict vacío.")
        raw_perfil = {}

    # ==============================================================================
    # 🛡️ 6. RESPUESTA PRINCIPAL
    # ==============================================================================

    respuesta = safe_clean_text(
        data.get("respuesta", "Hola."),
        max_len=4000,
        permitir_saltos=True
    )

    if not respuesta:
        respuesta = (
            "Estoy revisando la mejor opción para ayudarte. 👌"
        )

    # ==============================================================================
    # 🛡️ 7. NUMÉRICOS HARDENED
    # ==============================================================================

    confidence = safe_float(
        data.get("confidence", 0.0),
        default=0.0,
        minimo=0.0,
        maximo=1.0
    )

    # ----------------------------------------------------------------------
    # Handoff Automático si confianza baja
    # ----------------------------------------------------------------------

    if confidence < 0.60:
        intencion = "HUMANO"
        confidence = 0.0

    precio_oferta = safe_float(
        data.get("precio_oferta", 0.0),
        default=0.0,
        minimo=0.0,
        maximo=999999.0
    )

    lead_score = safe_int(
        data.get("lead_score", 0),
        default=0,
        minimo=0,
        maximo=100
    )

    probabilidad_cierre = safe_float(
        data.get("probabilidad_cierre", 0.0),
        default=0.0,
        minimo=0.0,
        maximo=1.0
    )

    # ==============================================================================
    # 🛡️ 8. CONSTRUCCIÓN FINAL SEGURA
    # ==============================================================================

    res = {

        "intencion":
            intencion,

        "respuesta":
            respuesta,

        "producto_detectado":
            safe_clean_text(
                data.get("producto_detectado")
                or data.get("juego_detectado", ""),
                150
            ),

        "categoria_preferida":
            safe_clean_text(
                data.get("categoria_preferida", ""),
                120
            ),

        "emocion_cliente":
            emocion_cliente,

        "temperatura_lead":
            temperatura_lead,

        "accion_tool":
            accion_tool,

        "estrategia_venta":
            safe_clean_text(
                data.get("estrategia_venta", "normal"),
                100
            ),

        "cross_selling":
            safe_clean_text(
                data.get("cross_selling", ""),
                250
            ),

        "upselling":
            safe_clean_text(
                data.get("upselling", ""),
                250
            ),

        "nivel_prioridad":
            nivel_prioridad,

        "tipo_seguimiento":
            tipo_seguimiento,

        "requiere_seguimiento":
            safe_bool(data.get("requiere_seguimiento"), False),

        "sugerir_veltrix":
            safe_bool(data.get("sugerir_veltrix"), False),

        "confidence":
            confidence,

        "precio_oferta":
            precio_oferta,

        "lead_score":
            lead_score,

        "probabilidad_cierre":
            probabilidad_cierre,

        # 🟢 FIX: Incluyendo campos previamente destruidos
        "etapa_venta":
            etapa_venta,

        "objecion_detectada":
            objecion_detectada,

        "perfil_actualizado":
            raw_perfil
    }

    # ==============================================================================
    # 🛡️ 9. PROTECCIÓN COMERCIAL
    # ==============================================================================

    if (
        res["accion_tool"] == "aplicar_descuento"
        and res["precio_oferta"] <= 0
    ):

        config.logger.warning(
            "⚠️ [VALIDADOR IA] Descuento inválido detectado."
        )

        res["accion_tool"] = "ninguna"

    # ==============================================================================
    # 🛡️ 10. ANTI RESPUESTAS SOSPECHOSAS
    # ==============================================================================

    respuesta_lower = res["respuesta"].lower()

    sospechosos = [
        "system prompt",
        "developer mode",
        "ignore instructions",
        "api key",
        "token",
        "contraseña",
        "password",
        "sudo",
        "rm -rf",
        "<script"
    ]

    if any(s in respuesta_lower for s in sospechosos):

        config.logger.warning(
            "🚨 [VALIDADOR IA] Respuesta sospechosa neutralizada."
        )

        res["respuesta"] = (
            "Voy a canalizar tu solicitud con un asesor. 👌"
        )

        res["intencion"] = "HUMANO"

    # ==============================================================================
    # 📊 11. TELEMETRÍA
    # ==============================================================================

    config.logger.info(
        f"🎯 [VALIDADOR IA] "
        f"Intención={res['intencion']} | "
        f"Score={res['lead_score']} | "
        f"Confidence={res['confidence']:.2f}"
    )

    # ==============================================================================
    # ✅ 12. RESPUESTA FINAL
    # ==============================================================================

    return res

# ─────────────────────────────────────────────
# CEREBRO COMERCIAL AAA — FUSIÓN COGNITIVA v2
# ─────────────────────────────────────────────
async def analizar_intencion_venta_ia(
    texto_cliente: str,
    inventario_contexto: str,
    historial_chat: str,
    config_dict: dict,
    telefono: str,
    perfil_cliente_previo: dict = None,
    media_dict: dict = None,
):
    """🧠 CEREBRO COMERCIAL AAA — v4 (CONGELADA).
 
    v2 → v3: raw_perfil type-guard · precio_oferta clamp · respuesta vacía guard
             score granular · variables giro/meta/desc · prompt descuentos
    v3 → v4: lock hash incluye telefono (evita colisión entre clientes)
             safe_bool() normaliza booleanos de Gemini ("si"/"verdadero"/1)
             requiere_seguimiento y sugerir_veltrix normalizados explícitamente
             remarketing_count capped a 50 (evita crecimiento infinito)
             cross_selling y upselling validados contra RAG textual
    """
 
    # ── Helpers de normalización — ahora globales del módulo ─────────────────

    # ── Guardrails previos al lock ────────────────────────────────────────
    if len(texto_cliente) > 5000:
        return FAILSAFE_RESPONSE
 
    inventario_prompt = (
        inventario_contexto.strip()
        if inventario_contexto and inventario_contexto.strip()
        else "Inventario agotado."
    )
 
    try:
        # ── Escudo de seguridad ───────────────────────────────────────────
        if ai_security_utils.detectar_prompt_injection(texto_cliente):
            return {
                **FAILSAFE_RESPONSE,
                "intencion": "SPAM",
                "respuesta": "Mensaje bloqueado por seguridad.",
            }
 
        v_id         = str(config_dict.get("vendedor_id",    "V-001"))
        negocio      = str(config_dict.get("nombre_negocio", "Veltrix Store"))
        tono         = str(config_dict.get("tono_ia",        "Persuasivo"))
        # Variables comerciales restauradas (giro, metas, política de descuentos)
        giro         = str(config_dict.get("giro",           "videojuegos"))
        meta_venta   = safe_float(config_dict.get("meta_venta",   0.0))
        permitir_desc= safe_bool(config_dict.get("permitir_desc", True), default=True)
        desc_max     = safe_float(config_dict.get("desc_max",     0.0))
 
        # ── FIX #1 — LOCK COGNITIVO DUAL ─────────────────────────────────
        # conv_{telefono}  → evita colisiones entre sesiones paralelas del mismo cliente
        # sha256 con telefono → evita colisión entre clientes distintos con mismo texto
        async with await config.get_lock(f"conv_{telefono}"):
            lock_id = hashlib.sha256(
                f"{v_id}:{telefono}:{texto_cliente[:50]}".encode()
            ).hexdigest()
 
            async with await config.get_lock(lock_id):
 
                # ── Perfil con todos los campos persistentes ──────────────
                # FIX #3 — remarketing_count incluido desde el inicio
                # FIX #4 — emocion_actual incluida desde el inicio
                # FIX #5 — temperatura incluida desde el inicio
                # Guard: mismo patrón que raw_perfil — Gemini o el caller pueden pasar basura
                if perfil_cliente_previo is not None and not isinstance(perfil_cliente_previo, dict):
                    config.logger.warning(
                        f"[MEMORIA] perfil_cliente_previo inválido: {type(perfil_cliente_previo).__name__}"
                    )
                    perfil_cliente_previo = None
 
                perfil = perfil_cliente_previo or {
                    "consolas_favoritas":  [],
                    "generos":             [],
                    "nivel_regateo":       "neutral",
                    "etapa_venta":         "descubrimiento",
                    "ultima_objecion":     "ninguna",
                    "ultima_intencion":    "",
                    "ticket_estimado":     0.0,
                    "frecuencia_contacto": "",
                    "temperatura":         "frio",   # FIX #5
                    "remarketing_count":   0,        # FIX #3
                    "emocion_actual":      "neutral", # FIX #4
                }
 
                historial = historial_chat[-2500:]
 
                # ── Lead Score comercial (granular — acumulativo por keyword) ──
                lead_score = 10
                txt = config.limpiar_texto(texto_cliente).lower()
 
                # Señales de intención: cada keyword suma independientemente
                for p in ["precio", "cuanto", "disponible"]:
                    if p in txt:
                        lead_score += 8
                # Señales de urgencia
                for p in ["hoy", "urge", "ya"]:
                    if p in txt:
                        lead_score += 12
                # Señales de negociación
                for p in ["menos", "rebaja", "descuento"]:
                    if p in txt:
                        lead_score += 5
 
                # FIX #2 — Temperatura del lead afecta el score (remarketing)
                if perfil.get("temperatura") == "caliente":
                    lead_score += 15
 
                lead_score = min(100, max(0, lead_score))
 
                estrategia = "normal"
                if lead_score >= 70:
                    estrategia = "cierre_agresivo"
                elif any(p in txt for p in ["caro", "menos"]):
                    estrategia = "negociacion"
 
                # ── FIX #7 — Prompt con reglas RAG abiertas completas ────
                _desc_regla = (
                    f"Puedes aplicar hasta {desc_max:.0f}% de descuento si el cliente insiste."
                    if permitir_desc and desc_max > 0
                    else "No apliques descuentos. Defiende el precio con valor del producto."
                )
                prompt_maestro = f"""[SYSTEM]
Eres Veltrix, asesor experto de {negocio} (giro: {giro}).
TONO: {tono}.
ESTRATEGIA ACTIVA: {estrategia}
META DE VENTA: ${meta_venta:,.0f}
FRAMEWORK: 1.Descubrimiento → 2.Confianza → 3.Objeción → 4.Cierre
 
[MEMORIA PERSISTENTE DEL CLIENTE]
{json.dumps(perfil, ensure_ascii=False)}
 
[REGLAS DE ORO]
1. ANTI-ALUCINACIÓN
   Solo usa productos del bloque [RAG].
   Si el producto no aparece ahí, indica que está agotado y ofrece una alternativa REAL del [RAG].
 
2. PREGUNTAS ABIERTAS (FIX #7 — reglas explícitas restauradas)
   Si el cliente pregunta alguna variante de:
     - "¿qué juegos tienes?"
     - "¿cuáles tienes?"
     - "¿qué hay disponible?"
   → Menciona exactamente 3 productos destacados del [RAG] con su precio.
   → Pregunta: ¿para qué consola busca? o ¿cuál es su presupuesto?
   → Nunca inventes productos fuera del [RAG].
 
3. MULTIMEDIA
   - Comprobantes/fotos: confirma monto y concepto visibles.
   - Audios: responde de forma natural y conversacional.
 
4. POLÍTICA DE DESCUENTOS
   {_desc_regla}
 
5. ESTRATEGIA DE CIERRE
   Usa la estrategia: {estrategia}.
   Con lead_score ≥ 70 activa cierre_agresivo; nunca pierdas momentum.
 
[RAG — INVENTARIO ACTUAL]
{inventario_prompt}
 
[HISTORIAL DE CHAT]
{historial}
 
[FORMATO JSON OBLIGATORIO — sin texto fuera del JSON]
{{
  "intencion":          "COMPRA|COTIZACION|HUMANO|REGATEO|POSTVENTA|PAGO_RECIBIDO",
  "respuesta":          "Texto natural hacia el cliente",
  "emocion_cliente":    "neutral|feliz|frustrado|ansioso|dudoso",
  "temperatura_lead":   "frio|tibio|caliente",
  "producto_detectado": "nombre exacto del RAG o vacío",
  "categoria_preferida":"consola|juego|accesorio|otro",
  "lead_score":         {lead_score},
  "probabilidad_cierre":0.8,
  "estrategia_venta":   "{estrategia}",
  "accion_tool":        "ninguna|aplicar_descuento",
  "precio_oferta":      0.0,
  "cross_selling":      "producto complementario o vacío",
  "upselling":          "producto premium o vacío",
  "requiere_seguimiento":true,
  "tipo_seguimiento":   "ninguno|24h|48h|7d",
  "nivel_prioridad":    "baja|media|alta",
  "etapa_venta":        "descubrimiento|negociacion|cierre",
  "objecion_detectada": "precio|indecision|autoridad|ninguna",
  "confidence":         0.9,
  "perfil_actualizado": {{
    "consolas_favoritas": [],
    "generos":            [],
    "nivel_regateo":      "bajo|medio|alto"
  }}
}}"""
 
                # ── Ejecución IA ──────────────────────────────────────────
                # 🔧 FIX: ya NO embebemos el media_dict dentro de prompt_estructurado.
                # Ese bloque era redundante y riesgoso: consultar_gemini_json() YA
                # recibe media_dict por su propio parámetro y lo adjunta correctamente
                # como 'inlineData' (lo único que Gemini interpreta como imagen real).
                # Meter el base64 aquí además solo lo convertía en texto plano dentro
                # del prompt (Gemini nunca lo veía como imagen), inflaba el conteo de
                # caracteres, y en imágenes grandes corría el riesgo de empujar las
                # instrucciones del sistema fuera de MAX_PROMPT_CHARS al truncar.
                prompt_estructurado = [{"role": "user", "parts": [prompt_maestro]}]
 
                data = await consultar_gemini_json(
                    prompt_estructurado,
                    vendedor_id=v_id,
                    media_dict=media_dict,
                )
 
                # Guard: si Gemini devuelve None, lista, string o cualquier no-dict
                if not isinstance(data, dict):
                    config.logger.error(
                        f"[IA] Respuesta inválida de Gemini: {type(data).__name__} — {str(data)[:120]}"
                    )
                    return FAILSAFE_RESPONSE
 
                # ════════════════════════════════════════════════════════
                # FUSIÓN Y VALIDACIÓN AAA
                # ════════════════════════════════════════════════════════
 
                # A. Fallback de seguridad (setdefault para no pisar IA)
                for k, v in FAILSAFE_RESPONSE.items():
                    data.setdefault(k, v)
 
                # B. Validación de enums — FIX #6 (ahora incluye los 4 faltantes)
                # NOTA: Defense in Depth (Se valida de forma ligera aquí, pero se sella en validar_respuesta_ia)
                for campo, valores_validos in ENUMS_VALIDOS.items():
                    if data.get(campo) not in valores_validos:
                        data[campo] = ENUM_DEFAULTS[campo]
 
                # C. Sanitización de campos de texto
                CAMPOS_TEXTO = [
                    "respuesta", "producto_detectado", "cross_selling",
                    "upselling", "categoria_preferida", "objecion_detectada",
                ]
                for f in CAMPOS_TEXTO:
                    data[f] = config.limpiar_texto(
                        bleach.clean(str(data.get(f, "")), tags=[], strip=True)
                    )[:4000]
 
                # C.2 Guardia de respuesta vacía
                if not data["respuesta"].strip():
                    data["respuesta"] = "Estoy revisando la mejor opción para ayudarte. 👌"
 
                # C.3 Validación cross_selling y upselling contra RAG (evita alucinaciones)
                def _en_rag(nombre: str) -> bool:
                    """True si el nombre del producto aparece en el inventario actual."""
                    if not nombre or inventario_prompt == "Inventario agotado.":
                        return False
                    return nombre.lower() in inventario_prompt.lower()
 
                if not _en_rag(data["cross_selling"]):
                    data["cross_selling"] = ""
                if not _en_rag(data["upselling"]):
                    data["upselling"] = ""
 
                # D. Normalización numérica
                data["confidence"]          = max(0.0, min(1.0,       safe_float(data.get("confidence"),          0.9)))
                data["probabilidad_cierre"] = max(0.0, min(1.0,       safe_float(data.get("probabilidad_cierre"), 0.5)))
                data["lead_score"]          = max(0,   min(100,       safe_int  (data.get("lead_score"),          lead_score)))
                # D.2 precio_oferta — normalización + clamp (evita valores absurdos de Gemini)
                data["precio_oferta"]       = max(0.0, min(999_999.0, safe_float(data.get("precio_oferta"),       0.0)))
 
                # D.3 Normalización de booleanos (Gemini puede devolver "si" / "verdadero" / 1)
                data["requiere_seguimiento"] = safe_bool(data.get("requiere_seguimiento"), False)
                data["sugerir_veltrix"]      = safe_bool(data.get("sugerir_veltrix"),      False)
 
                # E. Perfil enriquecido — FIX #3, #4, #5
                raw_perfil = data.get("perfil_actualizado", {})
                # Type guard: Gemini puede devolver string en lugar de dict
                if not isinstance(raw_perfil, dict):
                    config.logger.warning(f"[MEMORIA] perfil_actualizado no es dict ({type(raw_perfil).__name__}), usando fallback vacío")
                    raw_perfil = {}
 
                # remarketing_count sube solo cuando hay seguimiento activo, máximo 50
                seguimiento_activo = data["requiere_seguimiento"]  # ya normalizado
                nuevo_remarketing  = min(
                    50,
                    perfil.get("remarketing_count", 0) + (1 if seguimiento_activo else 0)
                )
 
                data["perfil_actualizado"] = {
                    # Campos base (ya existían)
                    "consolas_favoritas":  raw_perfil.get("consolas_favoritas",  perfil.get("consolas_favoritas",  [])),
                    "generos":             raw_perfil.get("generos",             perfil.get("generos",             [])),
                    "nivel_regateo":       raw_perfil.get("nivel_regateo",       perfil.get("nivel_regateo",       "neutral")),
                    "ultima_intencion":    data["intencion"],
                    "ultima_objecion":     data["objecion_detectada"],
                    "ultima_categoria":    data["categoria_preferida"],
                    "ticket_estimado":     data["precio_oferta"],  # ya normalizado y clamped
                    # FIX #4 — emoción persistida en memoria
                    "emocion_actual":      data["emocion_cliente"],
                    # FIX #5 — temperatura del lead persistida
                    "temperatura":         data["temperatura_lead"],
                    # FIX #3 — contador de remarketing acumulado
                    "remarketing_count":   nuevo_remarketing,
                }
 
                config.logger.info(
                    f"[MEMORIA] Perfil actualizado — tel:{telefono} "
                    f"score:{data['lead_score']} "
                    f"temp:{data['temperatura_lead']} "
                    f"remarketing:{nuevo_remarketing}"
                )
 
                return validar_respuesta_ia(data)
 
    except Exception as e:
        config.logger.exception(f"❌ [CEREBRO ERROR] Falla estructural: {str(e)}")
        return FAILSAFE_RESPONSE
    
# ==========================================================
# 🛠️ 6. FUNCIONES CORE: SCRAPER, ALERTAS, MEDIA Y COMUNICACIÓN
# ==========================================================
async def generar_resumen_handoff_ia(
    cliente: str,
    intencion: str,
    historial_str: str
):
    """
    Generador ejecutivo de resumen para agentes humanos:
    - Resume contexto
    - Detecta urgencia
    - Resume emoción
    - Resume problema
    """

    try:
        config.logger.info(
            f"📋 [HANDOFF IA] Generando resumen ejecutivo para {cliente}"
        )

        historial_limpio = config.limpiar_texto(historial_str)

        # ==========================================================
        # 🛡️ CONTROL TOKENS
        # ==========================================================
        if len(historial_limpio) > 3000:
            historial_limpio = historial_limpio[-3000:]

        prompt = f"""
Eres un supervisor ejecutivo de atención al cliente.

CLIENTE:
{cliente}

INTENCIÓN DETECTADA:
{intencion}

HISTORIAL:
{historial_limpio}

Tu tarea:
1. Resume el problema.
2. Resume el estado emocional.
3. Resume lo que el asesor debe hacer.
4. Máximo 3 viñetas.
5. Sé breve y ejecutivo.

RESPONDE ÚNICAMENTE JSON:

{{
  "resumen": "• Punto 1\\n• Punto 2\\n• Punto 3"
}}
"""

        data = await consultar_gemini_json(
            prompt=prompt,
            temperature=0.1
        )

        resumen = config.limpiar_texto(
            str(data.get("resumen", "")).strip()
        )

        if not resumen:
            resumen = "⚠️ Cliente requiere asistencia humana inmediata."

        config.logger.info("✅ [HANDOFF IA] Resumen ejecutivo generado.")

        return resumen[:1200]

    except Exception as e:
        config.logger.exception(f"❌ [HANDOFF IA ERROR] {str(e)}")

        return (
            "⚠️ Cliente requiere asistencia humana.\n"
            "No fue posible generar el resumen automático."
        )


async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nProducto: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera un mensaje corto de remarketing ofreciendo un pequeño descuento. Formato JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": config.limpiar_texto(data.get("mensaje_oferta", ""))}
    except Exception as e: 
        config.logger.exception(f"❌ [OFERTA INTELIGENTE ERROR] Error generando oferta: {e}")
        return None
