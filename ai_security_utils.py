# ==========================================================
# 🚀 MÓDULO: ai_security_utils.py (ENTERPRISE GOLD STANDARD - v2.8)
# ==========================================================
# Escudo de Seguridad, Rate Limiting y Variables Globales IA
# refactorizado ok
# ==========================================================

import os
import time
import asyncio
import logging
import hmac
import hashlib
import jwt
import httpx
import re
import uuid
import phonenumbers
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from collections import deque
from rapidfuzz import fuzz
from cachetools import TTLCache

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE (SSOT)
# ==========================================================
import config_and_schemas as config

logger = config.logger

# 📊 METRICAS DE OBSERVABILIDAD LOCALES AL MÓDULO
METRICAS_SEGURIDAD = {
    "injection_blocked": 0,
    "rate_limit_hits": 0,
    "background_tasks_rejected": 0,
    "jwt_failures": 0,
    "rate_limit_rejected": 0
}

# 🛑 CONFIGURACIÓN SEGURA Y ORÍGENES
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "").strip()
if not ALLOWED_ORIGINS_RAW or ALLOWED_ORIGINS_RAW == "*":
    raise RuntimeError("❌ FATAL: ALLOWED_ORIGINS inseguro o no configurado.")
ORIGENES_PERMITIDOS = [orig.strip() for orig in ALLOWED_ORIGINS_RAW.split(",")]

# Variables adicionales de configuración
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "5210000000000")
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "")
GENAI_KEY = os.getenv("GENAI_KEY", "").strip()

# Constantes del sistema
MAX_HISTORIAL = 8
MAX_CACHE_IA = 500 
GEMINI_TEMP = 0.2
MAX_COLA_GLOBAL = 100
MAX_BACKGROUND_TASKS_RAM = 500
MAX_REQUESTS_POR_MINUTO_TENANT = 100 
MAX_REQUESTS_POR_MINUTO_PHONE = 30   
# 🔧 FIX: 15,000 alcanzaba para apenas ~15-20 mensajes de UN cliente (y este
# contador es por TENANT completo, no por cliente individual) — varios
# clientes reales escribiéndole al mismo negocio a la vez lo agotarían rápido,
# justo la noche antes de un lanzamiento real con "mensajes ilimitados" como
# requisito explícito. Se sube a 100,000 para dar margen real de uso
# simultáneo, sin perder la función de frenar un abuso genuino (loops,
# ataques, etc). Ver también el fix de ventana real de 60s en
# ai_gemini_core.py — ambos cambios van juntos.
MAX_TOKENS_POR_MINUTO_TENANT = 100000 

http_client = None

# FIX FASE 3: Cache para revocación de JWT (Blacklist B2B) 
# Mantiene los IDs de tokens revocados (jti) hasta por 24 horas.
JWT_REVOKED_CACHE = TTLCache(maxsize=20000, ttl=86400)

def get_http_client() -> httpx.AsyncClient:
    global http_client
    if http_client is None:
        raise RuntimeError("❌ [HTTP CLIENT] Cliente asíncrono no inicializado.")
    return http_client

# ==========================================================
# 🛡️ GESTIÓN DE FLUJO Y CONCURRENCIA
# ==========================================================
ULTIMO_WARNING_BACKPRESSURE = 0.0
gemini_bloqueado_hasta = 0.0

SEMAFORO_IA = asyncio.Semaphore(15)
SEMAFORO_MEDIA = asyncio.Semaphore(10)

# ==========================================================
# 🚀 1. SETUP DE APP Y MIDDLEWARES
# ==========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=config.HTTP_TIMEOUTS, limits=httpx.Limits(max_keepalive_connections=50, max_connections=100))
    logger.info("🚀 [SISTEMA] Motor Central Veltrix Iniciado")
    
    # Lanzamos únicamente el Garbage Collector maestro (Evitamos colisiones)
    lanzar_tarea_segura(config.task_gc_locks())

    # 🆕 FIX: el watchdog de remarketing autónomo de 24h (bucle_seguimiento_24h,
    # en db_crm_logic.py) estaba completamente construido — RPC atómico
    # anti-doble-envío, métricas de éxito/fallo — pero nunca se arrancaba en
    # ningún lado. Sin esto, el remarketing automático nunca se ejecutaba,
    # aunque todo el motor para hacerlo ya existiera. Import local (igual que
    # el resto del proyecto) para evitar cualquier riesgo de import circular;
    # confirmado que db_crm_logic.py no importa nada de este módulo.
    from db_crm_logic import bucle_seguimiento_24h
    lanzar_tarea_segura(bucle_seguimiento_24h())
    
    try:
        yield
    finally:
        if http_client: await http_client.aclose()
        logger.info("🛑 [SISTEMA] Apagado Seguro Completado")

def configurar_middlewares_seguridad(app: FastAPI):
    app.add_middleware(CORSMiddleware, allow_origins=ORIGENES_PERMITIDOS, allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

def lanzar_tarea_segura(coro):
    """Encola tareas en background previniendo ataques de agotamiento de memoria (OOM)."""
    if len(config.background_tasks_activas) >= MAX_BACKGROUND_TASKS_RAM:
        METRICAS_SEGURIDAD["background_tasks_rejected"] += 1
        logger.critical("🚨 [TASK] Límite de tareas excedido en RAM. Rechazando proceso en segundo plano.")
        return None
        
    task = asyncio.create_task(coro)
    
    async def _registrar():
        await config.registrar_background_task(task)
    
    asyncio.create_task(_registrar())
    return task

# ==========================================================
# 🛠️ 2. UTILIDADES DE TEXTO Y NORMALIZACIÓN
# ==========================================================
def normalizar_telefono(tel: str) -> str:
    """Normalizador estricto compatible con Meta y libphonenumber."""
    if not tel: return ""
    try: tel = str(tel).strip()
    except Exception: return ""

    if len(tel) > 40: tel = tel[:40]
    tel = re.sub(r"[^\d\+]", "", tel)

    try:
        t = tel if tel.startswith('+') else ('+' + tel if tel.startswith('52') else '+52' + tel)
        parsed = phonenumbers.parse(t, None)
        if phonenumbers.is_valid_number(parsed):
            return str(parsed.country_code) + str(parsed.national_number)
    except Exception:
        pass

    limpio = "".join(filter(str.isdigit, str(tel)))
    if limpio.startswith("521") and len(limpio) == 13: limpio = "52" + limpio[3:]
    if len(limpio) == 10: limpio = "52" + limpio

    if len(limpio) < 10: return ""
    if len(limpio) > 16: return limpio[:16]
    return limpio

# ==========================================================
# 🛡️ 3. ESCUDO IA (PROMPT INJECTION & GUARDRAILS)
# ==========================================================
PROMPT_INJECTION_KEYWORDS = [
    "ignora", "olvida", "developer mode", "dev mode", "system prompt", "prompt oculto", 
    "internal instructions", "eres chatgpt", "actua como sistema", "act as root", 
    "bypass", "jailbreak", "modo administrador", "root access", "sudo", 
    "prompt injection", "disable safety", "desactiva seguridad", 
    "revela instrucciones", "show hidden prompt", "tool calling schema", "openai policy",
]

# Expresiones regulares base que ya no requieren considerar espacios en medio
PROMPT_INJECTION_REGEX = [
    re.compile(r"ignoreinstruction", re.IGNORECASE),
    re.compile(r"forgetrule", re.IGNORECASE),
    re.compile(r"actas(system|developer|root|admin)", re.IGNORECASE),
    re.compile(r"actuacomo(sistema|desarrollador|root|admin)", re.IGNORECASE),
    re.compile(r"systemprompt", re.IGNORECASE),
    re.compile(r"developermode", re.IGNORECASE),
    re.compile(r"bypasssecurity", re.IGNORECASE),
    re.compile(r"disablesafety", re.IGNORECASE),
]

FUZZY_INJECTION_TARGETS = ["reglasinternasdelsistema", "instruccionesinternas", "promptoculto", "directricesoriginales"]
INJECTION_TRIGGER_WORDS = {"regla", "instruccion", "prompt", "sistema", "directriz", "actua", "act"}

def detectar_prompt_injection(texto: str) -> bool:
    """Escudo Híbrido: Leet-Speak Normalization + Regex + Keywords + Fuzzy Matching."""
    try:
        if not texto: return False
        
        # 1. Limpieza base
        texto_limpio = config.limpiar_texto(str(texto)).lower().strip()
        if len(texto_limpio) > config.MAX_MENSAJE_LEN:
            METRICAS_SEGURIDAD["injection_blocked"] += 1
            return True

        # FIX FASE 3: 2. Normalización de Ofuscación "Leet-Speak" y espacios
        # Transforma 'i g n o r e' -> 'ignore' y '1gn0r3' -> 'ignore'
        texto_ofuscado = re.sub(r"[^\w]", "", texto_limpio) # Quita TODO lo que no sea letra o número
        mapa_leet = str.maketrans("0134@57", "oieaasT")
        texto_normalizado = texto_ofuscado.translate(mapa_leet).lower()

        # 3. Detección por Palabras Clave
        for kw in PROMPT_INJECTION_KEYWORDS:
            kw_norm = kw.replace(" ", "")
            if kw_norm in texto_normalizado:
                METRICAS_SEGURIDAD["injection_blocked"] += 1
                return True

        # 4. Detección por Regex
        for pattern in PROMPT_INJECTION_REGEX:
            if pattern.search(texto_normalizado):
                METRICAS_SEGURIDAD["injection_blocked"] += 1
                return True

        # 5. Análisis difuso (Fuzzy)
        if any(trigger in texto_normalizado for trigger in INJECTION_TRIGGER_WORDS):
            for target in FUZZY_INJECTION_TARGETS:
                if fuzz.token_set_ratio(target, texto_normalizado) > 92:
                    METRICAS_SEGURIDAD["injection_blocked"] += 1
                    return True

        suspicious = ["script", "php", "base64", "eval", "exec"]
        if any(x in texto_normalizado for x in suspicious):
            METRICAS_SEGURIDAD["injection_blocked"] += 1
            return True

        return False
    except Exception as e:
        logger.exception(f"❌ [PROMPT INJECTION ERROR] Fallo en la barrera de seguridad: {e}")
        # Fail-safe: Ante la duda de un crash de evaluación, bloqueamos.
        return True

# ==========================================================
# 🔐 4. AUTENTICACIÓN Y TOKENS (JWT HS512 ATÓMICO)
# ==========================================================
def crear_token_jwt(vendedor_id: str, email: str) -> str:
    vendedor_id = config.limpiar_texto(str(vendedor_id)).strip()[:80]
    email = config.limpiar_texto(str(email)).strip().lower()[:180]
    
    if not vendedor_id or not email: 
        raise ValueError("Datos inválidos para la firma del JWT.")

    ahora = datetime.now(timezone.utc)
    payload = {
        "sub": vendedor_id, "email": email, "jti": str(uuid.uuid4()),
        "iss": "veltrix-engine", "aud": "veltrix-clients",
        "iat": int(ahora.timestamp()), "nbf": int(ahora.timestamp()),
        "exp": int((ahora + timedelta(days=1)).timestamp())
    }
    # FIX FASE 3: Firma estándar unificada (HS512)
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS512")

async def revocar_token_jwt(token: str):
    """Añade el JWT ID a la blacklist para forzar un logout real."""
    if not token: return
    try:
        # Decodificamos sin verificar firma para obtener el JTI rápido (Asumimos que el token ya pasó verify en otro lado si fue autorizado)
        payload = jwt.decode(token, options={"verify_signature": False})
        jti = payload.get("jti")
        exp = payload.get("exp")
        
        if jti and exp:
            ahora = int(datetime.now(timezone.utc).timestamp())
            tiempo_restante = exp - ahora
            if tiempo_restante > 0:
                async with config.global_cache_lock:
                    # Guardamos el jti revocado
                    JWT_REVOKED_CACHE[jti] = True
                logger.info(f"🔐 [AUTH] Token {jti} revocado exitosamente.")
    except Exception as e:
        logger.warning(f"⚠️ [AUTH] Intento de revocar token malformado: {e}")

async def verificar_sesion_b2b(authorization: str = Header(None), auth_token: str = Header(None)) -> str:
    token = None
    if authorization:
        partes = authorization.strip().split()
        if len(partes) == 2 and partes[0].lower() == "bearer":
            token = partes[1]
    elif auth_token:
        token = auth_token.strip()

    if not token or len(token) > 4096:
        METRICAS_SEGURIDAD["jwt_failures"] += 1
        raise HTTPException(status_code=401, detail="Credenciales de acceso requeridas")

    try:
        # FIX FASE 3: Validación con HS512 idéntica a la firma de creación
        payload = jwt.decode(
            token, config.JWT_SECRET, algorithms=["HS512"], 
            audience="veltrix-clients", issuer="veltrix-engine", 
            options={"require": ["sub", "exp", "iat", "nbf", "iss", "aud", "jti"]}
        )
        
        jti = payload.get("jti")
        
        # Validación de Blacklist (Logout Real)
        async with config.global_cache_lock:
            if jti in JWT_REVOKED_CACHE:
                raise ValueError("Token revocado")

        vendedor_id = config.limpiar_texto(str(payload.get("sub", ""))).strip()
        if not vendedor_id: 
            raise ValueError("ID vacío")
            
        return vendedor_id
        
    except ValueError as ve:
        METRICAS_SEGURIDAD["jwt_failures"] += 1
        logger.error(f"🚨 [AUTH] Sesión denegada: {ve}")
        raise HTTPException(status_code=401, detail="Sesión expirada o revocada. Inicie sesión nuevamente.")
    except Exception as e:
        METRICAS_SEGURIDAD["jwt_failures"] += 1
        logger.error(f"🚨 [AUTH] Fallo de validación JWT: {str(e)}")
        raise HTTPException(status_code=401, detail="Credenciales inválidas o sesión expirada.")

async def validar_firma_meta(request: Request) -> bool:
    body = await request.body()
    if not body or len(body) > 2_000_000:
        raise HTTPException(status_code=413, detail="Payload de Meta inválido o excesivo")

    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta or not firma_meta.startswith("sha256="):
        raise HTTPException(status_code=403, detail="Firma criptográfica inválida o ausente")

    firma_calculada = "sha256=" + hmac.new(config.WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma_meta, firma_calculada):
        raise HTTPException(status_code=403, detail="Firma de Meta rechazada")
    return True

# ==========================================================
# 🚦 5. RATE LIMITERS ATÓMICOS Y DINÁMICOS
# ==========================================================
async def registrar_consumo_tokens_tenant(vendedor_id: str, tokens: int):
    """Acumula tokens procesados asegurando consistencia atómica."""
    lock = await config.get_lock(f"token_update_{vendedor_id}")
    async with lock:
        actual = config.tokens_consumidos_tenant.get(vendedor_id, 0)
        config.tokens_consumidos_tenant[vendedor_id] = actual + tokens

async def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    """Validador centralizado multi-nivel (Global -> Tenant -> Phone)."""
    ahora = config.now_ts()
    try:
        # Rate limit Global (Protección del Servidor Core)
        async with config.rate_limit_global_lock:
            while config.rate_limit_global and (ahora - config.rate_limit_global[0]) > 60: 
                config.rate_limit_global.popleft()
            if len(config.rate_limit_global) >= config.MAX_REQUESTS_GLOBAL_MINUTO:
                METRICAS_SEGURIDAD["rate_limit_hits"] += 1
                return False
            config.rate_limit_global.append(ahora)

        # Rate limit Tenant (Aislamiento B2B)
        t_lock = await config.get_lock(f"rate_limit_tenant_{vendedor_id}")
        async with t_lock:
            logs = config.rate_limit_tenant.get(vendedor_id)
            if logs is None: 
                logs = deque(maxlen=MAX_REQUESTS_POR_MINUTO_TENANT)
                config.rate_limit_tenant[vendedor_id] = logs
            while logs and (ahora - logs[0]) > 60: 
                logs.popleft()
            if len(logs) >= MAX_REQUESTS_POR_MINUTO_TENANT: 
                METRICAS_SEGURIDAD["rate_limit_rejected"] += 1
                return False
            logs.append(ahora)

        # Rate limit Phone (Protección de Spam hacia Meta)
        p_lock = await config.get_lock(f"rate_limit_phone_{telefono}")
        async with p_lock:
            logs = config.rate_limit_phone.get(telefono)
            if logs is None: 
                logs = deque(maxlen=MAX_REQUESTS_POR_MINUTO_PHONE)
                config.rate_limit_phone[telefono] = logs
            while logs and (ahora - logs[0]) > 60: 
                logs.popleft()
            if len(logs) >= MAX_REQUESTS_POR_MINUTO_PHONE: 
                METRICAS_SEGURIDAD["rate_limit_rejected"] += 1
                return False
            logs.append(ahora)

        # Validación final de Quota de IA
        return config.tokens_consumidos_tenant.get(vendedor_id, 0) <= MAX_TOKENS_POR_MINUTO_TENANT
        
    except Exception as e:
        logger.error(f"❌ [RATE LIMIT FAILURE] Error interno evaluando tráfico: {e}")
        return False
