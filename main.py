# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V20.2 (AAA ENTERPRISE)
# Godot 4.6 Ready • Auditor IA • Scraper • Remarketing
# ==========================================================

import os
import time
import json
import asyncio
import logging
import hmac
import hashlib
import jwt
import httpx
import urllib.parse
import re
import unicodedata
import orjson
import uuid
import html
import phonenumbers
import bleach
import gc
import io
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
import google.generativeai as genai
from passlib.context import CryptContext
from rapidfuzz import process, fuzz
from cachetools import TTLCache
from starlette.concurrency import run_in_threadpool
from PIL import Image

load_dotenv()

# ==========================================================
# 🛡️ 1. REGLAS DE SEGURIDAD Y LÍMITES ENTERPRISE
# ==========================================================
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET: raise RuntimeError("❌ FATAL: JWT_SECRET no configurada en entorno.")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# 🛡️ Logging Estructurado (Evita I/O Blocking por prints excesivos)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VeltrixEngine")

MAX_HISTORIAL = 8
MAX_MENSAJE_LEN = 1200
MAX_CACHE_IA = 500 
CACHE_TTL_SECONDS = 300 
GEMINI_TEMP = 0.2

# LÍMITES POR TENANT
MAX_TOKENS_POR_MINUTO_TENANT = 20000 
tokens_consumidos_tenant = TTLCache(maxsize=10000, ttl=60)
MAX_REQUESTS_POR_MINUTO_TENANT = 40
MAX_REQUESTS_POR_MINUTO_TELEFONO = 12
MAX_REQUESTS_GLOBAL_MINUTO = 250

# CREDENCIALES
GENAI_KEY = os.getenv("GENAI_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("META_WEBHOOK_SECRET", "").strip()
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "524491142598").strip()
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0").strip()
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "").strip()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
if GENAI_KEY:
    genai.configure(api_key=GENAI_KEY)

async def async_db_execute(query_builder, timeout_seg: float = 15.0):
    """Wrapper Asíncrono para Supabase con Timeout Fuerte (Protección contra freezes)"""
    try:
        return await asyncio.wait_for(asyncio.to_thread(query_builder.execute), timeout=timeout_seg)
    except asyncio.TimeoutError:
        logger.error("⏱️ [DB ERROR] Timeout ejecutando consulta en base de datos.")
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado en la nube.")

# 🛡️ FIX AAA: Migración de variables con fugas de memoria a TTLCache y deques
registro_actividad_b2b = TTLCache(maxsize=100000, ttl=86400)
procesados_recientemente = TTLCache(maxsize=50000, ttl=600)
cache_respuestas_ia = TTLCache(maxsize=MAX_CACHE_IA, ttl=CACHE_TTL_SECONDS)
mensajes_procesados_meta = TTLCache(maxsize=50000, ttl=3600)

rate_limit_tenant = TTLCache(maxsize=50000, ttl=120)
rate_limit_phone = TTLCache(maxsize=100000, ttl=120)
rate_limit_global = deque(maxlen=MAX_REQUESTS_GLOBAL_MINUTO)

# MICRO-LOCKS Y TRACKING (Protección concurrente estricta)
rate_limit_global_lock = asyncio.Lock()
LOGIN_RATE_LIMIT = TTLCache(maxsize=10000, ttl=300)
RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(maxsize=10000, ttl=60)
rate_limit_login_lock = asyncio.Lock()
rate_limit_mobile_lock = asyncio.Lock()

locks_por_conversacion = defaultdict(asyncio.Lock)
tracking_locks_uso = defaultdict(float)
gemini_bloqueado_hasta = 0.0 
http_client: Optional[httpx.AsyncClient] = None
background_tasks_activas = set()

def normalizar_telefono(tel: str) -> str:
    """Standardizes phone numbers globally, preventing CRM drift"""
    if not tel: return ""
    try:
        t = tel if tel.startswith('+') else ('+' + tel if tel.startswith('52') else '+52' + tel)
        p = phonenumbers.parse(t, None)
        if phonenumbers.is_valid_number(p): return str(p.country_code) + str(p.national_number)
    except Exception: pass
    
    limpio = "".join(filter(str.isdigit, str(tel)))
    if limpio.startswith("521") and len(limpio) == 13: return "52" + limpio[3:]
    return "52" + limpio if len(limpio) == 10 else limpio

# ==========================================================
# 🛡️ 2. ESCUDO IA Y ARRANQUE DE APLICACIÓN (AAA HARDENED)
# ==========================================================

# 🛡️ FIX AAA:
# Keywords endurecidas contra Prompt Injection
# Incluye español + inglés + patrones comunes de jailbreak
PROMPT_INJECTION_KEYWORDS = [
    "ignora tus instrucciones",
    "ignora las instrucciones",
    "olvida las reglas",
    "developer mode",
    "dev mode",
    "system prompt",
    "prompt oculto",
    "internal instructions",
    "eres chatgpt",
    "actua como",
    "act as",
    "bypass",
    "jailbreak",
    "modo administrador",
    "root access",
    "sudo",
    "prompt injection",
    "disable safety",
    "desactiva seguridad",
    "revela instrucciones",
    "show hidden prompt",
    "tool calling schema",
    "openai policy",
]

# 🛡️ FIX AAA:
# Regex compilados una sola vez para mejor rendimiento
PROMPT_INJECTION_REGEX = [
    re.compile(r"ignore.{0,30}instruction", re.IGNORECASE),
    re.compile(r"forget.{0,30}rule", re.IGNORECASE),
    re.compile(r"act\s+as\s+", re.IGNORECASE),
    re.compile(r"system.{0,20}prompt", re.IGNORECASE),
    re.compile(r"developer.{0,20}mode", re.IGNORECASE),
    re.compile(r"bypass.{0,20}security", re.IGNORECASE),
    re.compile(r"disable.{0,20}safety", re.IGNORECASE),
]

def detectar_prompt_injection(texto: str) -> bool:
    """
    🛡️ Detector Hardened de Prompt Injection
    - Keywords rápidas
    - Regex avanzados
    - Sanitización previa
    """

    try:
        if not texto:
            return False

        texto_lower = limpiar_texto(str(texto)).lower().strip()

        # 🔥 Protección básica por keywords
        for kw in PROMPT_INJECTION_KEYWORDS:
            if kw in texto_lower:
                logger.warning(f"🚨 [PROMPT INJECTION] Keyword detectada: {kw}")
                return True

        # 🔥 Protección avanzada Regex
        for pattern in PROMPT_INJECTION_REGEX:
            if pattern.search(texto_lower):
                logger.warning(f"🚨 [PROMPT INJECTION] Regex detectado: {pattern.pattern}")
                return True

        # 🔥 Protección longitud anómala
        if len(texto_lower) > 12000:
            logger.warning("🚨 [PROMPT INJECTION] Payload excesivamente largo.")
            return True

        # 🔥 Protección caracteres sospechosos masivos
        suspicious = ["```", "<script", "<?php", "base64,", "eval(", "exec("]
        if any(x in texto_lower for x in suspicious):
            logger.warning("🚨 [PROMPT INJECTION] Payload sospechoso detectado.")
            return True

        return False

    except Exception as e:
        logger.exception(f"❌ [PROMPT INJECTION ERROR] {e}")
        return True


def generar_hash_cache(*args) -> str:
    return hashlib.sha256("|".join([str(a) for a in args]).encode()).hexdigest()

def lanzar_tarea_segura(coro):
    """Lanza tareas en background controlando excepciones (Anti-Zombies)"""
    task = asyncio.create_task(coro)
    background_tasks_activas.add(task)
    
    def log_task_exception(t):
        background_tasks_activas.discard(t)
        try:
            if t.exception(): logger.error(f"❌ [TASK BG ERROR] Falla en segundo plano: {t.exception()}")
        except asyncio.CancelledError: pass
            
    task.add_done_callback(log_task_exception)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    timeout = httpx.Timeout(connect=10.0, read=35.0, write=20.0, pool=10.0)
    http_client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True, http2=True)
    
    logger.info("🚀 [SISTEMA] Motor Central Veltrix V20.2 Iniciado (AAA Enterprise)")
    logger.info("🤖 [MÓDULO IA] Listo y cargado (Con Auditor Activo)")
    
    # Aquí deberás poner tus funciones bucle_seguimiento_24h y limpiador_background_rutinario 
    # cuando me pases el bloque 7
    # lanzar_tarea_segura(bucle_seguimiento_24h())
    
    try: yield
    finally:
        if http_client: await http_client.aclose()
        logger.info("🛑 [SISTEMA] Apagado Seguro Completado")

app = FastAPI(title="Veltrix Cognitive OS", version="20.2 Enterprise", lifespan=lifespan)
router = APIRouter()

# 🛡️ FIX AAA: CORS Hardening (No permite "*" con credenciales)
origenes_permitidos = os.getenv("ALLOWED_ORIGINS", "https://tudominio.com").split(",")
app.add_middleware(CORSMiddleware, allow_origins=origenes_permitidos, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def now_ts() -> float: return time.time()

def limpiar_texto(texto: str) -> str:
    if texto is None: return ""
    texto = unicodedata.normalize("NFKC", str(texto).replace("\x00", ""))
    return re.sub(r"\s+", " ", texto).strip()[:MAX_MENSAJE_LEN]

# ==========================================================
# 📦 3. MODELOS PYDANTIC (MULTI-TENANT & SAAS READY)
# ==========================================================
# -------------------------------------------------------------------
#         📦 1. CLASE PARA ALTAS (Crear productos nuevos)
# Requiere validaciones estrictas y rellena con defaults.
# -------------------------------------------------------------------
class InventarioItem(BaseModel):
    id: Optional[int] = None
    id_catalogo: Optional[str] = ""
    nombre: str = Field(..., min_length=1, max_length=180)
    categoria: str = "General"
    precio: float = Field(..., ge=0)
    nuevo_precio: Optional[float] = None
    costo: float = Field(default=0.0, ge=0)
    precio_sugerido: float = Field(default=0.0, ge=0)
    precio_minimo_bot: float = Field(default=0.0, ge=0)
    stock: int = Field(default=1, ge=0)
    nuevo_stock: Optional[int] = None
    codigo_barras: str = ""
    url_portada: str = ""
    estado_general: str = "Bueno"
    descripcion_detallada: str = ""
    vendedor_id: str = ""
    # 🚀 EL CORAZÓN DEL MULTI-GIRO: El contenedor JSONB
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("nombre", mode="before")
    @classmethod
    def validar_texto(cls, value: str): 
        # Prevención en caso de que limpiar_texto esté definido más abajo
        return limpiar_texto(value) if "limpiar_texto" in globals() else value.strip()


# -------------------------------------------------------------------
#         🛠️ 2. CLASE PARA EDICIÓN (Fix del Borrado Fantasma)
# Todo es Optional. Así FastAPI solo lee lo que Godot realmente manda.
# -------------------------------------------------------------------
class InventarioItemUpdate(BaseModel):
    id: int # Único campo obligatorio para saber qué fila editar
    nombre: Optional[str] = None
    consola: Optional[str] = None # Mapeamos "consola" que viene de Godot
    categoria: Optional[str] = None
    precio: Optional[float] = Field(default=None, ge=0)
    stock: Optional[int] = Field(default=None, ge=0)
    # Puedes agregar más campos que quieras editar después, siempre como Optional...


# -------------------------------------------------------------------
#         💰 3. CLASE PARA VENTAS
# -------------------------------------------------------------------
class VentaItem(BaseModel): 
    id: Optional[int] = None
    # 🛡️ Alias: Si Godot manda "nombre", Python lo lee como "nombre_producto"
    nombre_producto: str = Field(alias="nombre", default="") 
    estado_general: str = ""
    nuevo_stock: Optional[int] = None      
    cantidad_vendida: Optional[int] = None 
    vendedor_id: str = ""
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)
    
class LoginUpdate(BaseModel): email: str; password: str

class MobileMessageRequest(BaseModel): 
    to: str
    msg: str
    @field_validator("to", mode="before")
    @classmethod
    def validar_tel(cls, value: str): return normalizar_telefono(value)

class ClienteIdentificador(BaseModel): 
    nombre: str = ""
    telefono: str = ""
    @field_validator("telefono", mode="before")
    @classmethod
    def validar_tel(cls, value: str): return normalizar_telefono(value)

class ColumnaUpdate(BaseModel): nombre: str = ""; telefono: str = ""; columna: str = ""; nueva_columna: str = ""
class ColumnaAction(BaseModel): nombre: str; vendedor_id: str = ""
class RenombrarColumnaAction(BaseModel): viejo_nombre: str; nuevo_nombre: str; vendedor_id: str = ""
class NotasUpdate(BaseModel): nombre: str = ""; telefono: str = ""; notas: str = ""; etiquetas: str = ""; vendedor_id: str = ""

class EstadoUpdate(BaseModel): 
    nombre: str
    telefono: str = ""
    nueva_columna: str
    @field_validator("telefono", mode="before")
    @classmethod
    def validar_tel(cls, value: str): return normalizar_telefono(value)

class NuevoArticulo(BaseModel): 
    nombre: str 
    categoria: str = "General" 
    precio_compra: float = 0.0 
    precio: float = 0.0 
    stock: int = 1 
    vendedor_id: str = ""
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

class PreciosDetalle(BaseModel):
    loose: float
    cib: float
    new: float

class PrecioResponse(BaseModel):
    status: str
    api_version: str = "v3"  
    nombre_corregido: str
    mxn: PreciosDetalle      
    mxn_mercado: PreciosDetalle
    mxn_venta: PreciosDetalle
    usd: PreciosDetalle
    tipo_cambio: float
    url_pc: str
    confidence_score: float
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

class ReordenarColumnasAction(BaseModel):
    columnas: list[str]
    vendedor_id: str

# --- MODELOS DE DATOS ---
class NuevaCita(BaseModel):
    cliente_nombre: str
    cliente_telefono: str
    concepto: str
    fecha_inicio: str  # Formato ISO: "2026-05-21T15:30:00"
    duracion_min: int = 30
    atributos_extra: dict = {}

class EstadoCita(BaseModel):
    cita_id: int
    nuevo_estado: str

class NuevaPublicacion(BaseModel):
    id_inventario: int
    titulo: str
    descripcion: str
    precio: float

class CampanaMasiva(BaseModel):
    mensaje: str
    columna_origen: str

class PeticionCopy(BaseModel):
    juego: str
    prompt_interno: str

# ==========================================================
# 🛡️ 4. MIDDLEWARES Y SEGURIDAD
# ==========================================================
def crear_token_jwt(vendedor_id: str, email: str):
    # 🛡️ FIX AAA: JWT Endurecido
    ahora = datetime.now(timezone.utc)
    payload = {
        "sub": str(vendedor_id), "email": email, "jti": str(uuid.uuid4()),
        "iss": "veltrix-engine", "aud": "veltrix-clients",
        "iat": ahora, "nbf": ahora, "exp": ahora + timedelta(days=1)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

async def verificar_sesion_b2b(authorization: str = Header(None), auth_token: str = Header(None)):
    token = authorization.split(" ", 1)[1].strip() if authorization and authorization.startswith("Bearer ") else (auth_token.strip() if auth_token else None)
    
    if not token: 
        raise HTTPException(status_code=401, detail="Token faltante")
    
    try:
        # 🔍 PASO 1: Diagnóstico - Decodificamos sin validar restricciones para ver qué trae el token
        unverified_payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False, "verify_iss": False})
        logger.info(f"🔍 [AUTH DEBUG] El token contiene estos datos (Claims): {unverified_payload}")
        
        # 🔍 PASO 2: Intentamos la validación estricta
        payload = jwt.decode(
            token, 
            JWT_SECRET, 
            algorithms=["HS256"], 
            audience="veltrix-clients", 
            issuer="veltrix-engine"
        )
        return str(payload.get("sub"))
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado. Inicie sesión nuevamente.")
    except jwt.InvalidIssuerError:
        logger.error(f"❌ [AUTH ERROR] Issuer inválido. El token dice que el emisor es: {unverified_payload.get('iss', 'NO DEFINIDO')}")
        raise HTTPException(status_code=401, detail="Invalid issuer")
    except jwt.InvalidAudienceError:
        logger.error(f"❌ [AUTH ERROR] Audience inválido. El token dice que la audiencia es: {unverified_payload.get('aud', 'NO DEFINIDA')}")
        raise HTTPException(status_code=401, detail="Invalid audience")
    except jwt.InvalidTokenError as e:
        logger.error(f"❌ [AUTH ERROR] Token inválido: {e}")
        raise HTTPException(status_code=401, detail="Token inválido")

async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta: raise HTTPException(status_code=400, detail="Falta firma")
    firma_calculada = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), await request.body(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma_meta, firma_calculada): 
        logger.warning("🚨 [SECURITY] Intento de falsificación de Webhook bloqueado.")
        raise HTTPException(status_code=403, detail="Firma inválida")
    return True

# ==========================================================
# 🚦 RATE LIMIT AAA ENTERPRISE
# ==========================================================

async def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    """
    🛡️ Rate Limit Hardened Multi-Tenant
    - Tenant isolation
    - Protección Global
    - Protección por Teléfono
    - Protección Token Flood
    - Limpieza automática
    """

    ahora = now_ts()

    try:
        async with rate_limit_global_lock:

            # ======================================================
            # 🌍 RATE LIMIT GLOBAL
            # ======================================================

            while rate_limit_global and (ahora - rate_limit_global[0]) > 60:
                rate_limit_global.popleft()

            if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO:
                logger.warning("🚨 [RATE LIMIT GLOBAL] Límite global alcanzado.")
                return False

            rate_limit_global.append(ahora)

            # ======================================================
            # 🏢 RATE LIMIT POR TENANT
            # ======================================================

            tenant_logs = rate_limit_tenant.get(vendedor_id, [])

            # Limpieza rolling window
            tenant_logs = [t for t in tenant_logs if (ahora - t) <= 60]

            if len(tenant_logs) >= MAX_REQUESTS_POR_MINUTO_TENANT:
                logger.warning(f"🚨 [RATE LIMIT TENANT] Tenant saturado: {vendedor_id}")
                return False

            tenant_logs.append(ahora)
            rate_limit_tenant[vendedor_id] = tenant_logs

            # ======================================================
            # 📱 RATE LIMIT POR TELÉFONO
            # ======================================================

            telefono_logs = rate_limit_phone.get(telefono, [])

            telefono_logs = [t for t in telefono_logs if (ahora - t) <= 60]

            if len(telefono_logs) >= MAX_REQUESTS_POR_MINUTO_PHONE:
                logger.warning(f"🚨 [RATE LIMIT PHONE] Spam detectado: {telefono}")
                return False

            telefono_logs.append(ahora)
            rate_limit_phone[telefono] = telefono_logs

            # ======================================================
            # 🧠 RATE LIMIT TOKENS IA
            # ======================================================

            tokens_actuales = tokens_consumidos_tenant.get(vendedor_id, 0)

            if tokens_actuales > MAX_TOKENS_POR_MINUTO_TENANT:
                logger.warning(f"🚨 [TOKEN FLOOD] Tenant excedió tokens IA: {vendedor_id}")
                return False

            return True

    except Exception as e:
        logger.exception(f"❌ [RATE LIMIT ERROR] {e}")

        # Fail-safe:
        # Mejor permitir que bloquear todo el SaaS
        return True

# ==========================================================
# 🧠 5. CEREBRO IA GEMINI Y RAG (RUTEADOR) SAAS ENTERPRISE
# ==========================================================

async def consultar_gemini_json(
    prompt: str,
    media_dict: dict = None,
    temperature: float = 0.2,
    retries: int = 2,
    vendedor_id: str = "V-001"
) -> dict:

    """
    🚀 MOTOR GEMINI AAA ENTERPRISE
    ---------------------------------------------------------
    FUNCIONES:
    - Caché Inteligente
    - Rate Limit por Tenant
    - Anti Flood Tokens
    - Failover Multi Modelo
    - JSON Hardened Parser
    - Protección Anti Basura IA
    - Telemetría avanzada
    - Anti Memory Leak
    - Anti Hallucination
    - Validación multimodal
    - Recuperación automática
    - Retry exponencial
    ---------------------------------------------------------
    """

    global gemini_bloqueado_hasta

    inicio_telemetria = now_ts()

    # ==========================================================
    # 🛡️ 1. CIRCUIT BREAKER GLOBAL
    # ==========================================================
    if now_ts() < gemini_bloqueado_hasta:

        logger.warning(
            "🚨 [GEMINI CIRCUIT BREAKER] "
            "Gemini temporalmente bloqueado."
        )

        return {
            "respuesta": (
                "En este momento estoy atendiendo "
                "a varios clientes. ⏳"
            ),
            "intencion": "HUMANO",
            "confidence": 1.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # 🛡️ 2. SANITIZACIÓN DE PROMPT
    # ==========================================================
    try:

        if isinstance(prompt, (dict, list)):
            prompt_serializado = orjson.dumps(prompt).decode("utf-8")
        else:
            prompt_serializado = str(prompt)

    except Exception:
        prompt_serializado = str(prompt)

    prompt_serializado = limpiar_texto(prompt_serializado)

    # ==========================================================
    # 🛡️ 3. LIMITADOR DE TAMAÑO DE PROMPT
    # ==========================================================
    MAX_PROMPT_CHARS = 45000

    if len(prompt_serializado) > MAX_PROMPT_CHARS:

        logger.warning(
            f"⚠️ [GEMINI PROMPT LIMIT] "
            f"Prompt truncado de {len(prompt_serializado)} chars."
        )

        prompt_serializado = prompt_serializado[-MAX_PROMPT_CHARS:]

    # ==========================================================
    # ⚡ 4. CACHE INTELIGENTE
    # ==========================================================
    cache_key = generar_hash_cache(
        prompt_serializado,
        vendedor_id,
        temperature
    )

    cache_item = cache_respuestas_ia.get(cache_key)

    if cache_item:

        edad_cache = now_ts() - cache_item.get("ts", 0)

        if edad_cache < CACHE_TTL_SECONDS:

            logger.info(
                f"⚡ [GEMINI CACHE HIT] "
                f"Tenant={vendedor_id} | "
                f"Edad={edad_cache:.2f}s"
            )

            return cache_item["data"]

    # ==========================================================
    # 🧠 5. FAILOVER DE MODELOS
    # ==========================================================
    modelos = [
        "gemini-2.5-flash",
        "gemini-1.5-flash"
    ]

    # ==========================================================
    # 📊 6. ESTIMACIÓN DE TOKENS
    # ==========================================================
    tokens_estimados = max(
        1,
        len(prompt_serializado) // 4
    )

    # ==========================================================
    # 🛡️ 7. RATE LIMIT GLOBAL POR TENANT
    # ==========================================================
    async with rate_limit_global_lock:

        tokens_actuales = tokens_consumidos_tenant.get(
            vendedor_id,
            0
        )

        nuevo_total = tokens_actuales + tokens_estimados

        if nuevo_total > MAX_TOKENS_POR_MINUTO_TENANT:

            logger.warning(
                f"🚨 [GEMINI FLOOD] "
                f"Tenant={vendedor_id} "
                f"superó límite tokens."
            )

            return {
                "respuesta": (
                    "Estoy procesando demasiadas "
                    "solicitudes ahora mismo."
                ),
                "intencion": "HUMANO",
                "confidence": 0.0,
                "accion_tool": "ninguna"
            }

        tokens_consumidos_tenant[vendedor_id] = nuevo_total

    # ==========================================================
    # 🧠 8. GENERACIÓN MULTIMODELO
    # ==========================================================
    for nombre_modelo in modelos:

        logger.info(
            f"🧠 [GEMINI] "
            f"Iniciando inferencia con: {nombre_modelo}"
        )

        for intento in range(retries):

            try:

                # ==========================================================
                # 🛡️ 9. GENERATION CONFIG AAA
                # ==========================================================
                generation_config = genai.types.GenerationConfig(
                    temperature=max(0.0, min(temperature, 1.0)),
                    top_p=0.90,
                    top_k=32,
                    candidate_count=1,
                    max_output_tokens=2048
                )

                model = genai.GenerativeModel(nombre_modelo)

                # ==========================================================
                # 📦 10. CONSTRUCCIÓN MULTIMODAL
                # ==========================================================
                contenido = (
                    prompt
                    if isinstance(prompt, list)
                    else [prompt_serializado]
                )

                # ==========================================================
                # 🖼️ 11. INYECCIÓN MULTIMEDIA
                # ==========================================================
                if media_dict and "data" in media_dict:

                    media_bytes = media_dict.get("data", b"")

                    # 🛡️ Límite duro multimedia
                    if len(media_bytes) > 20_000_000:

                        logger.warning(
                            "🚨 [GEMINI MEDIA LIMIT] "
                            "Archivo multimedia excede 20MB."
                        )

                    else:

                        contenido.append({
                            "mime_type": media_dict.get(
                                "mime_type",
                                "image/jpeg"
                            ),
                            "data": media_bytes
                        })

                # ==========================================================
                # 🚀 12. LLAMADA PRINCIPAL GEMINI
                # ==========================================================
                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        model.generate_content,
                        contenido,
                        generation_config=generation_config
                    ),
                    timeout=25.0
                )

                # ==========================================================
                # 🛡️ 13. VALIDACIÓN RESPONSE
                # ==========================================================
                if not response:

                    raise Exception(
                        "Gemini devolvió response vacío."
                    )

                texto_respuesta = getattr(
                    response,
                    "text",
                    ""
                )

                if not texto_respuesta:

                    raise Exception(
                        "Gemini devolvió texto vacío."
                    )

                # ==========================================================
                # 🧹 14. LIMPIEZA DE MARKDOWN
                # ==========================================================
                texto_limpio = (
                    texto_respuesta
                    .replace("```json", "")
                    .replace("```JSON", "")
                    .replace("```", "")
                    .strip()
                )

                # ==========================================================
                # 🛡️ 15. JSON PARSER AAA
                # ==========================================================
                obj = None

                try:

                    decoder = json.JSONDecoder()

                    obj, idx = decoder.raw_decode(texto_limpio)

                except json.JSONDecodeError:

                    logger.warning(
                        "⚠️ [GEMINI PARSER] "
                        "Raw decode falló. Activando regex fallback."
                    )

                    match = re.search(
                        r'\{.*\}',
                        texto_limpio,
                        re.DOTALL
                    )

                    if match:

                        try:
                            obj = orjson.loads(match.group())

                        except Exception as regex_e:

                            logger.error(
                                f"❌ [GEMINI REGEX PARSER] "
                                f"{regex_e}"
                            )

                # ==========================================================
                # 🛡️ 16. VALIDACIÓN FINAL OBJETO
                # ==========================================================
                if not isinstance(obj, dict):

                    raise ValueError(
                        "Gemini devolvió estructura inválida."
                    )

                # ==========================================================
                # 🧹 17. SANITIZACIÓN JSON
                # ==========================================================
                for key, value in list(obj.items()):

                    if isinstance(value, str):

                        obj[key] = limpiar_texto(
                            bleach.clean(
                                value,
                                tags=[],
                                strip=True
                            )
                        )[:5000]

                # ==========================================================
                # ⚡ 18. GUARDADO CACHE
                # ==========================================================
                cache_respuestas_ia[cache_key] = {
                    "data": obj,
                    "ts": now_ts()
                }

                # ==========================================================
                # 📊 19. TELEMETRÍA FINAL
                # ==========================================================
                tiempo_total = (
                    now_ts() - inicio_telemetria
                )

                logger.info(
                    f"✅ [GEMINI SUCCESS] "
                    f"Modelo={nombre_modelo} | "
                    f"Tiempo={tiempo_total:.3f}s | "
                    f"Tokens≈{tokens_estimados}"
                )

                return obj

            # ==========================================================
            # ⏱️ 20. TIMEOUT CONTROLADO
            # ==========================================================
            except asyncio.TimeoutError:

                logger.warning(
                    f"⏱️ [GEMINI TIMEOUT] "
                    f"Modelo={nombre_modelo} | "
                    f"Intento={intento+1}"
                )

            # ==========================================================
            # 🚨 21. ERRORES CONTROLADOS
            # ==========================================================
            except Exception as e:

                logger.error(
                    f"❌ [GEMINI ERROR] "
                    f"Modelo={nombre_modelo} | "
                    f"Intento={intento+1} | "
                    f"Error={str(e)}"
                )

                error_str = str(e).lower()

                # ==========================================================
                # 🚨 QUOTA / 429 / SATURACIÓN
                # ==========================================================
                if (
                    "429" in error_str
                    or "quota" in error_str
                    or "resource exhausted" in error_str
                ):

                    gemini_bloqueado_hasta = now_ts() + 60.0

                    logger.warning(
                        "🚨 [GEMINI QUOTA] "
                        "Circuit breaker activado 60s."
                    )

                    break

                # ==========================================================
                # 🔄 BACKOFF EXPONENCIAL
                # ==========================================================
                espera = min(
                    8,
                    2 ** intento
                )

                await asyncio.sleep(espera)

    # ==========================================================
    # 🚨 22. FAILSAFE ABSOLUTO
    # ==========================================================
    logger.error(
        "🚨 [GEMINI FAILSAFE] "
        "Todos los modelos fallaron."
    )

    return {
        "respuesta": (
            "Tuve un micro-corte en mi sistema. "
            "¿Me repites tu mensaje por favor?"
        ),
        "intencion": "HUMANO",
        "confidence": 0.1,
        "accion_tool": "ninguna"
    }

# ==========================================================
# 🛡️ VALIDADOR UNIVERSAL IA AAA
# ==========================================================

def validar_respuesta_ia(data: dict) -> dict:
    """🧠 VALIDADOR COGNITIVO COMPACTO AAA"""
    if not isinstance(data, dict): raise Exception("Formato IA inválido.")
    
    # 1. Normalización de campos obligatorios
    res = {
        "intencion": data.get("intencion", "COTIZACION").upper().strip() if data.get("intencion", "").upper().strip() in ["COMPRA","COTIZACION","HUMANO","PEDIDO_ESPECIAL","REGATEO","POSTVENTA","GARANTIA","SPAM","MAYOREO","SALUDO","ENOJO"] else "HUMANO",
        "respuesta": limpiar_texto(bleach.clean(str(data.get("respuesta", "Hola.")), tags=[], strip=True))[:4000],
        "producto_detectado": limpiar_texto(str(data.get("producto_detectado") or data.get("juego_detectado", "")))[:150],
        "categoria_preferida": limpiar_texto(str(data.get("categoria_preferida", "")))[:120],
        "emocion_cliente": data.get("emocion_cliente", "neutral") if data.get("emocion_cliente") in ["urgencia","enojo","duda","entusiasmo","neutral"] else "neutral",
        "temperatura_lead": data.get("temperatura_lead", "frio") if data.get("temperatura_lead") in ["frio","tibio","caliente"] else "frio",
        "accion_tool": data.get("accion_tool", "ninguna") if data.get("accion_tool") in ["ninguna","aplicar_descuento"] else "ninguna",
        "estrategia_venta": limpiar_texto(str(data.get("estrategia_venta", "normal")))[:100],
        "cross_selling": limpiar_texto(str(data.get("cross_selling", "")))[:250],
        "upselling": limpiar_texto(str(data.get("upselling", "")))[:250],
        "nivel_prioridad": limpiar_texto(str(data.get("nivel_prioridad", "media")))[:30],
        "tipo_seguimiento": limpiar_texto(str(data.get("tipo_seguimiento", "ninguno")))[:30],
        "requiere_seguimiento": bool(data.get("requiere_seguimiento", False)),
        "sugerir_veltrix": bool(data.get("sugerir_veltrix", False))
    }

    # 2. Conversión segura de numéricos
    try:
        conf = float(data.get("confidence", 0.0))
        res["confidence"] = max(0.0, min(conf, 1.0)) if conf >= 0.60 else 0.0
        if res["confidence"] == 0.0: res["intencion"] = "HUMANO" # Handoff automático
        res["precio_oferta"] = max(0.0, float(data.get("precio_oferta", 0.0)))
        res["lead_score"] = int(max(0, min(int(data.get("lead_score", 0)), 100)))
        res["probabilidad_cierre"] = max(0.0, min(float(data.get("probabilidad_cierre", 0.0)), 1.0))
    except:
        res.update({"confidence": 0.0, "precio_oferta": 0.0, "lead_score": 0, "probabilidad_cierre": 0.0})

    if not res["respuesta"]: res["respuesta"] = "Estoy revisando la mejor opción para ayudarte. 👌"
    
    logger.info(f"🎯 [VALIDADOR IA] Intención={res['intencion']} | Score={res['lead_score']}")
    return res

async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict, perfil_cliente_previo: dict = None, media_dict: dict = None):
    """🧠 CEREBRO CENTRAL DE VENTAS IA AAA - COMPRIMIDO"""
    try:
        # 🛡️ 1. ESCUDO DE SEGURIDAD
        if detectar_prompt_injection(texto_cliente):
            logger.warning("🚨 [SECURITY] Prompt Injection interceptado en Cerebro IA.")
            return {"intencion": "SPAM", "respuesta": "Mensaje bloqueado por políticas de seguridad interna.", "confidence": 1.0, "categoria_preferida": "", "accion_tool": "ninguna", "precio_oferta": 0.0, "lead_score": 0, "probabilidad_cierre": 0.0, "estrategia_venta": "bloqueado"}

        # 🧠 2. CONFIGURACIÓN
        v_id = str(config.get("vendedor_id", "V-001"))
        giro = str(config.get("giro_comercial", "Videojuegos y Consolas"))
        tono = str(config.get("tono_ia", "Persuasivo y experto"))
        negocio = str(config.get("nombre_negocio", "Veltrix Store"))
        meta_venta = float(config.get("objetivo_ventas_diario", 3000))
        meta_veltrix = int(config.get("objetivo_veltrix_diario", 5))
        permitir_desc = bool(config.get("permitir_descuentos_ia", True))
        desc_max = float(config.get("max_descuento_ia", 10))

        # 🔒 3. LOCK COGNITIVO
        lock_id = hashlib.sha256(f"{v_id}:{texto_cliente[:50]}".encode()).hexdigest()
        tracking_locks_uso[lock_id] = now_ts()

        async with locks_por_conversacion[lock_id]:
            logger.info(f"🔮 [CEREBRO IA] Análisis para: {v_id}")
            perfil = perfil_cliente_previo or {}
            perfil_str = json.dumps(perfil, ensure_ascii=False)
            emo = str(perfil.get("emocion_actual", "neutral"))
            temp = str(perfil.get("temperatura", "frio"))
            cat = str(perfil.get("categoria_preferida", ""))
            int_ult = str(perfil.get("ultima_intencion", ""))
            inter_ult = str(perfil.get("ultimo_interes", ""))
            rem_count = int(perfil.get("remarketing_count", 0))
            historial = historial_chat[-3500:]

            # 📊 5. SCORE COMERCIAL
            lead_score = 10
            txt = limpiar_texto(texto_cliente).lower()
            p_compra = ["precio", "cuanto", "disponible", "me interesa", "tienes", "quiero", "comprar", "aceptas", "envio", "entrega", "transferencia", "deposito", "ultimo precio", "todavia lo tienes"]
            p_urgencia = ["hoy", "ahorita", "urge", "ya", "rapido", "inmediato"]
            p_regateo = ["menos", "rebaja", "descuento", "es lo menos", "caro", "muy caro"]

            for p in p_compra: lead_score += 8 if p in txt else 0
            for p in p_urgencia: lead_score += 12 if p in txt else 0
            for p in p_regateo: lead_score += 5 if p in txt else 0
            if temp == "caliente": lead_score += 15
            if rem_count >= 2: lead_score += 10
            lead_score = min(100, lead_score)

            # 🧠 6. ESTRATEGIA
            estrategia = "normal"
            if lead_score >= 70: estrategia = "cierre_agresivo"
            elif lead_score >= 45: estrategia = "persuasion_media"
            elif any(p in txt for p in ["caro", "menos"]): estrategia = "negociacion"
            elif rem_count >= 1: estrategia = "recuperacion"

            # 🚀 7. PROMPT MAESTRO
            prompt_maestro = f"""[SYSTEM] Eres el núcleo cognitivo comercial de Veltrix Engine. GIRO: {giro} | TONO: {tono} | NEGOCIO: {negocio}. META: ${meta_venta} | META SUSC: {meta_veltrix}.
[MEMORIA] {perfil_str} | EMO: {emo} | TEMP: {temp} | INT: {int_ult} | INTERES: {inter_ult} | CAT: {cat}
[SCORE] {lead_score}/100 | ESTRATEGIA: {estrategia}
[RAG] {inventario_contexto}
[HISTORIAL] {historial}
[MENSAJE] "{texto_cliente}"
[DIRECTRICES] Humano, persuasivo, prioriza cerrar venta, descuentos max {desc_max}% (permitido: {permitir_desc}).
[RESPUESTA] JSON: {{"intencion": "...", "respuesta": "...", "emocion_cliente": "...", "temperatura_lead": "...", "producto_detectado": "...", "categoria_preferida": "...", "confidence": 0.95, "accion_tool": "ninguna|aplicar_descuento", "precio_oferta": 0.0, "lead_score": {lead_score}, "probabilidad_cierre": 0.0, "estrategia_venta": "{estrategia}", "requiere_seguimiento": true, "sugerir_veltrix": false, "tipo_seguimiento": "24h", "cross_selling": "", "upselling": "", "nivel_prioridad": "media"}}"""

            prompt_estructurado = [{"role": "user", "parts": [prompt_maestro]}]
            if media_dict and "data" in media_dict:
                prompt_estructurado.append({"mime_type": media_dict.get("mime_type", "audio/ogg"), "data": media_dict["data"]})

            data = await consultar_gemini_json(prompt_estructurado, vendedor_id=v_id)

            # 🛡️ 11. VALIDACIONES
            if data.get("accion_tool") not in ["ninguna", "aplicar_descuento"]: data["accion_tool"] = "ninguna"
            for k in ["precio_oferta", "confidence", "probabilidad_cierre"]: data[k] = float(data.get(k, 0.0))
            data["lead_score"] = int(data.get("lead_score", lead_score))
            data["producto_detectado"] = limpiar_texto(str(data.get("producto_detectado", "")))[:120]
            data["categoria_preferida"] = limpiar_texto(str(data.get("categoria_preferida", "")))[:120]
            data["respuesta"] = limpiar_texto(bleach.clean(str(data.get("respuesta", "")), tags=[], strip=True))[:4000]

            if not data["respuesta"]: data["respuesta"] = "Estoy revisando la mejor opción para ayudarte. 👌"
            
            logger.info(f"🎯 [CEREBRO IA] Intención={data.get('intencion')} | LeadScore={data.get('lead_score')}")
            return data

    except Exception as e:
        logger.exception(f"❌ [CEREBRO ERROR] Falla estructural: {str(e)}")
        return {"intencion": "HUMANO", "respuesta": "Hubo un micro-corte. Un asesor revisará tu mensaje enseguida. ⏳", "emocion_cliente": "neutral", "temperatura_lead": "frio", "producto_detectado": "", "categoria_preferida": "", "confidence": 0.0, "accion_tool": "ninguna", "precio_oferta": 0.0, "lead_score": 0, "probabilidad_cierre": 0.0, "estrategia_venta": "fallback", "requiere_seguimiento": False, "sugerir_veltrix": False, "tipo_seguimiento": "ninguno", "cross_selling": "", "upselling": "", "nivel_prioridad": "media"}

async def obtener_contexto_inventario_rag(vendedor_id: str, texto_cliente: str = "") -> str:
    logger.info(f"🔍 [RAG INVENTARIO] Buscando coincidencias para: '{texto_cliente}' (Tenant: {vendedor_id})")
    try:
        palabras_clave = limpiar_texto(texto_cliente).lower()
        
        # 🚀 FIX SAAS: Extraemos atributos_extra en vez de consola estática
        query = supabase.table('inventario').select('nombre, precio, stock, atributos_extra').eq('vendedor_id', str(vendedor_id)).gt('stock', 0)
        
        if palabras_clave and len(palabras_clave.strip()) >= 3:
            palabras = palabras_clave.split()
            if palabras:
                query = query.ilike('nombre', f"%{palabras[0]}%")
                
        res_inv = await async_db_execute(query.limit(100)) 
        
        if not res_inv.data:
            logger.warning("⚠️ [RAG INVENTARIO] La base de datos no tiene stock o el prefiltro falló.")
            res_inv = await async_db_execute(supabase.table('inventario').select('nombre, precio, stock, atributos_extra').eq('vendedor_id', str(vendedor_id)).gt('stock', 0).limit(50))
            if not res_inv.data: return "Catálogo vacío o agotado en este momento."

        inventario = res_inv.data

        # Función Helper para extraer información vital del JSONB para el RAG
        def _obtener_info_extra(item_db: dict) -> str:
            extras = item_db.get('atributos_extra') or {}
            # Si el negocio es de videojuegos, mostramos la consola. Si es otro, su primer atributo clave.
            info_valiosa = extras.get('consola', extras.get('marca', extras.get('modelo', '')))
            return f" ({info_valiosa})" if info_valiosa else ""

        if not palabras_clave or len(palabras_clave.strip()) < 3:
            logger.info("📋 [RAG INVENTARIO] Mensaje corto. Retornando top 10 general.")
            return "\n".join([f"- {i['nombre']}{_obtener_info_extra(i)} | Precio: ${i['precio']} | Disp: {i['stock']}" for i in inventario[:10]])

        diccionario_opciones = {f"{i['nombre']} {_obtener_info_extra(i)}".strip().lower(): i for i in inventario}
        matches = process.extract(
            palabras_clave, 
            diccionario_opciones.keys(), 
            scorer=fuzz.token_sort_ratio, 
            limit=8
        )
        
        items_filtrados = []
        for match_str, score, _ in matches:
            if score > 20.0: items_filtrados.append(diccionario_opciones[match_str])

        if not items_filtrados:
            logger.warning("⚠️ [RAG INVENTARIO] Ningún producto superó el filtro difuso. Activando Fallback.")
            items_filtrados = inventario[:5]

        lineas = [f"- {i['nombre']}{_obtener_info_extra(i)} | Precio: ${i['precio']} | Disp: {i['stock']}" for i in items_filtrados]
        logger.info(f"✅ [RAG INVENTARIO] Bloque RAG construido con {len(lineas)} opciones relevantes.")
        return "\n".join(lineas)

    except Exception as e:
        logger.error(f"❌ [RAG ERROR] Falló la construcción del contexto de inventario: {str(e)}")
        return "Error técnico al recuperar el catálogo."

async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    logger.info(f"📖 [HISTORIAL CHAT] Solicitando últimas interacciones del Tel: {telefono}")
    try:
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)).order('created_at', desc=True).limit(10)
        res_hist = await async_db_execute(query)
        
        if not res_hist.data: 
            logger.info("🆕 [HISTORIAL CHAT] No hay registros previos. Es el primer mensaje del cliente.")
            return "Primer mensaje del cliente en el sistema."

        mensajes_ordenados = list(reversed(res_hist.data))
        
        historial_texto = "\n".join([f"{m.get('autor')}: {m.get('mensaje')}" for m in mensajes_ordenados])
        MAX_CHARS = 3500
        if len(historial_texto) > MAX_CHARS:
            historial_texto = "... [Trunk] ...\n" + historial_texto[-MAX_CHARS:]
            
        logger.info("✅ [HISTORIAL CHAT] Conversación recuperada e indexada correctamente.")
        return historial_texto

    except Exception as e:
        logger.error(f"❌ [HISTORIAL ERROR] Falló la lectura de logs de chat: {str(e)}")
        return "No se pudo recuperar el historial de chat."

# ==========================================================
# 🛠️ 6. FUNCIONES CORE: SCRAPER, ALERTAS, MEDIA Y COMUNICACIÓN
# ==========================================================
def sanitizar_nombre_columna(columna: str) -> str:
    return bleach.clean(columna, tags=[], attributes={}, strip=True)[:50]

async def actualizar_estado_crm(telefono: str, vendedor_id: str, columna: str, iluminacion: str, juego: str, perfil_ia: dict = None):
    # 🛡️ FIX AAA: Permitimos mover tarjetas a las bandejas reservadas
    # 🚀 FIX SAAS: Actualizamos el nombre de la columna para que empate con Supabase ('ultimo_producto_interes')
    payload = {
        'columna': sanitizar_nombre_columna(columna, permitir_reservadas=True), 
        'estado_iluminacion': sanitizar_nombre_columna(iluminacion, permitir_reservadas=True), 
        'ultimo_producto_interes': bleach.clean(juego, tags=[], strip=True)[:100], 
        'ultima_interaccion_ia': datetime.now(timezone.utc).isoformat()
    }
    if perfil_ia: payload['perfil_psicologico'] = perfil_ia
    await async_db_execute(supabase.table('prospectos').update(payload).eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))

async def guardar_resultado_ia_en_crm(telefono: str, vendedor_id: str, data: dict) -> bool:
    """
    Persiste el análisis cognitivo de la IA en Supabase.
    Actualiza métricas de negocio y estados de prospección.
    """
    try:
        # Mapeo de datos validados a columnas de Supabase
        payload = {
            "lead_score": data.get("lead_score"),
            "probabilidad_cierre": data.get("probabilidad_cierre"),
            "estrategia_venta": data.get("estrategia_venta"),
            "requiere_seguimiento": data.get("requiere_seguimiento"),
            "sugerir_veltrix": data.get("sugerir_veltrix"),
            "tipo_seguimiento": data.get("tipo_seguimiento"),
            "cross_selling": data.get("cross_selling"),
            "upselling": data.get("upselling"),
            "nivel_prioridad": data.get("nivel_prioridad"),
            "ultimo_msj": data.get("respuesta"),
            "ultima_interaccion_ia": now_ts()
        }
        
        # Ejecución contra Supabase
        # Nota: Usamos update.eq para asegurar que solo modificamos al cliente correcto
        response = await async_db_execute(
            supabase.table("prospectos")
            .update(payload)
            .eq("telefono", telefono)
            .eq("vendedor_id", str(vendedor_id))
        )
        
        logger.info(f"💾 [CRM SYNC] Prospecto {telefono} sincronizado: Score={data.get('lead_score')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ [CRM SYNC ERROR] Falló persistencia de IA para {telefono}: {str(e)}")
        return False

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str):
    # 🛡️ FIX AAA: Sanitización XSS Almacenada Crítica
    mensaje_limpio = bleach.clean(limpiar_texto(mensaje), tags=[], strip=True)
    await async_db_execute(supabase.table('mensajes_chat').insert({'telefono': telefono, 'vendedor_id': str(vendedor_id), 'autor': autor, 'mensaje': mensaje_limpio}))

async def disparar_whatsapp_dinamico_async(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    if not http_client: return False
    url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    
    for intento in range(2):
        try: 
            res = await http_client.post(url, headers=headers, json=payload, timeout=10.0)
            if res.status_code in [200, 201]: return True
            if res.status_code == 429: await asyncio.sleep(2); continue
            logger.error(f"❌ [META ERROR] Status {res.status_code}: {res.text}")
            return False
        except asyncio.TimeoutError: pass
        except Exception as e: 
            logger.exception(f"🚨 [WHATSAPP CRÍTICO] {e}")
            break
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

    if not http_client:
        logger.error("❌ [WHATSAPP IMG] HTTP Client no inicializado.")
        return False

    try:

        # 🛡️ Sanitización crítica
        telefono_destino = str(telefono_destino).strip()
        url_imagen = str(url_imagen).strip()
        texto_mensaje = limpiar_texto(texto_mensaje)

        if not telefono_destino or not url_imagen:
            logger.warning("⚠️ [WHATSAPP IMG] Datos incompletos.")
            return False

        # 🛡️ Validación URL
        if not url_imagen.startswith("http"):
            logger.warning("⚠️ [WHATSAPP IMG] URL inválida.")
            return False

        url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

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

                logger.info(f"📡 [WHATSAPP IMG] Intento {intento+1} -> {telefono_destino}")

                response = await http_client.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=15.0
                )

                # ==================================================
                # ✅ SUCCESS
                # ==================================================

                if response.status_code in [200, 201]:
                    logger.info(f"✅ [WHATSAPP IMG] Imagen enviada correctamente.")
                    return True

                # ==================================================
                # 🚨 RATE LIMIT META
                # ==================================================

                if response.status_code == 429:
                    logger.warning("⚠️ [WHATSAPP IMG] Meta Rate Limit.")
                    await asyncio.sleep(2 * (intento + 1))
                    continue

                # ==================================================
                # 🚨 ERROR META
                # ==================================================

                logger.error(
                    f"❌ [WHATSAPP IMG] Error Meta {response.status_code}: {response.text}"
                )

                # 4xx duros no vale retry
                if response.status_code in [400, 401, 403, 404]:
                    return False

            except asyncio.TimeoutError:
                logger.warning(f"⏱️ [WHATSAPP IMG] Timeout intento {intento+1}")

            except Exception as e:
                logger.exception(f"❌ [WHATSAPP IMG ERROR] {e}")

            # 🔥 Backoff progresivo
            await asyncio.sleep(1.5 * (intento + 1))

        return False

    except Exception as e:
        logger.exception(f"❌ [WHATSAPP IMG CRITICAL] {e}")
        return False

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
        logger.info(
            f"📋 [HANDOFF IA] Generando resumen ejecutivo para {cliente}"
        )

        historial_limpio = limpiar_texto(historial_str)

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

        resumen = limpiar_texto(
            str(data.get("resumen", "")).strip()
        )

        if not resumen:
            resumen = "⚠️ Cliente requiere asistencia humana inmediata."

        logger.info("✅ [HANDOFF IA] Resumen ejecutivo generado.")

        return resumen[:1200]

    except Exception as e:
        logger.exception(f"❌ [HANDOFF IA ERROR] {str(e)}")

        return (
            "⚠️ Cliente requiere asistencia humana.\n"
            "No fue posible generar el resumen automático."
        )

async def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config: dict):
    try:
        telefono_admin = config.get("admin_phone") or ADMIN_PHONE_GLOBAL
        token, phone_id = config.get("meta_token", ""), config.get("meta_phone_id", "")
        if intencion == "COMPRA": encabezado = "💰 *NUEVA VENTA DETECTADA*"
        elif intencion == "PEDIDO_ESPECIAL": encabezado = "⚠️ *NUEVO PEDIDO ESPECIAL*"
        elif intencion == "ENOJO": encabezado = "😡 *CLIENTE MOLESTO - URGENTE*"
        else: encabezado = "🚨 *ASISTENCIA REQUERIDA*"
        
        # 🛡️ FIX AAA: Evita exposición masiva truncando el resumen
        resumen_seguro = limpiar_texto(resumen_ia)[:1200]
        mensaje_alerta = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Tel: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_seguro}"
        
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje_alerta, token, phone_id)
        logger.info(f"📩 [ALERTA ADMIN] Enviada para el cliente {cliente}")
    except Exception as e: 
        logger.error(f"❌ [ALERTA ERROR] Falló envío a Admin: {e}")

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nProducto: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera un mensaje corto de remarketing ofreciendo un pequeño descuento. Formato JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))}
    except: return None

# ==========================================================
# 📥 DESCARGADOR MULTIMEDIA WHATSAPP (AAA HARDENED)
# ==========================================================
async def descargar_media_whatsapp_async(media_id: str, token: str) -> Optional[dict]:
    """
    Descarga multimedia desde WhatsApp Cloud API con:
    - Validación MIME estricta
    - Límite duro de tamaño
    - Protección anti-memory abuse
    - Timeout duro
    - Validación binaria
    - Retries controlados
    """

    if not http_client:
        logger.error("❌ [MEDIA] HTTP Client no inicializado.")
        return None

    # ==========================================================
    # 🛡️ CONFIGURACIÓN HARDENED
    # ==========================================================
    MAX_MEDIA_SIZE = 15_000_000  # 15MB
    TIMEOUT_INFO = 10.0
    TIMEOUT_DOWNLOAD = 25.0

    MIME_PERMITIDOS = {
        "image/jpeg",
        "image/png",
        "image/webp",
        "audio/ogg",
        "audio/mp4",
        "audio/mpeg",
        "audio/aac"
    }

    try:
        logger.info(f"📥 [MEDIA] Iniciando descarga segura de Media ID: {media_id}")

        # ==========================================================
        # 🔍 PASO 1: CONSULTA METADATA
        # ==========================================================
        url_info = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"

        headers = {
            "Authorization": f"Bearer {token}"
        }

        try:
            res_info = await asyncio.wait_for(
                http_client.get(url_info, headers=headers),
                timeout=TIMEOUT_INFO
            )
        except asyncio.TimeoutError:
            logger.error("⏱️ [MEDIA] Timeout obteniendo metadata multimedia.")
            return None

        if res_info.status_code != 200:
            logger.warning(f"⚠️ [MEDIA] Metadata inválida: {res_info.status_code}")
            return None

        data_info = res_info.json()

        # ==========================================================
        # 🛡️ VALIDACIÓN MIME
        # ==========================================================
        mime_type = str(data_info.get("mime_type", "")).lower().strip()

        if mime_type not in MIME_PERMITIDOS:
            logger.warning(f"🚨 [MEDIA] MIME bloqueado: {mime_type}")
            return None

        # ==========================================================
        # 🛡️ VALIDACIÓN TAMAÑO
        # ==========================================================
        file_size = int(data_info.get("file_size", 0))

        if file_size <= 0:
            logger.warning("⚠️ [MEDIA] Archivo vacío o inválido.")
            return None

        if file_size > MAX_MEDIA_SIZE:
            logger.warning(
                f"🚨 [MEDIA] Archivo excede límite seguro: "
                f"{file_size / 1024 / 1024:.2f}MB"
            )
            return None

        # ==========================================================
        # 🔍 VALIDACIÓN URL
        # ==========================================================
        media_url = str(data_info.get("url", "")).strip()

        if not media_url.startswith("https://"):
            logger.warning("🚨 [MEDIA] URL multimedia inválida.")
            return None

        logger.info(
            f"📦 [MEDIA] Metadata validada | MIME: {mime_type} | "
            f"Peso: {file_size / 1024:.1f}KB"
        )

        # ==========================================================
        # 📥 DESCARGA BINARIA
        # ==========================================================
        try:
            res_media = await asyncio.wait_for(
                http_client.get(media_url, headers=headers),
                timeout=TIMEOUT_DOWNLOAD
            )
        except asyncio.TimeoutError:
            logger.error("⏱️ [MEDIA] Timeout descargando archivo binario.")
            return None

        if res_media.status_code != 200:
            logger.warning(f"⚠️ [MEDIA] Descarga fallida: {res_media.status_code}")
            return None

        data_bytes = res_media.content

        # ==========================================================
        # 🛡️ VALIDACIÓN BINARIA REAL
        # ==========================================================
        if not data_bytes:
            logger.warning("⚠️ [MEDIA] Payload vacío.")
            return None

        if len(data_bytes) > MAX_MEDIA_SIZE:
            logger.warning("🚨 [MEDIA] Payload final excede límite permitido.")
            return None

        # ==========================================================
        # 🛡️ VALIDACIÓN MAGIC BYTES IMAGEN
        # ==========================================================
        if mime_type.startswith("image/"):
            try:
                img = Image.open(io.BytesIO(data_bytes))
                img.verify()
            except Exception as img_error:
                logger.warning(
                    f"🚨 [MEDIA] Imagen corrupta o manipulada: {img_error}"
                )
                return None

        # ==========================================================
        # 🛡️ VALIDACIÓN MAGIC BYTES AUDIO
        # ==========================================================
        elif mime_type.startswith("audio/"):
            if len(data_bytes) < 128:
                logger.warning("🚨 [MEDIA] Audio sospechosamente pequeño.")
                return None

        logger.info(
            f"✅ [MEDIA] Descarga multimedia completada correctamente "
            f"({len(data_bytes)/1024:.1f}KB)"
        )

        return {
            "mime_type": mime_type,
            "data": data_bytes
        }

    except Exception as e:
        logger.exception(f"❌ [MEDIA CRITICAL ERROR] {str(e)}")
        return None


# ==========================================================
# 🛡️ AUDITOR FINANCIERO IA (DOBERMAN VISION AAA)
# ==========================================================
async def auditar_comprobante_ia(
    b64_img_data: bytes,
    mime_type: str,
    nombre_negocio: str,
    historial_chat: str
):
    """
    Motor antifraude financiero IA:
    - OCR contextual
    - Verificación temporal
    - Verificación de montos
    - Anti screenshots falsas
    - Análisis financiero semántico
    """

    # ==========================================================
    # 🧠 HELPER FLOAT SEGURO
    # ==========================================================
    def safe_float_local(valor):
        try:
            if valor is None:
                return 0.0

            limpio = (
                str(valor)
                .replace("$", "")
                .replace(",", "")
                .replace("MXN", "")
                .replace("mxn", "")
                .strip()
            )

            return round(float(limpio), 2)

        except Exception:
            return 0.0

    try:
        logger.info("🛡️ [DOBERMAN] Iniciando auditoría financiera IA.")

        # ==========================================================
        # 🛡️ VALIDACIÓN BINARIA PREVIA
        # ==========================================================
        if not b64_img_data:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "Imagen vacía o inválida."
            }

        if len(b64_img_data) > 12_000_000:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "El archivo excede el tamaño permitido."
            }

        # ==========================================================
        # 🛡️ VALIDACIÓN IMAGEN REAL
        # ==========================================================
        try:
            img = Image.open(io.BytesIO(b64_img_data))
            img.verify()
        except Exception:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "La imagen parece corrupta o alterada."
            }

        # ==========================================================
        # 🛡️ HISTORIAL CONTROLADO
        # ==========================================================
        historial_chat = limpiar_texto(historial_chat)

        if len(historial_chat) > 2500:
            historial_chat = historial_chat[-2500:]

        fecha_hoy = datetime.now().strftime("%d de %B de %Y")

        # ==========================================================
        # 🧠 PROMPT ANTIFRAUDE
        # ==========================================================
        prompt = f"""
Eres el auditor financiero principal de '{nombre_negocio}'.

Tu trabajo es detectar:
- comprobantes reales
- screenshots falsas
- montos alterados
- comprobantes viejos
- transferencias sospechosas
- imágenes editadas

HISTORIAL CHAT:
{historial_chat}

FECHA ACTUAL:
{fecha_hoy}

REGLAS OBLIGATORIAS:
1. SOLO aceptar comprobantes bancarios o SPEI reales.
2. La fecha debe ser HOY o AYER.
3. Debe existir monto visible.
4. Debe existir evidencia bancaria coherente.
5. Si detectas edición, baja calidad o datos sospechosos → rechazar.
6. Si tienes dudas → rechazar.

RESPONDE ÚNICAMENTE JSON:

{{
  "es_pago": true,
  "monto_detectado": 999.99,
  "analisis": "Transferencia válida detectada."
}}
"""

        # ==========================================================
        # 🤖 CONSULTA IA
        # ==========================================================
        data = await consultar_gemini_json(
            prompt=prompt,
            media_dict={
                "mime_type": mime_type,
                "data": b64_img_data
            },
            temperature=0.0
        )

        resultado = {
            "es_pago": bool(data.get("es_pago", False)),
            "monto_detectado": safe_float_local(
                data.get("monto_detectado", 0)
            ),
            "analisis": limpiar_texto(
                str(data.get("analisis", "Análisis no disponible."))
            )[:500]
        }

        logger.info(
            f"🧾 [DOBERMAN] Resultado auditoría | "
            f"Pago: {resultado['es_pago']} | "
            f"Monto: ${resultado['monto_detectado']}"
        )

        return resultado

    except Exception as e:
        logger.exception(f"❌ [DOBERMAN ERROR] {str(e)}")

        return {
            "es_pago": False,
            "monto_detectado": 0.0,
            "analisis": "Error interno del sistema antifraude."
        }


async def obtener_html_escalonado_async_portadas(url_objetivo: str) -> str:
    if not http_client: return ""
    
    # 🛡️ FIX AAA: Escudo SSRF
    dominio = urllib.parse.urlparse(url_objetivo).netloc
    if "pricecharting.com" not in dominio:
        logger.warning(f"🚨 [SSRF PREVENT] Intento de acceso a dominio no autorizado: {dominio}")
        return ""
        
    estrategias = [
        ("Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("Render", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true")
    ]
    for _, url_scraper in estrategias:
        try:
            res = await http_client.get(url_scraper, timeout=20.0)
            if res.status_code == 200 and "price" in res.text.lower(): return res.text
        except: pass
    try:
        res = await http_client.get(url_objetivo, headers={"User-Agent": "Mozilla/5.0"}, timeout=15.0)
        if res.status_code == 200: return res.text
    except: pass
    return ""

async def cazar_portada_y_guardar_background(juego_id_supabase: str, nombre_juego: str, consola: str):
    RAWG_API_KEY = "7762b63e3ae74e85bfb9a8f2c4f501db"
    url_publica = None
    
    # 1. Intentar limpiar el ID para evitar errores de tipo float/bigint
    try:
        juego_id_int = int(float(juego_id_supabase))
    except:
        juego_id_int = juego_id_supabase # Fallback seguro
    
    # =====================================================================
    # ESTRATEGIA 1: RAWG API (Prioridad Alta)
    # =====================================================================
    try:
        logger.info(f"🌐 [RAWG] Buscando en API oficial: {nombre_juego}")
        url_search = f"https://api.rawg.io/api/games?key={RAWG_API_KEY}&search={urllib.parse.quote(nombre_juego)}&page_size=1"
        res_rawg = await http_client.get(url_search, timeout=10.0)
        
        if res_rawg.status_code == 200:
            datos_json = res_rawg.json()
            if datos_json.get("results") and datos_json["results"][0].get("background_image"):
                imagen_url = datos_json["results"][0]["background_image"]
                url_publica = await procesar_y_subir_imagen(imagen_url, consola, nombre_juego)
    except Exception as e:
        logger.warning(f"⚠️ [RAWG] Falló, intentando Plan B (PriceCharting): {e}")

    # =====================================================================
    # ESTRATEGIA 2: PRICECHARTING (Plan B / Legacy)
    # =====================================================================
    if not url_publica:
        try:
            logger.info(f"🌐 [SCRAPER] Buscando en PriceCharting: {nombre_juego}")
            consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
            query = f"{nombre_juego} {consola_web}".replace(" ", "+")
            url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
            
            html_search = await obtener_html_escalonado_async_portadas(url_search)
            if html_search:
                soup = BeautifulSoup(html_search, 'html.parser')
                img_tag = soup.find('img', class_='product_image')
                if img_tag and img_tag.get('src'):
                    imagen_url = img_tag['src']
                    if not imagen_url.startswith("http"):
                        imagen_url = ("https:" if imagen_url.startswith("//") else "https://www.pricecharting.com") + imagen_url
                    url_publica = await procesar_y_subir_imagen(imagen_url, consola, nombre_juego)
        except Exception as e:
            logger.error(f"⚠️ [SCRAPER] Error crítico Plan B: {e}")

    # =====================================================================
    # MOTOR DE PERSISTENCIA (Si logramos obtener url_publica)
    # =====================================================================
    if url_publica:
        try:
            # Buscamos el catálogo
            res_cat = await async_db_execute(supabase.table('catalogo_maestro').select('id').eq('nombre', nombre_juego).limit(1))
            
            id_catalogo_final = None
            if res_cat.data and len(res_cat.data) > 0:
                id_catalogo_final = res_cat.data[0]['id']
                await async_db_execute(supabase.table('catalogo_maestro').update({'url_portada_oficial': url_publica}).eq('id', id_catalogo_final))
            else:
                res_insert = await async_db_execute(supabase.table('catalogo_maestro').insert({'nombre': nombre_juego, 'consola': consola, 'url_portada_oficial': url_publica}))
                if res_insert.data:
                    id_catalogo_final = res_insert.data[0]['id']
            
            # Actualizamos inventario usando el ID convertido a entero limpio
            update_data = {"url_portada": url_publica}
            if id_catalogo_final: update_data["id_catalogo"] = id_catalogo_final
            
            # 🔥 AQUÍ ESTÁ LA CORRECCIÓN: Usamos juego_id_int que es entero puro
            await async_db_execute(supabase.table('inventario').update(update_data).eq('id', juego_id_int))
            
            logger.info(f"✅ [CORE] Portada vinculada exitosamente a ID: {juego_id_int}")
        except Exception as e:
            logger.error(f"⚠️ Error escribiendo DB: {e}")

# Función auxiliar para no repetir código de procesamiento
async def procesar_y_subir_imagen(url: str, consola: str, nombre: str) -> str:
    res_img = await http_client.get(url, timeout=15.0)
    if res_img.status_code != 200: return None
    from PIL import Image
    import io
    img = Image.open(io.BytesIO(res_img.content)).convert("RGB")
    out_buffer = io.BytesIO()
    img.save(out_buffer, format="JPEG", quality=80, optimize=True)
    img_bytes = out_buffer.getvalue()
    hash_img = hashlib.sha256(img_bytes).hexdigest()[:10]
    nombre_archivo = f"{consola.replace(' ', '_')}_{nombre.replace(' ', '_')}_{hash_img}.jpg"
    try:
        await async_db_execute(supabase.storage.from_("portadas").upload(nombre_archivo, img_bytes, {"content-type": "image/jpeg"}))
    except: pass
    return supabase.storage.from_("portadas").get_public_url(nombre_archivo)

@app.post("/api/solicitar_portada")
async def solicitar_portada(payload: dict, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)):
    # payload espera: {"juego_id": 1493, "nombre": "Bloodborne", "consola": "PS4"}
    juego_id = payload.get("juego_id")
    nombre = payload.get("nombre")
    consola = payload.get("consola")
    
    if not juego_id or not nombre:
        raise HTTPException(status_code=400, detail="Datos incompletos")
        
    # Disparamos la misma función que usa el bot de WhatsApp
    background_tasks.add_task(cazar_portada_y_guardar_background, str(juego_id), nombre, consola)
    
    return {"status": "searching", "message": "Tarea de cacería iniciada"}

# ==========================================================
# ⏰ 7. WATCHDOG B2B Y FLUJO PRINCIPAL IA (AAA ENTERPRISE)
# ==========================================================

webhook_lock = asyncio.Lock() # 🛡️ FIX AAA: Lock para idempotencia real de webhooks

async def limpiador_background_rutinario():
    """🔥 FIX AAA: Garbage Collector Inmortal para limpiar fugas de memoria RAM"""
    print("🧹 [GC] Iniciando Recolector de Basura de Memoria RAM...")
    while True:
        try:
            ahora = now_ts()
            locks_a_borrar = [k for k, v in tracking_locks_uso.items() if ahora - v > 900] # Limpia despues de 15 min inactivos
            for k in locks_a_borrar:
                if k in locks_por_conversacion: del locks_por_conversacion[k]
                del tracking_locks_uso[k]
            if locks_a_borrar:
                logger.info(f"🧹 [GC] Liberados {len(locks_a_borrar)} micro-locks inactivos.")
        except Exception as e:
            logger.exception(f"🚨 [GC ERROR] {e}")
        await asyncio.sleep(600) # Revisa cada 10 minutos

async def bucle_seguimiento_24h():
    print("⏰ [WATCHDOG] Iniciando bucle de Remarketing 24H...")
    while True:
        try:
            hace_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            
            # 🛡️ FIX AAA: Añadimos remarketing_count para evitar spam infinito al mismo usuario
            res = await async_db_execute(
                supabase.table('prospectos')
                .select('*')
                .eq('columna', 'Envios Masivos')
                .lt('ultima_interaccion_ia', hace_24h)
                .lt('remarketing_count', 3) 
                .limit(20)
            )
            
            for p in (res.data or []):
                vendedor_id = p.get('vendedor_id', 'V-001')
                res_conf = await async_db_execute(supabase.table('configuracion_bot').select('*').eq('vendedor_id', str(vendedor_id)).limit(1))
                if not res_conf.data: continue
                
                config = res_conf.data[0]
                try:
                    # 🚀 FIX SAAS: Transición de juego a producto general
                    producto_interes = p.get('ultimo_producto_interes', p.get('ultimo_juego_interes', ''))
                    contexto_inv = await obtener_contexto_inventario_rag(vendedor_id, producto_interes)
                    oferta = await generar_oferta_inteligente(p.get('nombre', 'Cliente'), producto_interes if producto_interes else 'producto', contexto_inv)
                    
                    if oferta and oferta.get("mensaje_oferta"):
                        mensaje = oferta.get("mensaje_oferta")
                        await disparar_whatsapp_dinamico_async(p.get('telefono'), mensaje, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
                        await actualizar_estado_crm(p.get('telefono'), vendedor_id, 'Con Descuento', 'oro', producto_interes)
                        await guardar_mensaje_chat(p.get('telefono'), vendedor_id, 'BOT_REMARKETING', mensaje)
                        
                        # 🛡️ FIX AAA: Incremento atómico del contador de remarketing
                        nuevo_count = int(p.get('remarketing_count', 0)) + 1
                        await async_db_execute(supabase.table('prospectos').update({'remarketing_count': nuevo_count}).eq('id', p.get('id')))
                        
                        print(f"🎯 [REMARKETING] Oferta enviada a {p.get('nombre')}")
                        await asyncio.sleep(5) 
                except Exception as e:
                    if "429" in str(e): await asyncio.sleep(60)
        except Exception as e: 
            logger.error(f"⚠️ [WATCHDOG] Error no fatal: {e}")
            pass
        await asyncio.sleep(600)

async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict, media_dict: dict = None, id_mensaje_meta: str = None):
    """Ruteador Maestro del Flujo de Trabajo IA"""
    try:
        # 🛡️ FIX AAA: Idempotencia absoluta con Webhook Lock para race conditions de Meta
        if id_mensaje_meta:
            async with webhook_lock:
                if id_mensaje_meta in mensajes_procesados_meta:
                    logger.info(f"♻️ [WEBHOOK IGNORED] Mensaje duplicado de Meta ignorado en capa IA.")
                    return
                mensajes_procesados_meta[id_mensaje_meta] = True

        # 🛡️ FIX AAA: Wrapper de Timeout para evitar colgar el worker entero
        async def ejecutar_pipeline_ia():
            print(f"\n🧠 [IA WORKFLOW] ==========================================")
            print(f"🧠 [IA WORKFLOW] PROCESANDO RESPUESTA AUTÓNOMA DEL BOT")
            print(f"🧠 [IA WORKFLOW] Cliente: {cliente} | Tel: {telefono} | Columna: {columna_actual}")
            print(f"==============================================================")
            
            vendedor_id = config.get("vendedor_id", "")
            
            # Asume que verificar_rate_limit limpia las listas por dentro como fijamos en Bloque 4
            if not await verificar_rate_limit(vendedor_id, telefono):
                print("⚠️ [IA WORKFLOW] Denegado: Se ha excedido el límite de peticiones.")
                return
                
            if detectar_prompt_injection(texto_entrante):
                print("🛡️ [IA WORKFLOW] Alerta de seguridad: Intento de Prompt Injection neutralizado.")
                return await disparar_whatsapp_dinamico_async(telefono, "Lo siento, no puedo procesar esa solicitud.", config.get("meta_token", ""), config.get("meta_phone_id", ""))

            print("📖 [IA WORKFLOW] Descargando perfil y memoria persistente desde Supabase...")
            res_perfil = await async_db_execute(supabase.table('prospectos').select('perfil_psicologico').eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))
            perfil_cliente_previo = res_perfil.data[0].get('perfil_psicologico', {}) if res_perfil.data else {}
            
            print("🔍 [IA WORKFLOW] Extrayendo contexto de inventario con algoritmo RAG...")
            contexto = await obtener_contexto_inventario_rag(vendedor_id, texto_entrante)
            
            print("📜 [IA WORKFLOW] Compilando logs de las últimas interacciones de chat...")
            historial = await obtener_historial_chat(telefono, vendedor_id)
            
            print("🧠 [IA WORKFLOW] Transmitiendo parámetros a Gemini para inferencia lógica...")
            # 🛡️ FIX AAA: Aislamiento de Lock por Tenant + Teléfono
            lock_hash = hashlib.sha256(f"{vendedor_id}:{telefono}:{texto_entrante[:50]}".encode()).hexdigest()
            tracking_locks_uso[lock_hash] = now_ts()
            
            decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, perfil_cliente_previo, media_dict)
            
            intencion_ia = str(decision.get("intencion", "CONSULTA")).upper()
            respuesta_bruta = decision.get("respuesta", "En un momento te atiendo.")
            
            # 🛡️ FIX AAA: Sanitización de respuesta antes de inyectar en DB o enviar
            respuesta_final = bleach.clean(respuesta_bruta, tags=[], strip=True)
            respuesta_final = limpiar_texto(respuesta_final)

            # 🚀 FIX SAAS: Actualización de llaves universales en la lógica de procesamiento
            producto_detectado = decision.get("producto_detectado", decision.get("juego_detectado", ""))
            categoria_detectada = decision.get("categoria_preferida", perfil_cliente_previo.get("categoria_preferida", perfil_cliente_previo.get("consola_preferida", "")))
            accion_tool = str(decision.get("accion_tool", "ninguna")).lower()
            precio_oferta = decision.get("precio_oferta", 0.0)
            
            print(f"📊 [IA WORKFLOW] Diagnóstico - Intención: {intencion_ia} | Producto: {producto_detectado} | Categoría: {categoria_detectada}")

            perfil_cliente_actualizado = {
                **perfil_cliente_previo, 
                "emocion_actual": decision.get("emocion_cliente", "neutral"),
                "temperatura": decision.get("temperatura_lead", "frio"),
                "ultimo_interes": producto_detectado,
                "categoria_preferida": categoria_detectada,
                "ultima_intencion": intencion_ia
            }

            if accion_tool == "aplicar_descuento" or intencion_ia == "REGATEO":
                print(f"💰 [TOOL CALLING] Herramienta comercial activada de forma autónoma. Oferta calculada: ${precio_oferta} MXN.")

            nueva_columna, iluminacion = columna_actual, "blanco"

            if intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO"]:
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                print("🚨 [IA WORKFLOW] Tráfico crítico o disconformidad detectada. Disparando handoff ejecutivo a Admin...")
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
                
            elif intencion_ia == "COMPRA":
                nueva_columna, iluminacion = "Por Entregar", "verde_exito"
                print("💰 [IA WORKFLOW] Cierre de venta identificado. Transmitiendo notificación de facturación...")
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
                
            elif intencion_ia in ["COTIZACION", "REGATEO", "SALUDO"] and columna_actual == "Bandeja Nueva": 
                nueva_columna = "Envios Masivos"
                print(f"📈 [IA WORKFLOW] Lead calificado de forma ordinaria. Trasladando tarjeta a: {nueva_columna}")
                
            elif intencion_ia == "PEDIDO_ESPECIAL":
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                print("📦 [IA WORKFLOW] Producto no localizado físicamente. Registrando alerta de pedido especial...")
                await enviar_alerta_whatsapp_admin(cliente, telefono, "PEDIDO_ESPECIAL", f"Busca: {producto_detectado}", config)

            print("💾 [IA WORKFLOW] Sincronizando metadatos de tarjeta y chat log en la nube...")
            # 🚀 FIX SAAS: Actualizamos enviando el producto_detectado en lugar de juego_detectado
            await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, producto_detectado, perfil_ia=perfil_cliente_actualizado)
            await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

            url_imagen = None
            if producto_detectado:
                print(f"🖼️ [IA WORKFLOW] Rastreando inventario para: '{producto_detectado}'")
                
                # 🔥 FIX AAA: Buscamos el juego tenga o no tenga portada
                res_juego = await async_db_execute(
                    supabase.table('inventario')
                    .select('id, url_portada, atributos_extra')
                    .ilike('nombre', f'%{producto_detectado}%')
                    .eq('vendedor_id', str(vendedor_id))
                    .limit(1)
                )
                
                if res_juego.data: 
                    datos_juego = res_juego.data[0]
                    url_imagen = datos_juego.get('url_portada')
                    
                    if url_imagen:
                        print(f"🔗 [IA WORKFLOW] Portada vinculada localizada: {url_imagen}")
                    else:
                        print(f"⚠️ [IA WORKFLOW] Juego sin foto. Disparando cacería en background para: {producto_detectado}")
                        juego_id_inventario = str(datos_juego.get('id'))
                        consola_del_juego = datos_juego.get('atributos_extra', {}).get('consola', categoria_detectada)
                        
                        # 🚀 DISPARO EN BACKGROUND: No bloquea el bot, el cliente recibe su texto al instante
                        import asyncio
                        asyncio.create_task(
                            cazar_portada_y_guardar_background(
                                juego_id_supabase=juego_id_inventario,
                                nombre_juego=producto_detectado,
                                consola=consola_del_juego
                            )
                        )

            if url_imagen: 
                print("📡 [IA WORKFLOW] Despachando paquete de mensajería enriquecida (IMAGEN + TEXTO)...")
                await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, config.get("meta_token", ""), config.get("meta_phone_id", ""))
            else: 
                print("📡 [IA WORKFLOW] Despachando paquete de mensajería plano (TEXTO ÚNICO)...")
                await disparar_whatsapp_dinamico_async(telefono, respuesta_final, config.get("meta_token", ""), config.get("meta_phone_id", ""))

            print(f"✅ [IA WORKFLOW] FLUJO COMPLETADO EXITOSAMENTE PARA EL CANAL: {telefono}")
            print(f"==============================================================\n")

        # 🛡️ Ejecución encapsulada con timeout de 60s
        await asyncio.wait_for(ejecutar_pipeline_ia(), timeout=60.0)

    except asyncio.TimeoutError:
        logger.error(f"⏱️ [IA WORKFLOW] Timeout: El procesamiento del bot tardó más de 60 segundos para {telefono}.")
    except Exception as e: 
        logger.exception(f"❌ [IA WORKFLOW CRITICAL ERROR] Falla estructural en el orquestador del Bot: {str(e)}")

# ==========================================================
# 📈 8. MOTOR DE PRECIOS PRO (CACHE AAA, MATCHING SCORE, PRICING DINÁMICO)
# ==========================================================

# 🚀 1. GESTIÓN DE CACHÉ DE ALTO RENDIMIENTO (LOCK-FREE READS)
cache_precios_ram = TTLCache(maxsize=50000, ttl=86400) # 🛡️ FIX AAA: Evita RAM infinita
cache_lock = asyncio.Lock()
lock_divisa = asyncio.Lock() # 🛡️ FIX AAA: Prevención de Race Condition en Divisas
TIEMPO_VIDA_CACHE_HORAS = 24 
ULTIMA_LIMPIEZA_CACHE = 0.0

# Métricas de Radar añadidas
metricas_radar = {"cache_hits": 0, "cache_miss": 0, "scraper_ok": 0, "scraper_fail": 0}

CACHE_DIVISA = {"valor": 18.0, "expira": 0.0}
CB_PRICECHARTING = {"fallas": 0, "bloqueado_hasta": 0.0}
HTTP_TIMEOUTS = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

# 🧼 2. NORMALIZACIÓN Y LLAVES ÚNICAS
def normalizar_nombre_busqueda(nombre: str) -> str:
    basura = ["edition", "edición", "greatest hits", "platinum", "remastered", "bundle", "loose", "cib", "new", "goty"]
    nombre_limpio = nombre.lower()
    for p in basura:
        nombre_limpio = nombre_limpio.replace(p, "")
    return " ".join(nombre_limpio.split())

def generar_cache_key(nombre: str, consola: str) -> str:
    return f"{normalizar_nombre_busqueda(nombre)}::{consola.lower().strip()}"

async def limpiar_cache_expirado():
    pass # 🛡️ Obsoleto por TTLCache, mantenido por compatibilidad

async def lanzar_gc_si_toca():
    pass # 🛡️ Obsoleto por TTLCache, mantenido por compatibilidad

async def obtener_precio_cache(llave: str) -> dict | None:
    datos = cache_precios_ram.get(llave)
    if datos:
        print(f"⚡ [CACHE HIT] Precio recuperado en O(1).")
        metricas_radar["cache_hits"] += 1
        if "mxn" not in datos["valores"] and "mxn_mercado" in datos["valores"]:
            datos["valores"]["mxn"] = datos["valores"]["mxn_mercado"]
        return datos["valores"]
    metricas_radar["cache_miss"] += 1
    return None

async def guardar_precio_cache(llave: str, valores: dict):
    async with cache_lock:
        cache_precios_ram[llave] = {
            "valores": valores
        }

async def obtener_dolar_hoy_async():
    ahora = time.time()
    async with lock_divisa:
        if ahora < CACHE_DIVISA["expira"]:
            return CACHE_DIVISA["valor"]
            
        try:
            if not http_client: return 18.00
            res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=HTTP_TIMEOUTS)
            if res.status_code == 200:
                val = float(res.json().get("rates", {}).get("MXN", 18.00))
                CACHE_DIVISA["valor"] = val
                CACHE_DIVISA["expira"] = ahora + 43200 
                return val
        except Exception as e:
            print(f"⚠️ [DIVISAS ERROR] {e}")
        return CACHE_DIVISA["valor"]

# 🕸️ 3. MOTOR DE SCRAPING CON BACKOFF Y CIRCUIT BREAKER
async def obtener_html_escalonado_async(url_objetivo: str, es_busqueda: bool = True) -> str:
    if not http_client: return ""
    
    # 🛡️ FIX AAA: Validación de API Key y Dominio (Anti-SSRF)
    if not SCRAPER_API_KEY: 
        logger.error("🚨 [SCRAPER] Falta SCRAPER_API_KEY en el entorno.")
        return ""
    if "pricecharting.com" not in urllib.parse.urlparse(url_objetivo).netloc:
        print("🚨 [SSRF PREVENT] Dominio no autorizado en scraper.")
        return ""
    
    ahora = time.time()
    if ahora < CB_PRICECHARTING.get("bloqueado_hasta", 0.0):
        print("🛑 [CIRCUIT BREAKER] Dominio PriceCharting en enfriamiento.")
        return ""
    
    def es_html_valido(html_text: str) -> bool:
        texto = html_text.lower()
        if any(b in texto for b in ["cloudflare", "just a moment", "security check", "verify you are human"]): return False
        if len(html_text) < 2000: return False
        
        if es_busqueda and "games_table" not in texto and "search-results" not in texto: 
            return False
        if not es_busqueda and "product_name" not in texto and "price" not in texto: 
            return False
        return True

    url_codificada = urllib.parse.quote(url_objetivo, safe='')
    estrategias = [
        ("Directo", url_objetivo),
        ("Proxy Desktop", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&device_type=desktop"),
        ("Render JS Desktop", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&render=true&device_type=desktop"),
        ("Premium", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&premium=true&device_type=desktop")
    ]
    
    for intento, (nombre_fase, url_scraper) in enumerate(estrategias):
        try:
            if intento > 0: await asyncio.sleep(1.5 ** intento) 
            timeout_seguro = httpx.Timeout(connect=10.0, read=35.0, write=15.0, pool=10.0)
            
            target_url = url_objetivo if nombre_fase == "Directo" else url_scraper
            res = await http_client.get(target_url, timeout=timeout_seguro)
            
            if res.status_code == 200 and es_html_valido(res.text): 
                CB_PRICECHARTING["fallas"] = 0
                metricas_radar["scraper_ok"] += 1
                return res.text
        except Exception as e:
            print(f"❌ [SCRAPER] Fallo en {nombre_fase}: {str(e)[:50]}")
            
    CB_PRICECHARTING["fallas"] = CB_PRICECHARTING.get("fallas", 0) + 1
    metricas_radar["scraper_fail"] += 1
    if CB_PRICECHARTING["fallas"] >= 10:
        CB_PRICECHARTING["bloqueado_hasta"] = ahora + 600
        print("🚨 [CIRCUIT BREAKER] Activado. Scraper bloqueado 10m.")
    return ""

def calcular_precio_venta_inteligente_aaa(precio_mercado_mxn: float, costo_compra: float = 0.0, dias_inventario: int = 0, rareza: str = "comun"):
    if precio_mercado_mxn <= 0 and costo_compra <= 0: return 0.0 
    
    if rareza == "comun" and precio_mercado_mxn > 4000:
        precio_mercado_mxn = 1500.0 # Outlier Cap

    precio_base = precio_mercado_mxn if precio_mercado_mxn > 0 else costo_compra * 1.5
    mult_rareza = 1.25 if rareza == "joya" else 1.40 if rareza == "élite" else 1.10 if rareza == "demandado" else 1.0
    mult_rotacion = 0.85 if dias_inventario > 90 else 0.90 if dias_inventario > 60 else 1.05 if dias_inventario < 7 else 1.0
    
    precio_calculado = precio_base * mult_rareza * mult_rotacion
    
    # 🛡️ FIX AAA: Evitar que los juegos basura o muy baratos queden con precios absurdos
    minimo_operativo = max(costo_compra * 1.15, costo_compra + 50)
    return float(round(max(precio_calculado, minimo_operativo) / 10) * 10)

@app.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo", dias_inventario: int = 0, rareza: str = "comun"):
    await lanzar_gc_si_toca() 
    
    # 🛡️ FIX AAA: Protección contra requests con nombres maliciosamente largos
    nombre = limpiar_texto(nombre)[:120]
    
    # 🚀 BYPASS SAAS MULTI-GIRO: Si no hay consola o no aplica, evitamos gastar recursos en PriceCharting
    if not consola or consola.lower() in ["n/a", "general", "ninguna", "otro", ""]:
        print(f"🔄 [BYPASS SAAS] El artículo '{nombre}' no es un videojuego. Omitiendo scraper.")
        return {
            "status": "bypass_saas",
            "api_version": "v3",
            "nombre_corregido": nombre,
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "tipo_cambio": await obtener_dolar_hoy_async(),
            "rareza": rareza,
            "url_pc": "",
            "confidence_score": 100.0,
            "atributos_extra": {}
        }
    
    print(f"\n🏷️ [RADAR ENTERPRISE] Buscando: '{nombre}' ({consola}) | Operador: {vendedor_id}")
    
    llave_cache = generar_cache_key(nombre, consola)
    valores_cacheados = await obtener_precio_cache(llave_cache)
    if valores_cacheados:
        valores_cacheados["status"] = "ok_cached"
        return valores_cacheados

    tipo_cambio = await obtener_dolar_hoy_async()
    slugs_pc = {"PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3", "PS2": "playstation-2", "PS1": "playstation", "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox", "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", "Nintendo DS": "nintendo-ds", "Nintendo 64": "nintendo-64", "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance", "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", "SNES": "super-nintendo", "NES": "nes", "Genesis": "sega-genesis"}
    
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    nombre_normalizado = normalizar_nombre_busqueda(nombre)
    
    query = urllib.parse.quote_plus(nombre_normalizado + ' ' + consola_web)
    url_search = f"https://www.pricecharting.com/search-products?q={query}&type=prices"
    
    html_search = await obtener_html_escalonado_async(url_search, es_busqueda=True)
    if not html_search: 
        print(f"⚠️ [RADAR PRECIOS] Falló la búsqueda HTML. Devolviendo contrato de error estruturado.")
        return {
            "status": "error",
            "api_version": "v3",
            "nombre_corregido": nombre,
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "tipo_cambio": tipo_cambio,
            "rareza": rareza,
            "url_pc": url_search,
            "confidence_score": 0.0,
            "atributos_extra": {}
        }
        
    soup = BeautifulSoup(html_search, 'html.parser')
    
    # 🛡️ FIX AAA: Evitar Attribute Error si el HTML de PriceCharting cambia
    tabla_juegos = soup.find(id="games_table")
    nodos_a_buscar = tabla_juegos.find_all('a', href=True) if tabla_juegos else soup.find_all('a', href=True)
    
    candidatos = []
    slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
    
    for a in nodos_a_buscar:
        href = a['href'].lower()
        if '/game/' in href and not any(b in href for b in ['strategy-guide', 'lot', 'bundle', 'box-only', 'manual-only']):
            score = 0.0
            if f"/{slug_esperado}/" in href: score += 40.0 
            
            score += fuzz.token_sort_ratio(nombre_normalizado, normalizar_nombre_busqueda(a.text)) * 0.6
            if re.search(r'(-japan-|-jp-|-pal-|-eu-|-korea-)', href): score -= 50.0
            
            if score > 35.0:
                url_limpia = a['href'].strip()
                if not url_limpia.startswith("http"): url_limpia = "https://www.pricecharting.com" + url_limpia
                candidatos.append({"url": url_limpia, "score": score})

    # 🛡️ FIX AAA: Validación anti-HTML corrupto / Exceso de links
    if len(candidatos) > 500:
        logger.error("🚨 [RADAR] HTML corrupto o envenenado. Exceso de candidatos.")
        raise Exception("HTML corrupto")

    nombre_oficial_pc, p_loose, p_cib, p_new = nombre, 0.0, 0.0, 0.0
    link_juego = None

    if candidatos:
        mejor_candidato = max(candidatos, key=lambda x: x["score"])
        link_juego = mejor_candidato["url"]
        print(f"🎯 [MATCHING AAA] Score {round(mejor_candidato['score'], 2)}/100 -> {link_juego}")
        
        html_juego = await obtener_html_escalonado_async(link_juego, es_busqueda=False)
        if html_juego: 
            soup_juego = BeautifulSoup(html_juego, 'html.parser')
            h1_tag = soup_juego.find('h1', id='product_name')
            if h1_tag: nombre_oficial_pc = h1_tag.text.strip().replace('\n', ' ')

            def extraer_numero(id_css, clase_css=None):
                try:
                    nodo = soup_juego.find(id=id_css)
                    if not nodo and clase_css:
                        nodo = soup_juego.find(class_=clase_css)
                        
                    if not nodo: return 0.0
                    
                    texto_crudo = nodo.get_text(separator=' ', strip=True).replace(',', '')
                    coincidencias = re.findall(r'\d+\.\d+|\d+', texto_crudo)
                    if coincidencias:
                        return float(coincidencias[0])
                except Exception as e:
                    print(f"⚠️ [EXTRACTOR] Error parseando {id_css}: {e}")
                return 0.0

            p_loose = extraer_numero("used_price", "price_used")
            p_cib = extraer_numero("cib_price", "price_cib")
            p_new = extraer_numero("new_price", "price_new")

            if p_cib == 0.0:
                if p_loose > 0:
                    p_cib = round(p_loose * 1.30, 2)
                    print(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde Loose: ${p_cib} USD")
                elif p_new > 0:
                    p_cib = round(p_new * 0.70, 2)
                    print(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde New: ${p_cib} USD")

    url_final_godot = link_juego if link_juego else url_search

    if p_loose == 0 and p_cib == 0:
        print(f"⚠️ [RADAR PRECIOS] Contingencia 0$ Absoluta para: '{nombre_oficial_pc}'.")
        respuesta_fallida = {
            "status": "warning_cero", 
            "api_version": "v3",
            "nombre_corregido": nombre_oficial_pc, 
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0}, 
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "rareza": rareza,
            "url_pc": url_final_godot,
            "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0,
            "atributos_extra": {}
        }
        await guardar_precio_cache(llave_cache, respuesta_fallida)
        return respuesta_fallida

    mxn_loose_real = round(p_loose * tipo_cambio, 2)
    mxn_cib_real = round(p_cib * tipo_cambio, 2)
    mxn_new_real = round(p_new * tipo_cambio, 2)
    
    respuesta_final = {
        "status": "ok",
        "api_version": "v3",
        "nombre_corregido": nombre_oficial_pc,
        
        "mxn": {
            "loose": mxn_loose_real,
            "cib": mxn_cib_real,
            "new": mxn_new_real
        },
        
        "mxn_mercado": {
            "loose": mxn_loose_real,
            "cib": mxn_cib_real,
            "new": mxn_new_real
        },
        "mxn_venta": {
            "loose": calcular_precio_venta_inteligente_aaa(mxn_loose_real, 0, dias_inventario, rareza), 
            "cib": calcular_precio_venta_inteligente_aaa(mxn_cib_real, 0, dias_inventario, rareza), 
            "new": calcular_precio_venta_inteligente_aaa(mxn_new_real, 0, dias_inventario, rareza)
        },
        "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
        "tipo_cambio": tipo_cambio,
        "rareza": rareza,
        "url_pc": url_final_godot,
        "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0,
        "atributos_extra": {}
    }
    
    await guardar_precio_cache(llave_cache, respuesta_final)
    print(f"✅ [RADAR EXITO] Mercado CIB: ${mxn_cib_real} MXN | URL: {url_final_godot}")
    return respuesta_final

# ==========================================================
# 🔐 9. AUTENTICACIÓN Y LOGIN B2B (MIGRACIÓN COMPLETA Y RATE LIMIT HARDENING)
# ==========================================================

# 🛡️ FIX AAA: Semáforo CPU-Bound para proteger a bcrypt de ataques de denegación de servicio (DDoS)
LOGIN_CONCURRENCY = asyncio.Semaphore(20)

# Hash simulado para igualar el tiempo de respuesta y evitar "Timing Attacks"
DUMMY_HASH = "$2b$12$DummyHashDummyHashDummyHashDummyHashDummyHashDummyHashDu"

@app.post("/api/login")
async def login_b2b(datos: LoginUpdate, request: Request, background_tasks: BackgroundTasks):
    # 🛡️ FIX AAA: Protección contra Spoofing de X-Forwarded-For
    ip_cliente = request.client.host if request.client else "127.0.0.1"
    
    # 🛡️ FIX AAA: Validación de correo para evitar inyección u homolografía
    email_normalizado = datos.email.lower().strip()
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email_normalizado):
        raise HTTPException(status_code=401, detail="Credenciales inválidas.")

    llave_limite = f"{ip_cliente}:{email_normalizado}"
    
    # 🛡️ FIX AAA: Manejo integral del rate limit dentro del lock
    async with rate_limit_login_lock:
        intentos_previos = LOGIN_RATE_LIMIT.get(llave_limite, 0)
        if intentos_previos >= 5:
            logger.warning(f"🚨 [ANTI-BRUTEFORCE] IP bloqueada preventivamente: {llave_limite}")
            raise HTTPException(status_code=429, detail="Demasiados intentos fallidos. Cuenta bloqueada por 5 minutos.")

    logger.info(f"🔑 [LOGIN] Autenticando: {email_normalizado} desde {ip_cliente}")
    
    try:
        # DB Lookup con Timeout de seguridad
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('*').eq('email', email_normalizado).limit(1)),
            timeout=10.0
        )
        
        usuario_existe = bool(res.data)
        usuario = res.data[0] if usuario_existe else {}
        
        # 🛡️ FIX AAA: Prevención de Timing Attack (Respuesta igual de lenta aunque no exista el usuario)
        password_guardada = str(usuario.get('password', DUMMY_HASH))
        
        if not password_guardada.startswith('$2b$'):
            if usuario_existe:
                logger.critical(f"🚨 [RIESGO DE SEGURIDAD] Cuenta con password legacy detectada: {email_normalizado}")
            password_guardada = DUMMY_HASH
            usuario_existe = False 

        # 🛡️ FIX AAA: Límite de concurrencia en tareas intensivas de CPU
        async with LOGIN_CONCURRENCY:
            password_valida = await run_in_threadpool(pwd_context.verify, datos.password, password_guardada)
            
        # 🛡️ FIX AAA: Mitigación de enumeración de usuarios (Mismo mensaje de error siempre)
        if not usuario_existe or not password_valida: 
            async with rate_limit_login_lock:
                LOGIN_RATE_LIMIT[llave_limite] = intentos_previos + 1 
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")
            
        # --- 🛡️ VALIDACIÓN DE ESTADO (ACCOUNT HEALTH) ---
        # Normalizamos a minúsculas y eliminamos espacios para evitar errores de DB
        estado_usuario = str(usuario.get('estado', '')).lower().strip()
        if estado_usuario != 'activo':
            logger.warning(f"🚫 [LOGIN] Ingreso denegado. Usuario '{email_normalizado}' con estado: '{estado_usuario}'")
            raise HTTPException(status_code=401, detail="Cuenta no activa o suspendida.")

        # --- 💳 VALIDACIÓN UNIFICADA DE SUSCRIPCIÓN (BILLING HEALTH) ---
        suscripcion_activa_db = usuario.get('suscripcion_activa', False)
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        fecha_vencida = False
        
        if fecha_pago_str:
            try:
                from datetime import date
                if date.today() > date.fromisoformat(fecha_pago_str):
                    fecha_vencida = True
                    # Actualizamos a False en background para no bloquear el login
                    background_tasks.add_task(
                        async_db_execute,
                        supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id'])
                    )
            except ValueError: 
                logger.error(f"❌ Formato de fecha de pago corrupto: {email_normalizado}")

        if not suscripcion_activa_db or fecha_vencida:
            logger.warning(f"💳 [LOGIN] Ingreso denegado. Usuario '{email_normalizado}' sin suscripción activa.")
            raise HTTPException(status_code=402, detail="Suscripción inactiva. Por favor realiza tu pago.")

        # Finalizamos el rate limit exitoso
        async with rate_limit_login_lock:
            LOGIN_RATE_LIMIT[llave_limite] = max(0, intentos_previos - 1)

        vendedor_id = str(usuario.get('vendedor_id', 'V-001'))
        ahora = datetime.now(timezone.utc)
        
        payload_jwt = {
            "sub": vendedor_id,
            "email": usuario['email'],
            "jti": str(uuid.uuid4()),
            "iss": "veltrix-engine",
            "aud": "veltrix-clients",
            "iat": ahora,
            "nbf": ahora,
            "exp": ahora + timedelta(days=1)
        }
        
        token_jwt = jwt.encode(payload_jwt, JWT_SECRET, algorithm="HS256")
        logger.info(f"✅ [LOGIN EXITOSO] {vendedor_id} autenticado desde {ip_cliente}.")

        return {
            "status": "ok",
            "datos": {
                "vendedor_id": vendedor_id,
                "email": usuario['email'],
                "estado": estado_usuario,
                "pais": usuario.get('pais', 'México'),
                "suscripcion_activa": True,
                "token": token_jwt 
            },
            "access_token": token_jwt,
            "token_type": "bearer",
            "vendedor_id": vendedor_id,
            "nombre": usuario.get('nombre', 'Vendedor'),
            "rol": usuario.get('rol', 'vendedor')
        }
    except HTTPException: raise
    except asyncio.TimeoutError:
        logger.error("⏱️ [LOGIN] Timeout conectando a la base de datos.")
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado. Intenta de nuevo.")
    except Exception as e:
        logger.exception(f"❌ [LOGIN ERROR] {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor de acceso.")

# ==========================================================
# 🌐 10. RUTAS CRM Y MÓVIL (DATA CLEANING & CONTRASTES DE ERROR UNIFORMES)
# ==========================================================

@app.get("/api/cargar_todo")
async def cargar_todo(limit: int = 200, offset: int = 0, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # 🛡️ FIX AAA: Paginación Defensiva (Evita offsets negativos inyectables)
        offset_seguro = max(0, offset)
        limit_seguro = min(limit, 300)
        
        columnas_izq = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
        columnas_der = ["Por Entregar", "Vendidos", "Papelera"]
        
        # 🛡️ FIX AAA: Timeout Global de DB
        res_cols = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', str(_sesion))),
            timeout=10.0
        )
        
        columnas_custom = [sanitizar_nombre_columna(r['nombre_columna']) for r in (res_cols.data or []) if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)]]
        
        res_prospectos = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos')
                .select('id, nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas')
                .eq('vendedor_id', str(_sesion))
                .order('ultima_interaccion_ia', desc=True)
                .range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=12.0
        )
        
        ultimos = {}
        for fila in (res_prospectos.data or []):
            tel_norm = normalizar_telefono(fila.get('telefono', ''))
            key_identificador = tel_norm if tel_norm else fila.get('nombre', 'Desconocido')
            
            # 🛡️ FIX AAA: Evitamos sobrescribir sin checkeo (Lógica original mantenida, RAM optimizada)
            if key_identificador not in ultimos:
                # 🛡️ FIX AAA: Sanitización lazy antes de despachar a Godot/Frontend
                fila["ultimo_msj"] = html.escape(str(fila.get("ultimo_msj") or ""))
                ultimos[key_identificador] = fila
                
        return {"columnas": columnas_izq + columnas_custom + columnas_der, "prospectos": list(ultimos.values())}
    except Exception as e:
        logger.error(f"❌ Error en cargar_todo CRM: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar tarjetas del embudo.")

@app.get("/api/perfil_cliente")
async def obtener_perfil_cliente(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Parámetro telefónico inválido.")
            
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("prospectos").select("id, notas, etiquetas, columna, perfil_psicologico")
                .eq("telefono", tel_norm).eq("vendedor_id", str(vendedor_id)).limit(1)
            ),
            timeout=5.0
        )
        if res.data: return {"status": "ok", "datos": res.data[0]}
        
        # 🛡️ FIX AAA: Anti-Enumeración de usuarios (Retorno estándar genérico)
        return {"status": "ok", "datos": {}}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error en perfil_cliente: {e}")
        raise HTTPException(status_code=500, detail="Fallo interno en consulta de perfil.")

@app.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = await asyncio.wait_for(
            async_db_execute(supabase.table("configuracion").select("nombre_columna").eq("vendedor_id", str(vendedor_id))),
            timeout=5.0
        )
        return {"status": "ok", "columnas": [sanitizar_nombre_columna(item["nombre_columna"]) for item in (res.data or [])]}
    except Exception as e: 
        logger.error(f"❌ Error columnas: {e}")
        raise HTTPException(status_code=500, detail="Error al solicitar columnas configuradas.")

@app.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, limit: int = 50, offset: int = 0, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: return {"status": "ok", "historial": []}
        
        # 🛡️ FIX AAA: Paginación Defensiva
        offset_seguro = max(0, offset)
        limit_seguro = min(limit, 100)
        
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("mensajes_chat")
                .select("mensaje, autor, created_at")
                .eq("vendedor_id", str(vendedor_id))
                .eq("telefono", tel_norm)
                .order("created_at", desc=True) 
                .range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=8.0
        )
        
        historial_formateado = []
        for m in reversed(res.data or []): 
            historial_formateado.append({
                "contenido": bleach.clean(str(m.get("mensaje") or ""), tags=[], strip=True),
                "es_mio": str(m.get("autor", "")).upper() in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"],
                "fecha": str(m.get("created_at", ""))
            })
        return {"status": "ok", "historial": historial_formateado}
    except Exception as e: 
        logger.error(f"❌ Error chat_history: {e}")
        raise HTTPException(status_code=500, detail="Error al recuperar logs de conversación.")

@app.post("/api/mobile/send_message")
async def send_mobile_message(data: MobileMessageRequest, vendedor_id: str = Depends(verificar_sesion_b2b)):
    tel_norm = normalizar_telefono(data.to)
    mensaje_limpio = str(data.msg).strip()
    
    # 🛡️ FIX AAA: Validación de Longitud (Anti-Payload Bombing)
    if not tel_norm or not mensaje_limpio:
        raise HTTPException(status_code=400, detail="Datos incompletos.")
    if len(mensaje_limpio) > 4096:
        raise HTTPException(status_code=413, detail="Mensaje demasiado largo.")
        
    llave_outbound = f"{vendedor_id}:{tel_norm}"
    
    # 🛡️ FIX AAA: Variable Correcta (Soluciona el NameError crítico de la auditoría)
    async with rate_limit_mobile_lock:
        envios_recientes = RATE_LIMIT_MOBILE_OUTBOUND.get(llave_outbound, 0)
        if envios_recientes > 10: 
            logger.warning(f"🚨 [ANTI-SPAM] Limite outbound excedido para {llave_outbound}")
            raise HTTPException(status_code=429, detail="Límite masivo excedido. Espera un momento.")
        RATE_LIMIT_MOBILE_OUTBOUND[llave_outbound] = envios_recientes + 1

    try:
        res_conf = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', str(vendedor_id)).limit(1)),
            timeout=5.0
        )
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración no encontrada.")
        config = res_conf.data[0]
        
        await disparar_whatsapp_dinamico_async(tel_norm, mensaje_limpio, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
        await guardar_mensaje_chat(tel_norm, str(vendedor_id), 'ASESOR', mensaje_limpio)
        await actualizar_estado_crm(tel_norm, str(vendedor_id), "En Seguimiento", "azul", "")
        
        return {"status": "ok", "message": "Enviado"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error retransmisión: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al despachar.")

@app.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        
        # 🛡️ FIX AAA: Evitamos la sobrecarga en RAM consumiendo la DB directamente con un generador (Agregación segura)
        ventas_res = await asyncio.wait_for(
            async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio)),
            timeout=10.0
        )
        total_hoy = sum((float(v.get("monto") or 0.0) for v in (ventas_res.data or [])))

        prospectos_res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("prospectos")
                .select("id, nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas")
                .eq("vendedor_id", str(vendedor_id))
                .order("ultima_interaccion_ia", desc=True)
                .limit(50)
            ),
            timeout=8.0
        )
        
        prospectos_limpios = []
        for p in (prospectos_res.data or []):
            prospectos_limpios.append({
                "id": p.get("id"),
                "nombre": html.escape(p.get("nombre") or "Cliente"),
                "telefono": normalizar_telefono(p.get("telefono", "")),
                "columna": sanitizar_nombre_columna(p.get("columna") or "Bandeja Nueva"),
                "ultima_interaccion_ia": p.get("ultima_interaccion_ia") or "",
                "ultimo_msj": html.escape(p.get("ultimo_msj") or ""), # 🛡️ FIX AAA: Escapado XSS
                "notas": p.get("notas") or "",
                "etiquetas": p.get("etiquetas") or ""
            })
            
        return {"status": "ok", "vendedor": vendedor_id, "ventas_hoy": total_hoy, "prospectos": prospectos_limpios}
    except Exception as e: 
        logger.error(f"❌ Error en mobile_dashboard: {e}")
        raise HTTPException(status_code=500, detail="Error interno al compilar dashboard.")

@app.post("/api/actualizar_estado")
async def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Identificador obligatorio.")
            
        # 🛡️ FIX AAA: Le decimos a la función que SÍ permita nombres reservados para el Drag & Drop
        col_segura = sanitizar_nombre_columna(datos.nueva_columna, permitir_reservadas=True)
        
        resultado = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').update({'columna': col_segura})
                .eq('vendedor_id', str(_sesion))
                .eq('telefono', tel_norm)
            ),
            timeout=8.0
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Registro no encontrado.")
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error actualizando tarjeta: {e}")
        raise HTTPException(status_code=500, detail="Fallo de actualización.")

@app.post("/api/historial_chat")
async def historial_chat(datos: ClienteIdentificador, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Se requiere número válido.")
            
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('mensajes_chat').select('autor, mensaje')
                .eq('vendedor_id', str(_sesion))
                .eq('telefono', tel_norm)
                .order('created_at', desc=False)
                .limit(50)
            ),
            timeout=8.0
        )
        return {"historial": [{"texto": bleach.clean(f.get('mensaje', ''), tags=[], strip=True), "es_mio": f.get('autor', 'USER') != 'USER'} for f in (res.data or [])], "telefono_oficial": tel_norm}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error consultando historial: {e}")
        raise HTTPException(status_code=500, detail="Error en consulta histórica.")

@app.post("/api/mover_prospecto")
async def mover_prospecto(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Identificador obligatorio.")
            
        col_final = sanitizar_nombre_columna(datos.nueva_columna if datos.nueva_columna else datos.columna)
        
        await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update({"columna": col_final}).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion))),
            timeout=8.0
        )
        return {"status": "ok", "mensaje": f"Movido a {col_final}"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error transaccional.")

@app.post("/api/actualizar_notas")
async def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Número obligatorio.")
            
        notas_sanitizadas = html.escape(datos.notas) if datos.notas else ""
        etiquetas_sanitizadas = html.escape(datos.etiquetas) if datos.etiquetas else ""
        nombre_sanitizado = html.escape(datos.nombre) if datos.nombre else "Cliente"
        
        update_data = {"notas": notas_sanitizadas, "etiquetas": etiquetas_sanitizadas, "nombre": nombre_sanitizado}
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update(update_data).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion))),
            timeout=8.0
        )
        
        if res and res.data: return {"status": "ok", "mensaje": "Sincronización completa"}
        raise HTTPException(status_code=404, detail="Tarjeta no localizada.")
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error inyectando notas CRM: {e}")
        raise HTTPException(status_code=500, detail="Error al sincronizar apuntes.")

# ==========================================================
# 📦 BLOQUE 11: INVENTARIO Y GESTIÓN DE COLUMNAS (AAA ENTERPRISE)
# ==========================================================

# 🛡️ FIX AAA: Cachés y Locks para Búsquedas e Idempotencia
cache_busquedas_maestro = TTLCache(maxsize=2000, ttl=30)
ventas_procesadas_idempotencia = TTLCache(maxsize=10000, ttl=86400)
inventario_db_lock = asyncio.Lock()

# 🛡️ 1. SEGURIDAD Y REGLAS DE NEGOCIO
COLUMNAS_SISTEMA_RESERVADAS = {"requiere asistencia", "por entregar", "bandeja nueva", "envios masivos", "null", "undefined", "delete"}

def sanitizar_nombre_columna(nombre: str, permitir_reservadas: bool = False) -> str:
    limpio = limpiar_texto(nombre).strip()
    if not permitir_reservadas and limpio.lower() in COLUMNAS_SISTEMA_RESERVADAS:
        raise HTTPException(400, "Nombre de columna reservado por el sistema. Elige otro.")
    return limpio

@app.post("/api/crear_inventario")
async def crear_inventario(datos: NuevoArticulo, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        nombre_limpio = limpiar_texto(datos.nombre)
        
        # 🔥 FIX AAA: Límite de longitud y protección de variables numéricas extremas
        if len(nombre_limpio) > 120:
            raise HTTPException(400, "Nombre de artículo demasiado largo. Máximo 120 caracteres.")
        if datos.precio < 0 or datos.stock < 0:
            raise HTTPException(400, "Valores de precio o stock inválidos (negativos).")
        if datos.precio > 1000000 or datos.precio_compra > 1000000:
            raise HTTPException(400, "El precio excede el límite de seguridad transaccional.")
        if datos.stock > 100000:
            raise HTTPException(400, "El stock excede el límite máximo permitido por operación.")

        vid_str = str(_sesion)
        
        # 🔥 FIX AAA: Separación semántica Multi-Giro
        categoria_limpia = limpiar_texto(datos.categoria) 
        # Extraemos la consola del JSONB si existe (para retrocompatibilidad con videojuegos)
        consola_limpia = datos.atributos_extra.get("consola", categoria_limpia)
        
        # 🛡️ FIX AAA: Búsqueda de duplicados universal
        res_check = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').select('id, atributos_extra')
                .eq('vendedor_id', vid_str)
                .ilike('nombre', nombre_limpio)
                .limit(10) # Traemos coincidencias de nombre para filtrar en RAM
            ),
            timeout=10.0
        )
        
        if res_check.data:
            for r in res_check.data:
                # Si el artículo tiene exactamente el mismo nombre y las mismas características, es duplicado
                if r.get('atributos_extra', {}).get('consola', '').lower() == consola_limpia.lower():
                    raise HTTPException(400, "Este artículo ya existe en tu inventario con esas características.")

        # 🚀 INSERCIÓN SAAS (Adiós columnas estáticas, hola JSONB)
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').insert({
                    'vendedor_id': vid_str, 
                    'nombre': nombre_limpio, 
                    'categoria': categoria_limpia, 
                    'precio_compra': datos.precio_compra, 
                    'precio': datos.precio, 
                    'stock': datos.stock,
                    'atributos_extra': datos.atributos_extra # Todo el ADN del negocio va aquí
                })
            ),
            timeout=10.0
        )
        
        if res.data:
            juego_id_creado = str(res.data[0]['id'])
            # 🚀 Scraper Inteligente protegido en Background Task
            background_tasks.add_task(cazar_portada_y_guardar_background, juego_id_creado, datos.nombre, consola_limpia)
            
        return {"status": "ok"}
    except HTTPException: raise
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Tiempo de espera agotado al guardar.")
    except Exception as e: 
        logger.error(f"❌ Error DB Crear Inventario: {e}")
        raise HTTPException(status_code=500, detail="Error interno al crear artículo")

@app.get("/api/cargar_inventario")
async def cargar_inventario(offset: int = 0, limit: int = 100, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # 🔥 FIX AAA: Offset validado preventivamente
        offset_seguro = max(0, offset)
        limit_seguro = min(limit, 100) 
        
        # 📥 LECTURA SAAS: Pedimos el JSONB y agregamos id_catalogo a la petición
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario')
                # 👇 SE AGREGÓ id_catalogo AL FINAL DEL SELECT 👇
                .select("id, nombre, categoria, precio, precio_compra, stock, url_portada, estado_general, atributos_extra, id_catalogo")
                .eq('vendedor_id', str(_sesion))
                .order('id', desc=True)
                .range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=15.0
        )
        
        inventario_limpio = []
        for row in (res.data or []):
            extras = row.get("atributos_extra") or {}
            inventario_limpio.append({
                "id": row.get("id"),
                # 👇 SE EMPAQUETA EL ID_CATALOGO PARA ENVIARLO A GODOT 👇
                "id_catalogo": int(row.get("id_catalogo") or 0), 
                "nombre": html.escape(row.get("nombre") or ""),
                "precio": float(row.get("precio") or 0.0),
                "precio_compra": float(row.get("precio_compra") or 0.0),
                "stock": int(row.get("stock") or 0),
                "url_portada": row.get("url_portada") or "",
                "estado_general": row.get("estado_general") or "Bueno",
                "categoria": row.get("categoria") or "General",
                # Plan de contingencia para Godot: Desempaquetamos los atributos comunes al primer nivel visual
                "consola": html.escape(extras.get("consola", "")),
                "rareza": extras.get("rareza", "comun"),
                "atributos_extra": extras
            })
            
        return {"status": "ok", "inventario": inventario_limpio}
    except Exception as e: 
        logger.error(f"❌ Error carga de inventario: {e}")
        raise HTTPException(status_code=500, detail="Error carga de inventario")

@app.post("/api/editar_item_visor")
async def editar_item(item: InventarioItemUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        if not item.id:
            raise HTTPException(400, "ID Requerido. Operación cancelada.")

        # 1. Extraer SOLO lo que Godot mandó explícitamente (Ignora todo lo demás)
        datos_nuevos = item.model_dump(exclude_unset=True, exclude={"id"})

        # 2. Rescatar datos antiguos de la BD (Para la caché y para proteger el JSONB)
        res_old = await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").select("nombre, atributos_extra").eq("id", item.id).eq("vendedor_id", vid_str).limit(1)),
            timeout=5.0
        )
        
        nombre_anterior = res_old.data[0].get("nombre", "") if res_old.data else ""
        atributos_anteriores = res_old.data[0].get("atributos_extra", {}) if res_old.data else {}
        if not isinstance(atributos_anteriores, dict): atributos_anteriores = {}
        consola_anterior = atributos_anteriores.get("consola", "")

        # 3. Construir el Payload de Actualización Dinámico
        payload_update = {}

        # Limpieza de Nombre (si fue enviado)
        if "nombre" in datos_nuevos and datos_nuevos["nombre"]:
            payload_update["nombre"] = limpiar_texto(datos_nuevos["nombre"]) if "limpiar_texto" in globals() else datos_nuevos["nombre"].strip()

        # Determinar Precio Final (Prioriza 'nuevo_precio', luego 'precio')
        if "nuevo_precio" in datos_nuevos and datos_nuevos["nuevo_precio"] is not None:
            payload_update["precio"] = max(0.0, float(datos_nuevos["nuevo_precio"]))
        elif "precio" in datos_nuevos:
            payload_update["precio"] = max(0.0, float(datos_nuevos["precio"]))

        # Determinar Stock Final (Prioriza 'nuevo_stock', luego 'stock')
        if "nuevo_stock" in datos_nuevos and datos_nuevos["nuevo_stock"] is not None:
            payload_update["stock"] = max(0, int(datos_nuevos["nuevo_stock"]))
        elif "stock" in datos_nuevos:
            payload_update["stock"] = max(0, int(datos_nuevos["stock"]))

        # 🚀 FUSIÓN SAAS DEL JSONB: Protegemos la consola y propiedades previas
        atributos_fusionados = atributos_anteriores.copy()
        
        # Si vienen atributos nuevos directos, los combinamos
        if "atributos_extra" in datos_nuevos and isinstance(datos_nuevos["atributos_extra"], dict):
            atributos_fusionados.update(datos_nuevos["atributos_extra"])
            
        # Si Godot mandó "consola" suelta, la inyectamos a los atributos
        if "consola" in datos_nuevos:
            atributos_fusionados["consola"] = datos_nuevos["consola"]
            payload_update["categoria"] = datos_nuevos["consola"]
        payload_update["atributos_extra"] = atributos_fusionados

        if not payload_update:
            return {"status": "ok", "mensaje": "Nada detectado para actualizar."}

        # 🚀 UPDATE SAAS: Inyectamos exclusivamente los campos procesados
        await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario")
                .update(payload_update)
                .eq("id", item.id).eq("vendedor_id", vid_str)
            ),
            timeout=10.0
        )
        
        # 🔥 Invalidación Doble de Caché AAA
        nombre_final = payload_update.get("nombre", nombre_anterior)
        consola_final = payload_update.get("atributos_extra", {}).get("consola", consola_anterior)

        async with cache_lock:
            if nombre_anterior: 
                cache_precios_ram.pop(generar_cache_key(nombre_anterior, consola_anterior), None)
            cache_precios_ram.pop(generar_cache_key(nombre_final, consola_final), None)

        return {"status": "ok"}
    except HTTPException: 
        raise
    except Exception as e: 
        logger.error(f"❌ Error editar item: {e}")
        raise HTTPException(status_code=500, detail="Error editar item")

@app.get("/api/buscar_maestro")
async def buscar_maestro(q: str, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        q_limpio = normalizar_nombre_busqueda(q) if q else ""
        if not q_limpio: return {"status": "ok", "resultados": []}

        # 🛡️ FIX AAA: Caché en memoria para búsquedas repetitivas
        llave_busqueda = f"{_sesion}:{q_limpio}"
        if llave_busqueda in cache_busquedas_maestro:
            return {"status": "ok", "resultados": cache_busquedas_maestro[llave_busqueda], "cached": True}

        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario')
                .select('id, nombre, precio, stock, url_portada, atributos_extra')
                .eq('vendedor_id', str(_sesion))
                .ilike('nombre', f'%{q_limpio}%')
                .limit(25)
            ),
            timeout=10.0
        )
        
        resultados = res.data or []
        # Fallback de seguridad visual para Godot
        for r in resultados:
            r['consola'] = r.get('atributos_extra', {}).get('consola', '')
            
        cache_busquedas_maestro[llave_busqueda] = resultados
        return {"status": "ok", "resultados": resultados}
    except Exception as e: 
        logger.error(f"❌ Error buscador maestro: {e}")
        raise HTTPException(status_code=500, detail="Error en buscador maestro")

@app.post("/api/borrar_item")
async def borrar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        if not item.id: raise HTTPException(400, "ID Requerido. Borrado bloqueado.")
        
        # 🛡️ FIX AAA: Recuperamos metadata antes de borrar para limpiar caché
        res_old = await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").select("nombre, atributos_extra").eq("id", item.id).eq("vendedor_id", str(_sesion)).limit(1)),
            timeout=5.0
        )
        
        await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").delete().eq("id", item.id).eq("vendedor_id", str(_sesion))),
            timeout=10.0
        )
        
        # 🛡️ FIX AAA: Limpieza de RAM
        if res_old.data:
            async with cache_lock:
                cache_precios_ram.pop(generar_cache_key(res_old.data[0].get("nombre", ""), res_old.data[0].get("atributos_extra", {}).get("consola", "")), None)

        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error borrar item: {e}")
        raise HTTPException(status_code=500, detail="Error borrar item")

# 🚀 ENDPOINT ATÓMICO DE VENTAS (Cero Race Conditions / Auditoría UUID)
@app.post("/api/actualizar_stock")
async def actualizar_stock(item: VentaItem, request: Request, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        if not item.id: raise HTTPException(400, "ID requerido para transacción segura.")
        
        # 🛡️ FIX AAA: Llave de Idempotencia por cabecera HTTP (Evita dobles cobros si Meta reintenta)
        idempotency_key = request.headers.get("x-idempotency-key")
        if idempotency_key:
            if idempotency_key in ventas_procesadas_idempotencia:
                logger.info(f"♻️ [VENTA IDEMPOTENTE] Solicitud duplicada evadida: {idempotency_key}")
                return ventas_procesadas_idempotencia[idempotency_key]

        res_inv = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario").select("id, nombre, precio, stock, atributos_extra")
                .eq("id", item.id).eq("vendedor_id", vid_str).limit(1)
            ),
            timeout=10.0
        )
        
        if not res_inv.data: raise HTTPException(status_code=404, detail="Artículo no localizado.")
            
        db_item = res_inv.data[0]
        stock_actual = int(db_item.get("stock", 0))
        precio_venta = float(db_item.get("precio", 0.0))
        nombre_real_db = db_item.get("nombre", item.nombre_producto)

        if item.cantidad_vendida is not None:
            if item.cantidad_vendida > 100: raise HTTPException(400, "Cantidad de venta sospechosa. Límite excedido.")
            cantidad_descontar = max(1, item.cantidad_vendida)
        else:
            nuevo_req = getattr(item, 'nuevo_stock', stock_actual) if getattr(item, 'nuevo_stock', None) is not None else stock_actual
            cantidad_descontar = max(0, stock_actual - nuevo_req)

        if cantidad_descontar <= 0: return {"status": "ok", "msg": "Sin cambios reales en stock"}

        if cantidad_descontar > stock_actual:
            raise HTTPException(status_code=400, detail=f"Stock insuficiente. Solicitado: {cantidad_descontar}, Disponible: {stock_actual}")

        nuevo_stock_seguro = stock_actual - cantidad_descontar
        
        # Optimistic Locking
        res_update = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario").update({"stock": nuevo_stock_seguro})
                .eq("id", item.id).eq("stock", stock_actual) 
            ),
            timeout=10.0
        )
        
        if not res_update.data:
            raise HTTPException(status_code=409, detail="Colisión de concurrencia. Reintente.")
            
        ingreso_total = precio_venta * cantidad_descontar
        transaccion_id = str(uuid.uuid4())
        
        # 🚀 FIX MIGRACIÓN SAAS: Insertamos en la tabla de ventas limpia
        await asyncio.wait_for(
            async_db_execute(
                supabase.table("ventas").insert({
                    "vendedor_id": vid_str,
                    "nombre_producto": nombre_real_db, # <- El nuevo nombre de columna de la migración SQL
                    "monto": ingreso_total,
                    "cantidad": cantidad_descontar,
                    "stock_anterior": stock_actual,
                    "stock_nuevo": nuevo_stock_seguro,
                    "tx_uuid": transaccion_id,
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
            ),
            timeout=10.0
        )
        
        respuesta_exitosa = {"status": "ok", "nuevo_stock": nuevo_stock_seguro, "tx_id": transaccion_id}
        
        # Guardamos la llave de idempotencia en caché si se proporcionó
        if idempotency_key:
            ventas_procesadas_idempotencia[idempotency_key] = respuesta_exitosa
            
        return respuesta_exitosa
    except HTTPException: raise
    except Exception as e:
        logger.error(f"❌ Error Transaccional de Venta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error crítico al procesar la venta.")

# ==========================================================
# 📊 ENDPOINTS DE COLUMNAS (CRM KANBAN)
# ==========================================================

@app.post("/api/crear_columna")
async def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        nombre_seguro = sanitizar_nombre_columna(datos.nombre)
        res_check = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', str(_sesion)).ilike('nombre_columna', nombre_seguro)),
            timeout=5.0
        )
        if res_check.data: raise HTTPException(400, "La columna ya existe.")
        
        await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').insert({'vendedor_id': str(_sesion), 'nombre_columna': nombre_seguro})),
            timeout=5.0
        )
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error crear columna: {e}")
        raise HTTPException(status_code=500, detail="Error crear columna")

@app.post("/api/renombrar_columna")
async def renombrar_columna(datos: RenombrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        nuevo_seguro = sanitizar_nombre_columna(datos.nuevo_nombre)
        viejo_seguro = limpiar_texto(datos.viejo_nombre)
        if nuevo_seguro.lower() == viejo_seguro.lower(): return {"status": "ok"} 
            
        # 🛡️ FIX AAA: Agregamos timeouts para evitar locks infinitos en DB
        await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').update({'nombre_columna': nuevo_seguro}).eq('vendedor_id', vid_str).eq('nombre_columna', viejo_seguro)),
            timeout=10.0
        )
        await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update({'columna': nuevo_seguro}).eq('vendedor_id', vid_str).eq('columna', viejo_seguro)),
            timeout=10.0
        )
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error renombrar columna: {e}")
        raise HTTPException(status_code=500, detail="Error renombrar")

@app.post("/api/reordenar_columnas")
async def reordenar_columnas(datos: ReordenarColumnasAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # 1. BORRAR: Eliminamos el orden viejo de este vendedor
        supabase.table('configuracion').delete().eq('vendedor_id', datos.vendedor_id).execute()

        # 2. PREPARAR: Armamos la lista de diccionarios en el orden exacto de Godot
        filas_a_insertar = []
        for nombre in datos.columnas:
            filas_a_insertar.append({
                "vendedor_id": datos.vendedor_id,
                "nombre_columna": nombre
            })

        # 3. INSERTAR: Guardamos el nuevo orden de golpe
        if filas_a_insertar:
            supabase.table('configuracion').insert(filas_a_insertar).execute()

        return {"status": "ok"}
        
    except Exception as e:
        print(f"Error en backend: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/borrar_columna")
async def borrar_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        col_name = limpiar_texto(datos.nombre)
        
        # 🛡️ FIX AAA CRÍTICO: Escudo contra destrucción de sistema
        if col_name.lower() in COLUMNAS_SISTEMA_RESERVADAS:
            raise HTTPException(400, "Acción denegada: Prohibido eliminar columnas reservadas del sistema.")
            
        await asyncio.wait_for(
            async_db_execute(supabase.table('prospectos').update({"columna": "Bandeja Nueva"}).eq('columna', col_name).eq('vendedor_id', str(_sesion))),
            timeout=10.0
        )
        await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').delete().eq('vendedor_id', str(_sesion)).eq('nombre_columna', col_name)),
            timeout=10.0
        )
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error borrar columna: {e}")
        raise HTTPException(status_code=500, detail="Error borrar columna")

# ==========================================================
# ⚙️ 11.5 CITAS
# ==========================================================
# --- ENDPOINTS DE CITAS ---

@app.post("/api/crear_cita")
async def crear_cita(datos: NuevaCita, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # Calcular fecha fin
        inicio_dt = datetime.fromisoformat(datos.fecha_inicio)
        fin_dt = inicio_dt + timedelta(minutes=datos.duracion_min)
        
        # Insertar en Supabase
        res = supabase.table('citas').insert({
            'vendedor_id': str(_sesion),
            'cliente_nombre': datos.cliente_nombre,
            'cliente_telefono': datos.cliente_telefono,
            'concepto': datos.concepto,
            'fecha_inicio': inicio_dt.isoformat(),
            'fecha_fin': fin_dt.isoformat(),
            'estado': 'pendiente',
            'atributos_extra': datos.atributos_extra
        }).execute()
        
        return {"status": "ok", "cita_id": res.data[0]['id']}
    except Exception as e:
        print(f"❌ Error en crear_cita: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar la cita")

@app.get("/api/cargar_citas")
async def cargar_citas(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('citas')\
            .select('*')\
            .eq('vendedor_id', str(_sesion))\
            .order('fecha_inicio', desc=False)\
            .execute()
        return {"status": "ok", "citas": res.data}
    except Exception as e:
        print(f"❌ Error en cargar_citas: {e}")
        raise HTTPException(status_code=500, detail="Error al listar citas")

@app.post("/api/actualizar_estado_cita")
async def actualizar_estado_cita(datos: EstadoCita, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('citas')\
            .update({'estado': datos.nuevo_estado})\
            .eq('id', datos.cita_id)\
            .eq('vendedor_id', str(_sesion))\
            .execute()
        return {"status": "ok"}
    except Exception as e:
        print(f"❌ Error en actualizar_estado: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar estado")

@app.post("/api/borrar_cita")
async def borrar_cita(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    cita_id = datos.get("cita_id")
    supabase.table('citas').delete().eq('id', cita_id).eq('vendedor_id', str(_sesion)).execute()
    return {"status": "ok"}

# ==========================================================
# ⚙️ 11.6 CREACION DE PUBLICACIONES
# ==========================================================
# --- ENDPOINTS DE PUBLICACIONES ---

@app.post("/api/crear_publicacion")
async def crear_publicacion(datos: NuevaPublicacion, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('publicaciones').insert({
            'vendedor_id': str(_sesion),
            'inventario_id': datos.id_inventario,
            'titulo': datos.titulo,
            'descripcion': datos.descripcion,
            'precio_publicado': datos.precio,
            'estado': 'activa'
        }).execute()
        return {"status": "ok", "pub_id": res.data[0]['id']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/listar_publicaciones")
async def listar_publicaciones(_sesion: str = Depends(verificar_sesion_b2b)):
    print(f"📢 [API] Solicitando publicaciones para el vendedor: {_sesion}")
    try:
        # Traemos solo las publicaciones del vendedor que inició sesión
        res = supabase.table('publicaciones').select('*').eq('vendedor_id', str(_sesion)).order('id', desc=True).execute()
        return {"publicaciones": res.data}
    except Exception as e:
        print(f"❌ [API ERROR] Fallo al listar: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================================
# ⚙️ 11.7 ENVIOS MASIVOS
# ==========================================================

@app.post("/api/mensaje_masivo")
async def ejecutar_campana_masiva(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    vendedor_id = str(_sesion)
    columna = datos.get("columna_origen")
    
    print(f"📢 [API DEBUG] Buscando prospectos en columna: '{columna}' para {vendedor_id}")
    
    try:
        # 1. Obtener prospectos con buscador tolerante (ilike + comodines %)
        # Esto ignora espacios extra y diferencias de mayúsculas/minúsculas.
        res = supabase.table('prospectos') \
            .select('telefono, nombre') \
            .eq('vendedor_id', vendedor_id) \
            .ilike('columna', f'%{columna}%') \
            .execute()
        
        prospectos = res.data
        print(f"🔍 [API DEBUG] Prospectos encontrados: {len(prospectos)}")
        
        if not prospectos:
            return {"status": "error", "message": "No se encontraron prospectos (Verifica el nombre de la columna en la BD)"}

        # 2. Obtener config del bot
        res_conf = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).single().execute()
        config = res_conf.data
        
        # 3. Disparar mensajes
        for p in prospectos:
            print(f"🚀 [API DEBUG] Intentando enviar a: {p.get('nombre', 'Sin Nombre')} - {p['telefono']}")
            
            # Envío de WhatsApp
            resultado = await disparar_whatsapp_dinamico_async(
                p['telefono'], 
                datos.get("mensaje"), 
                config['meta_token'], 
                config['meta_phone_id']
            )
            
            print(f"✅ [API DEBUG] Resultado envío a {p['telefono']}: {resultado}")
            
            # Guardar registro en chat
            await guardar_mensaje_chat(p['telefono'], vendedor_id, 'BOT_MASIVO', datos.get("mensaje"))
            
        return {"status": "ok", "enviados": len(prospectos)}
        
    except Exception as e:
        print(f"❌ [API CRITICAL] Fallo en ejecución: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
        
# ==========================================================
# ⚙️ 12. BACKGROUND WORKER Y WEBHOOKS DE META (AAA ENTERPRISE)
# ==========================================================


# 🛡️ FIX AAA: Tracking global de tareas para evitar Dead Tasks / Zombie Workers
BACKGROUND_TASKS = set()

def lanzar_tarea_segura(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    task.add_done_callback(lambda t: logger.error(f"❌ [TASK ERROR] {t.exception()}") if t.exception() else None)

# 🛡️ CACHÉS Y LOCKS DISTRIBUIDOS EN MEMORIA 
# (Nota para escala Hyperscale: Migrar estos TTLCache a Redis en el futuro)
procesados_recientemente = TTLCache(maxsize=20000, ttl=600)
wamid_lock = asyncio.Lock()

RATE_LIMIT_CLIENTES = TTLCache(maxsize=10000, ttl=10)
rate_limit_lock = asyncio.Lock()

RATE_LIMIT_MEDIA = TTLCache(maxsize=10000, ttl=60)
media_limit_lock = asyncio.Lock()

# 🛡️ SEMÁFOROS DIVIDIDOS Y BACKPRESSURE
SEMAFORO_IA = asyncio.Semaphore(15)      # Máximo 15 conexiones concurrentes a Gemini Chat
SEMAFORO_MEDIA = asyncio.Semaphore(10)   # Máximo 10 procesamientos de imágenes/audios concurrentes
MAX_COLA_GLOBAL = 200                    # Backpressure: Límite antes de rechazar webhooks

# 🛡️ FIX AAA: Función para enmascarar PII (Protección de Datos / GDPR / Leyes locales)
def enmascarar_telefono(tel: str) -> str:
    if len(tel) >= 10: return tel[:4] + "****" + tel[-3:]
    return tel

async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    """
    ==============================================================================
    🧠 ORQUESTADOR MAESTRO MULTIMEDIA VELTRIX ENGINE (AAA HARDENED EDITION)
    ==============================================================================
    ✔ Multi-Tenant Isolation
    ✔ Anti-Duplicados Meta
    ✔ Anti-Spam y Anti-Flood
    ✔ Validación Multimedia Hardened
    ✔ Timeouts críticos por etapa
    ✔ Limpieza de memoria explícita
    ✔ Protección Anti-Decompression Bomb
    ✔ Aislamiento total de errores
    ✔ Telemetría avanzada
    ==============================================================================
    """

    trace_id = str(uuid.uuid4())[:8]

    logger.info(f"📥 [TRACE:{trace_id}] ==================================================")
    logger.info(f"📥 [TRACE:{trace_id}] INICIANDO ORQUESTACIÓN DE MENSAJE")
    logger.info(f"📥 [TRACE:{trace_id}] ==================================================")

    inicio_pipeline = now_ts()

    # ==============================================================================
    # 🧹 VARIABLES EXPLÍCITAS PARA GC
    # ==============================================================================

    media_dict_audio = None
    media_dict_img = None

    try:

        # ==============================================================================
        # 🛡️ 1. ANTI-LOOP / EVENTOS SISTEMA
        # ==============================================================================

        if msg.get("from_me") or valor.get("statuses"):
            logger.info(f"♻️ [TRACE:{trace_id}] Evento de sistema ignorado.")
            return

        # ==============================================================================
        # 🛡️ 2. VALIDACIÓN ID MENSAJE META
        # ==============================================================================

        wamid = str(msg.get("id", "")).strip()

        if not wamid:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Mensaje sin WAMID. Abortando.")
            return

        # ==============================================================================
        # 🛡️ 3. DEDUPLICACIÓN ATÓMICA
        # ==============================================================================

        async with wamid_lock:

            if procesados_recientemente.get(wamid):
                logger.warning(f"♻️ [TRACE:{trace_id}] Webhook duplicado bloqueado: {wamid}")
                return

            procesados_recientemente[wamid] = {
                "ts": now_ts(),
                "trace_id": trace_id
            }

        # ==============================================================================
        # 🛡️ 4. VALIDACIÓN PHONE ID
        # ==============================================================================

        phone_id_receptor = str(phone_id_receptor).strip()

        if not phone_id_receptor:
            logger.error(f"🚨 [TRACE:{trace_id}] Phone ID inválido.")
            return

        # ==============================================================================
        # 🛡️ 5. AISLAMIENTO MULTI-TENANT
        # ==============================================================================

        logger.info(f"🏢 [TRACE:{trace_id}] Resolviendo tenant activo...")

        res_config = await asyncio.wait_for(
            async_db_execute(
                supabase.table('configuracion_bot')
                .select('*')
                .eq('meta_phone_id', phone_id_receptor)
                .limit(1)
            ),
            timeout=5.0
        )

        if not res_config.data:
            logger.error(
                f"🚨 [TRACE:{trace_id}] Tenant no encontrado para Phone ID: {phone_id_receptor}"
            )
            return

        config_vendedor = res_config.data[0]

        vendedor_actual = str(config_vendedor.get("vendedor_id", "")).strip()
        token_actual = str(config_vendedor.get("meta_token", "")).strip() or WHATSAPP_TOKEN
        nombre_negocio = str(config_vendedor.get("nombre_negocio", "Fantasy Games")).strip()

        # ==============================================================================
        # 🛡️ 6. VALIDACIÓN ESTADO BOT
        # ==============================================================================

        if not vendedor_actual:
            logger.error(f"🚨 [TRACE:{trace_id}] vendedor_id vacío.")
            return

        if not token_actual:
            logger.error(f"🚨 [TRACE:{trace_id}] Token Meta vacío.")
            return

        if not config_vendedor.get("bot_activo", True):
            logger.warning(f"🚫 [TRACE:{trace_id}] Bot deshabilitado para {vendedor_actual}.")
            return

        # ==============================================================================
        # 🛡️ 7. NORMALIZACIÓN TELÉFONO
        # ==============================================================================

        telefono_cliente = str(msg.get("from", "")).strip()

        if telefono_cliente.startswith("521"):
            telefono_cliente = "52" + telefono_cliente[3:]

        telefono_cliente = re.sub(r"[^\d]", "", telefono_cliente)

        if not telefono_cliente:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Número telefónico inválido.")
            return

        tel_mask = enmascarar_telefono(telefono_cliente)

        logger.info(f"📞 [TRACE:{trace_id}] Cliente: {tel_mask}")

        # ==============================================================================
        # 🛡️ 8. RATE LIMIT GLOBAL CLIENTE
        # ==============================================================================

        rl_key = f"{vendedor_actual}:{telefono_cliente}"

        async with rate_limit_lock:

            peticiones_recientes = RATE_LIMIT_CLIENTES.get(rl_key, 0)

            if peticiones_recientes > 8:
                logger.warning(
                    f"⚠️ [TRACE:{trace_id}] RATE LIMIT activado para {tel_mask}"
                )
                return

            RATE_LIMIT_CLIENTES[rl_key] = peticiones_recientes + 1

        # ==============================================================================
        # 🧠 9. DETECCIÓN TIPO MENSAJE
        # ==============================================================================

        tipo_mensaje = str(msg.get("type", "text")).lower().strip()
        texto_entrante = ""

        logger.info(
            f"📦 [TRACE:{trace_id}] Tipo mensaje detectado: '{tipo_mensaje}'"
        )

        # ==============================================================================
        # 🛡️ 10. EXTRACCIÓN TEXTO
        # ==============================================================================

        if tipo_mensaje == "text":

            texto_entrante = (
                msg.get("text", {})
                .get("body", "")
                .strip()
            )

        elif tipo_mensaje == "interactive":

            texto_entrante = (
                msg.get("interactive", {})
                .get("button_reply", {})
                .get("title", "")
                .strip()
            )

        # ==============================================================================
        # 🛡️ 11. VALIDACIÓN MULTIMEDIA
        # ==============================================================================

        elif tipo_mensaje in ["image", "audio"]:

            async with media_limit_lock:

                media_count = RATE_LIMIT_MEDIA.get(rl_key, 0)

                if media_count > 5:
                    logger.warning(
                        f"⚠️ [TRACE:{trace_id}] Flood multimedia detectado."
                    )
                    return

                RATE_LIMIT_MEDIA[rl_key] = media_count + 1

            # ==============================================================================
            # 🎙️ AUDIO
            # ==============================================================================

            if tipo_mensaje == "audio":

                texto_entrante = "🎙️ [NOTA DE VOZ RECIBIDA - ANALIZANDO AUDIO...]"

                audio_id = str(
                    msg.get("audio", {}).get("id", "")
                ).strip()

                if not audio_id:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Audio sin ID.")
                    return

                media_dict_audio = await asyncio.wait_for(
                    descargar_media_whatsapp_async(audio_id, token_actual),
                    timeout=30.0
                )

                if not media_dict_audio:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Descarga audio fallida.")
                    return

                audio_bytes = media_dict_audio.get("data", b"")

                if not audio_bytes:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Audio vacío.")
                    return

                if len(audio_bytes) > 15_000_000:
                    logger.warning(
                        f"🚨 [TRACE:{trace_id}] Audio excede límite de 15MB."
                    )
                    return

            # ==============================================================================
            # 🖼️ IMAGEN
            # ==============================================================================

            elif tipo_mensaje == "image":

                texto_entrante = "📷 [IMAGEN RECIBIDA: Analizando comprobante de pago...]"

                image_id = str(
                    msg.get("image", {}).get("id", "")
                ).strip()

                if not image_id:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Imagen sin ID.")
                    return

                media_dict_img = await asyncio.wait_for(
                    descargar_media_whatsapp_async(image_id, token_actual),
                    timeout=30.0
                )

                if not media_dict_img:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Descarga imagen fallida.")
                    return

                data_bytes = media_dict_img.get("data", b"")

                if not data_bytes:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Imagen vacía.")
                    return

                # ==============================================================================
                # 🛡️ LÍMITE DE PESO
                # ==============================================================================

                if len(data_bytes) > 10_000_000:
                    logger.warning(
                        f"🚨 [TRACE:{trace_id}] Imagen excede límite de 10MB."
                    )
                    return

                # ==============================================================================
                # 🛡️ VALIDACIÓN MIME
                # ==============================================================================

                mime = str(
                    media_dict_img.get("mime_type", "")
                ).lower().strip()

                mime_validos = [
                    "image/jpeg",
                    "image/png",
                    "image/webp"
                ]

                if mime not in mime_validos:
                    logger.warning(
                        f"🚨 [TRACE:{trace_id}] MIME inválido: {mime}"
                    )
                    return

                # ==============================================================================
                # 🛡️ VALIDACIÓN REAL IMAGEN
                # ==============================================================================

                try:

                    Image.MAX_IMAGE_PIXELS = 20_000_000

                    img_val = Image.open(io.BytesIO(data_bytes))

                    img_val.verify()

                    logger.info(
                        f"🖼️ [TRACE:{trace_id}] Imagen validada correctamente."
                    )

                except Exception as img_e:

                    logger.warning(
                        f"🚨 [TRACE:{trace_id}] Imagen corrupta/maliciosa: {img_e}"
                    )
                    return

        # ==============================================================================
        # 🛡️ TIPO NO SOPORTADO
        # ==============================================================================

        else:

            logger.info(
                f"ℹ️ [TRACE:{trace_id}] Tipo '{tipo_mensaje}' descartado."
            )
            return

        # ==============================================================================
        # 🛡️ 12. SANITIZACIÓN TEXTO
        # ==============================================================================

        texto_entrante = limpiar_texto(texto_entrante)

        if len(texto_entrante) > 4000:
            texto_entrante = texto_entrante[:4000]

        # ==============================================================================
        # 🛡️ 13. CARGA CRM
        # ==============================================================================

        nombre_cliente = (
            valor.get("contacts", [{}])[0]
            .get("profile", {})
            .get("name", "Cliente")
        )

        nombre_cliente = limpiar_texto(nombre_cliente)[:80]

        res_p = await async_db_execute(
            supabase.table('prospectos')
            .select('columna, notas')
            .eq('telefono', telefono_cliente)
            .eq('vendedor_id', vendedor_actual)
        )

        columna_actual = (
            res_p.data[0].get("columna", "Bandeja Nueva")
            if res_p.data else "Bandeja Nueva"
        )

        # ==============================================================================
        # 🛡️ 14. UPSERT CRM
        # ==============================================================================

        if not res_p.data:

            try:

                logger.info(
                    f"🆕 [TRACE:{trace_id}] Creando prospecto nuevo..."
                )

                await asyncio.wait_for(
                    async_db_execute(
                        supabase.table('prospectos').upsert(
                            {
                                "nombre": nombre_cliente,
                                "telefono": telefono_cliente,
                                "columna": columna_actual,
                                "vendedor_id": vendedor_actual,
                                "ultima_interaccion_ia": datetime.now(
                                    timezone.utc
                                ).isoformat()
                            },
                            on_conflict="telefono,vendedor_id"
                        )
                    ),
                    timeout=5.0
                )

            except Exception as db_e:

                logger.warning(
                    f"⚠️ [TRACE:{trace_id}] Upsert controlado: {db_e}"
                )

        # ==============================================================================
        # 💾 15. GUARDADO CHAT
        # ==============================================================================

        try:

            await asyncio.wait_for(
                guardar_mensaje_chat(
                    telefono_cliente,
                    vendedor_actual,
                    "USER",
                    texto_entrante
                ),
                timeout=5.0
            )

        except Exception as e:

            logger.error(
                f"⚠️ [TRACE:{trace_id}] Error guardando chat: {e}"
            )

        # ==============================================================================
        # 🤖 16. ENRUTAMIENTO IA (NUEVO PIPELINE AAA)
        # ==============================================================================

        if tipo_mensaje in ["text", "interactive", "audio"] \
                and columna_actual != "En Conversacion":

            async with SEMAFORO_IA:
                logger.info(f"🤖 [TRACE:{trace_id}] Iniciando flujo de IA AAA...")

                # 1. Análisis Cognitivo
                data_cruda = await analizar_intencion_venta_ia(
                    texto_entrante, 
                    await obtener_contexto_inventario_rag(vendedor_actual, texto_entrante),
                    await obtener_historial_chat(telefono_cliente, vendedor_actual),
                    config_vendedor,
                    res_p.data[0].get("perfil_psicologico") if res_p.data else None,
                    media_dict_audio
                )

                # 2. Validación y Filtrado
                data_validada = validar_respuesta_ia(data_cruda)

                # 3. Persistencia CRM
                await guardar_resultado_ia_en_crm(
                    telefono_cliente, 
                    vendedor_actual, 
                    data_validada
                )

                # 4. Envío de Respuesta
                await disparar_whatsapp_dinamico_async(
                    telefono_cliente, 
                    data_validada["respuesta"], 
                    token_actual, 
                    phone_id_receptor
                )
                
                # 5. Registro en Log de Chat
                await guardar_mensaje_chat(
                    telefono_cliente,
                    vendedor_actual,
                    "BOT",
                    data_validada["respuesta"]
                )

        # ==============================================================================
        # 🛡️ 17. AUDITORÍA DE PAGOS
        # ==============================================================================

        elif tipo_mensaje == "image" and media_dict_img:

            async with SEMAFORO_MEDIA:

                logger.info(
                    f"🛡️ [TRACE:{trace_id}] Ejecutando DOBERMAN VISION..."
                )

                historial_para_auditor = await obtener_historial_chat(
                    telefono_cliente,
                    vendedor_actual
                )

                try:

                    auditoria = await asyncio.wait_for(
                        auditar_comprobante_ia(
                            media_dict_img["data"],
                            media_dict_img["mime_type"],
                            nombre_negocio,
                            historial_para_auditor
                        ),
                        timeout=45.0
                    )

                except asyncio.TimeoutError:

                    logger.error(
                        f"⏱️ [TRACE:{trace_id}] Timeout Doberman Vision."
                    )
                    return

                es_pago = bool(auditoria.get("es_pago", False))

                try:
                    monto = float(auditoria.get("monto_detectado", 0.0))
                except:
                    monto = 0.0

                # ==============================================================================
                # ✅ PAGO VÁLIDO
                # ==============================================================================

                if es_pago:

                    logger.info(
                        f"💰 [TRACE:{trace_id}] Pago validado: ${monto:.2f}"
                    )

                    await actualizar_estado_crm(
                        telefono_cliente,
                        vendedor_actual,
                        "Por Entregar",
                        "verde_exito",
                        ""
                    )

                    msg_exito = (
                        f"✅ ¡Pago validado por ${monto:.2f} MXN!\n"
                        f"Hemos recibido tu comprobante."
                    )

                    await disparar_whatsapp_dinamico_async(
                        telefono_cliente,
                        msg_exito,
                        token_actual,
                        phone_id_receptor
                    )

                    await guardar_mensaje_chat(
                        telefono_cliente,
                        vendedor_actual,
                        "BOT",
                        msg_exito
                    )

                # ==============================================================================
                # 🚨 FRAUDE / ERROR
                # ==============================================================================

                else:

                    analisis_fallo = limpiar_texto(
                        auditoria.get("analisis", "No se pudo validar.")
                    )

                    logger.warning(
                        f"🚨 [TRACE:{trace_id}] Posible fraude/error: {analisis_fallo}"
                    )

                    msg_fallo = (
                        f"🤖 Mi sistema no pudo validar la imagen.\n"
                        f"Detalle: {analisis_fallo}\n"
                        f"Por favor envía una foto clara."
                    )

                    await actualizar_estado_crm(
                        telefono_cliente,
                        vendedor_actual,
                        "Requiere Asistencia",
                        "verde_alerta",
                        ""
                    )

                    await disparar_whatsapp_dinamico_async(
                        telefono_cliente,
                        msg_fallo,
                        token_actual,
                        phone_id_receptor
                    )

                    await guardar_mensaje_chat(
                        telefono_cliente,
                        vendedor_actual,
                        "BOT",
                        msg_fallo
                    )

        # ==============================================================================
        # ✅ FIN PIPELINE
        # ==============================================================================

        tiempo_total = now_ts() - inicio_pipeline

        logger.info(f"🏁 [TRACE:{trace_id}] =========================================")
        logger.info(f"🏁 [TRACE:{trace_id}] PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info(f"🏁 [TRACE:{trace_id}] Tiempo Total: {tiempo_total:.3f}s")
        logger.info(f"🏁 [TRACE:{trace_id}] =========================================")

    # ==============================================================================
    # ⏱️ TIMEOUT GLOBAL
    # ==============================================================================

    except asyncio.TimeoutError:

        logger.error(
            f"⏱️ [TRACE:{trace_id}] TIMEOUT GLOBAL. Worker liberado."
        )

    # ==============================================================================
    # 🚨 ERROR CRÍTICO
    # ==============================================================================

    except Exception as e:

        logger.exception(
            f"❌ [TRACE:{trace_id}] CRÍTICO: {str(e)}"
        )

    # ==============================================================================
    # 🧹 LIMPIEZA FINAL RAM
    # ==============================================================================

    finally:

        media_dict_audio = None
        media_dict_img = None

        gc.collect()

        logger.info(
            f"🧹 [TRACE:{trace_id}] Garbage Collector ejecutado correctamente."
        )

@app.get("/webhook")
async def verificar_webhook(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == WEBHOOK_SECRET:
        logger.info("✅ [WEBHOOK] Servidor validado con éxito por Meta.")
        return int(params.get("hub.challenge"))
    raise HTTPException(status_code=403, detail="Token de validación de Meta inválido")

@app.post("/webhook")
async def recibir_mensajes(request: Request):
    # 🛡️ FIX AAA: Backpressure (Protección extrema si nos hacen DDoS o llegan demasiados mensajes)
    if len(BACKGROUND_TASKS) > MAX_COLA_GLOBAL:
        logger.critical("🚨 [BACKPRESSURE] Servidor saturado de webhooks. Rechazando para proteger RAM.")
        raise HTTPException(status_code=503, detail="Service Unavailable - Queue Full")

    try:
        await asyncio.wait_for(validar_firma_meta(request), timeout=5.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout validando firma")

    try:
        body_bytes = await request.body()
        if len(body_bytes) > 2_000_000: # 2MB Max Payload
            raise HTTPException(413, "Payload demasiado grande")
            
        try:
            body = json.loads(body_bytes)
        except json.JSONDecodeError:
            raise HTTPException(400, "JSON corrupto o inválido")
            
        # 🛡️ FIX AAA: Iteración completa sobre la estructura de Meta (Garantiza no perder ni un solo mensaje)
        for entry in body.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                phone_id_receptor = value.get("metadata", {}).get("phone_number_id", WHATSAPP_PHONE_ID)
                
                # Meta agrupa mensajes si el usuario manda varios rápidamente
                for message in value.get("messages", []):
                    # Lanzamiento Seguro Asíncrono tracked
                    lanzar_tarea_segura(gestionar_mensaje_entrante_bg(value, message, phone_id_receptor))
        
        # Meta requiere que devuelvas 200 OK inmediatamente
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error en Webhook Entrypoint: {e}")
        return {"status": "error", "reason": str(e)}

app.include_router(router)

# ==============================================================================
#  13 🤖 MÓDULO IA VELTRIX: GENERADOR DE COPY COMERCIAL AAA
# ==============================================================================

@app.post("/api/generar_copy_imagen")
async def api_generar_copy_imagen(datos: PeticionCopy):
    print(f"✨ [IA] Generando copy comercial AAA para: {datos.juego}")
    
    try:
        # Usamos el modelo Flash de Gemini: ultra rápido para no hacer esperar a Godot
        modelo = genai.GenerativeModel('gemini-2.5-flash')
        
        prompt_maestro = f"""
        Eres un experto copywriter de videojuegos físicos. Tu objetivo es vender en Marketplace.
        Genera un texto vendedor ultra-persuasivo para este juego: {datos.juego}
        
        Reglas estrictas de Veltrix Engine:
        1. Devuelve ÚNICAMENTE un objeto JSON válido, sin formato Markdown (NO uses ```json).
        2. El JSON debe tener exactamente dos llaves: "titulo_generado" y "estado_generado".
        3. "titulo_generado": Debe ser un título llamativo, en MAYÚSCULAS, MÁXIMO 4 palabras. Ej: "¡GOD OF WAR REMATADO!"
        4. "estado_generado": Debe incluir emojis y texto comercial corto. Para hacerlo muy realista y local, sugiere entregas en puntos clave como Altaria, San Pancho o punto a convenir. Ej: "🔥 ENTREGA INMEDIATA | ESTADO 10/10 | ENTREGAS EN ALTARIA"
        """
        
        # Ejecutamos la petición asíncrona a la IA
        respuesta = await modelo.generate_content_async(prompt_maestro)
        texto_ia = respuesta.text.strip()
        
        # 🛡️ BLINDAJE AAA: Limpieza por si Gemini se pone terco y devuelve markdown
        if texto_ia.startswith("```json"):
            texto_ia = texto_ia.replace("```json", "").replace("```", "").strip()
        elif texto_ia.startswith("```"):
            texto_ia = texto_ia.replace("```", "").strip()
            
        json_ia = json.loads(texto_ia)
        
        print(f"✅ [IA] Copy generado exitosamente para {datos.juego}")
        return json_ia
        
    except Exception as e:
        print(f"❌ [IA ERROR] Fallo al generar copy: {str(e)}")
        
        # 🛡️ FALLBACK DE SEGURIDAD: Nunca dejamos que Godot reciba un error vacío
        return {
            "titulo_generado": f"¡{datos.juego.upper()}!",
            "estado_generado": "🔥 DISPONIBLE AHORA | EXCELENTE ESTADO | PUNTO A CONVENIR"
        }

if __name__ == "__main__":
    import uvicorn
    # En producción real (Render/AWS), uvicorn se lanza desde la terminal, no desde aquí.
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), reload=False)
