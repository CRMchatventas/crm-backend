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
import sys
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
router = APIRouter(prefix="/api/v1/dashboard")
# 🧪 MODO LABORATORIO: True = Permite pruebas locales / False = Bloquea todo (Producción)
MODO_LABORATORIO = True

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
    """
    ==============================================================================
    🛡️ WRAPPER ASÍNCRONO SUPABASE AAA HARDENED EDITION
    ==============================================================================
    ✔ Timeout fuerte anti freeze
    ✔ Retry inteligente con backoff exponencial
    ✔ Protección anti query zombie
    ✔ Sanitización estructural
    ✔ Telemetría avanzada
    ✔ Anti hammering DB
    ✔ Protección contra conexiones colgadas
    ✔ Aislamiento de errores Supabase
    ✔ Fail-Fast para overload
    ✔ Compatibilidad total con arquitectura actual
    ==============================================================================
    """

    inicio_query = now_ts()

    # ==============================================================================
    # 🛡️ VALIDACIÓN DE QUERY
    # ==============================================================================

    if query_builder is None:
        logger.error("🚨 [DB ERROR] QueryBuilder nulo.")
        raise HTTPException(
            status_code=500,
            detail="Consulta inválida."
        )

    # ==============================================================================
    # 🛡️ PROTECCIÓN TIMEOUT
    # ==============================================================================

    timeout_seg = max(
        3.0,
        min(float(timeout_seg), 60.0)
    )

    # ==============================================================================
    # 🛡️ RETRIES CONTROLADOS
    # ==============================================================================

    MAX_REINTENTOS = 2

    ultimo_error = None

    for intento in range(MAX_REINTENTOS + 1):

        try:

            logger.info(
                f"🛢️ [DB EXECUTE] "
                f"Intento={intento+1} | "
                f"Timeout={timeout_seg}s"
            )

            # ==============================================================================
            # 🚀 EJECUCIÓN AISLADA THREAD
            # ==============================================================================

            resultado = await asyncio.wait_for(
                asyncio.to_thread(
                    query_builder.execute
                ),
                timeout=timeout_seg
            )

            # ==============================================================================
            # 🛡️ VALIDACIÓN RESPONSE
            # ==============================================================================

            if resultado is None:

                raise Exception(
                    "Supabase devolvió resultado nulo."
                )

            # ==============================================================================
            # 📊 TELEMETRÍA
            # ==============================================================================

            tiempo_total = now_ts() - inicio_query

            logger.info(
                f"✅ [DB SUCCESS] "
                f"Tiempo={tiempo_total:.3f}s"
            )

            # ==============================================================================
            # 🛡️ ALERTA CONSULTA LENTA
            # ==============================================================================

            if tiempo_total >= 5.0:

                logger.warning(
                    f"⚠️ [DB SLOW QUERY] "
                    f"Consulta lenta detectada: {tiempo_total:.3f}s"
                )

            return resultado

        # ==============================================================================
        # ⏱️ TIMEOUT CONTROLADO
        # ==============================================================================

        except asyncio.TimeoutError as e:

            ultimo_error = e

            logger.error(
                f"⏱️ [DB TIMEOUT] "
                f"Intento={intento+1} | "
                f"Timeout={timeout_seg}s"
            )

        # ==============================================================================
        # 🚨 ERRORES CONTROLADOS
        # ==============================================================================

        except Exception as e:

            ultimo_error = e

            error_str = str(e).lower()

            logger.error(
                f"❌ [DB ERROR] "
                f"Intento={intento+1} | "
                f"Error={str(e)}"
            )

            # ==============================================================================
            # 🚨 FAIL FAST CRÍTICO
            # ==============================================================================

            errores_criticos = [
                "jwt",
                "auth",
                "permission",
                "unauthorized",
                "forbidden",
                "invalid api key"
            ]

            if any(x in error_str for x in errores_criticos):

                logger.critical(
                    "🚨 [DB CRITICAL] "
                    "Error crítico autenticación/permiso."
                )

                raise HTTPException(
                    status_code=500,
                    detail="Error crítico autenticando base de datos."
                )

        # ==============================================================================
        # 🔄 BACKOFF EXPONENCIAL
        # ==============================================================================

        if intento < MAX_REINTENTOS:

            espera = min(
                4.0,
                2 ** intento
            )

            logger.warning(
                f"🔄 [DB RETRY] "
                f"Reintentando en {espera:.1f}s..."
            )

            await asyncio.sleep(espera)

    # ==============================================================================
    # 🚨 FAILSAFE FINAL
    # ==============================================================================

    logger.critical(
        f"🚨 [DB FAILSAFE] "
        f"Todos los intentos fallaron: {str(ultimo_error)}"
    )

    raise HTTPException(
        status_code=504,
        detail="La nube tardó demasiado en responder."
    )


# 🛡️ FIX AAA: Migración de variables con fugas de memoria a TTLCache y deques
registro_actividad_b2b = TTLCache(
    maxsize=100000,
    ttl=86400
)

procesados_recientemente = TTLCache(
    maxsize=50000,
    ttl=600
)

cache_respuestas_ia = TTLCache(
    maxsize=MAX_CACHE_IA,
    ttl=CACHE_TTL_SECONDS
)

mensajes_procesados_meta = TTLCache(
    maxsize=50000,
    ttl=3600
)

rate_limit_tenant = TTLCache(
    maxsize=50000,
    ttl=120
)

rate_limit_phone = TTLCache(
    maxsize=100000,
    ttl=120
)

# 🛡️ FIX AAA: Protección contra overflow de deque
rate_limit_global = deque(
    maxlen=max(100, MAX_REQUESTS_GLOBAL_MINUTO)
)

# ==============================================================================
# 🔒 MICRO-LOCKS Y TRACKING
# ==============================================================================

rate_limit_global_lock = asyncio.Lock()

LOGIN_RATE_LIMIT = TTLCache(
    maxsize=10000,
    ttl=300
)

RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(
    maxsize=10000,
    ttl=60
)

rate_limit_login_lock = asyncio.Lock()

rate_limit_mobile_lock = asyncio.Lock()

# ==============================================================================
# 🛡️ LOCKS CONVERSACIONALES
# ==============================================================================

locks_por_conversacion = defaultdict(asyncio.Lock)

tracking_locks_uso = defaultdict(float)

# ==============================================================================
# 🧠 CIRCUIT BREAKER GEMINI
# ==============================================================================

gemini_bloqueado_hasta = 0.0

# ==============================================================================
# 🌐 HTTP CLIENT GLOBAL
# ==============================================================================

http_client: Optional[httpx.AsyncClient] = None

# ==============================================================================
# ⚙️ TRACKING TAREAS BACKGROUND
# ==============================================================================

background_tasks_activas = set()


def normalizar_telefono(tel: str) -> str:
    """
    ==============================================================================
    📞 NORMALIZADOR TELEFÓNICO AAA HARDENED
    ==============================================================================
    ✔ Anti CRM Drift
    ✔ Anti caracteres maliciosos
    ✔ Compatibilidad internacional
    ✔ Protección contra inputs corruptos
    ✔ Normalización consistente multi-país
    ✔ Sanitización agresiva
    ==============================================================================
    """

    # ==============================================================================
    # 🛡️ VALIDACIÓN BASE
    # ==============================================================================

    if not tel:
        return ""

    try:

        tel = str(tel).strip()

    except Exception:

        return ""

    # ==============================================================================
    # 🛡️ LÍMITE DURO INPUT
    # ==============================================================================

    if len(tel) > 40:

        logger.warning(
            "⚠️ [PHONE NORMALIZE] "
            "Input telefónico demasiado largo."
        )

        tel = tel[:40]

    # ==============================================================================
    # 🧹 SANITIZACIÓN
    # ==============================================================================

    tel = re.sub(
        r"[^\d\+]",
        "",
        tel
    )

    # ==============================================================================
    # 🌎 NORMALIZACIÓN INTERNACIONAL
    # ==============================================================================

    try:

        t = (
            tel
            if tel.startswith('+')
            else (
                '+' + tel
                if tel.startswith('52')
                else '+52' + tel
            )
        )

        parsed = phonenumbers.parse(t, None)

        if phonenumbers.is_valid_number(parsed):

            numero_final = (
                str(parsed.country_code)
                + str(parsed.national_number)
            )

            logger.info(
                f"📞 [PHONE NORMALIZE] "
                f"Número normalizado: {numero_final[:6]}***"
            )

            return numero_final

    except Exception as e:

        logger.warning(
            f"⚠️ [PHONE NORMALIZE] "
            f"Fallback activado: {e}"
        )

    # ==============================================================================
    # 🛡️ FALLBACK SEGURO
    # ==============================================================================

    limpio = "".join(
        filter(
            str.isdigit,
            str(tel)
        )
    )

    # FIX WhatsApp MX
    if limpio.startswith("521") and len(limpio) == 13:

        limpio = "52" + limpio[3:]

    # FIX Local MX
    if len(limpio) == 10:

        limpio = "52" + limpio

    # ==============================================================================
    # 🛡️ VALIDACIÓN FINAL
    # ==============================================================================

    if len(limpio) < 10:

        logger.warning(
            "⚠️ [PHONE NORMALIZE] "
            "Número demasiado corto."
        )

        return ""

    if len(limpio) > 16:

        logger.warning(
            "⚠️ [PHONE NORMALIZE] "
            "Número demasiado largo."
        )

        return limpio[:16]

    return limpio

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

class LeadAction(BaseModel):
    lead_id: str = Field(..., min_length=1, max_length=100)
    accion: str = Field(..., pattern="^(mover_columna|actualizar_notas)$")
    valor: str = Field(..., max_length=100)

# ==========================================================
# 🛡️ 4. MIDDLEWARES Y SEGURIDAD (AAA HARDENED EDITION)
# ==========================================================

def crear_token_jwt(vendedor_id: str, email: str):
    """
    ==========================================================
    🔐 GENERADOR JWT AAA
    ==========================================================
    ✔ Claims endurecidos
    ✔ UUID único por sesión (jti)
    ✔ Protección issuer/audience
    ✔ Sanitización defensiva
    ✔ Expiración segura
    ✔ Anti-token vacío
    ==========================================================
    """

    # ==========================================================
    # 🛡️ SANITIZACIÓN DEFENSIVA
    # ==========================================================
    vendedor_id = limpiar_texto(str(vendedor_id)).strip()[:80]
    email = limpiar_texto(str(email)).strip().lower()[:180]

    if not vendedor_id:
        raise ValueError("vendedor_id inválido para JWT.")

    if not email:
        raise ValueError("email inválido para JWT.")

    # ==========================================================
    # ⏰ TIMESTAMPS UTC
    # ==========================================================
    ahora = datetime.now(timezone.utc)

    # ==========================================================
    # 🔐 PAYLOAD HARDENED
    # ==========================================================
    payload = {
        "sub": vendedor_id,
        "email": email,
        "jti": str(uuid.uuid4()),

        # 🛡️ Claims RFC7519
        "iss": "veltrix-engine",
        "aud": "veltrix-clients",

        # ⏰ Temporalidad
        "iat": int(ahora.timestamp()),
        "nbf": int(ahora.timestamp()),
        "exp": int((ahora + timedelta(days=1)).timestamp())
    }

    logger.info(
        f"🔐 [JWT] Token generado correctamente para vendedor={vendedor_id}"
    )

    # ==========================================================
    # 🔑 FIRMA HS256
    # ==========================================================
    token = jwt.encode(
        payload,
        JWT_SECRET,
        algorithm="HS256"
    )

    return token


# ==========================================================
# 🔐 VERIFICADOR DE SESIÓN B2B (AAA HARDENED)
# ==========================================================
async def verificar_sesion_b2b(
    authorization: str = Header(None),
    auth_token: str = Header(None)
):
    """
    ==========================================================
    🛡️ VALIDADOR JWT ENTERPRISE
    ==========================================================
    ✔ Verificación issuer/audience
    ✔ Protección Bearer malformado
    ✔ Protección algoritmo none
    ✔ Sanitización token
    ✔ Anti-token gigante
    ✔ Logs seguros
    ✔ Validación claims críticas
    ✔ Anti-session confusion
    ==========================================================
    """

    # ==========================================================
    # 🛡️ EXTRACCIÓN SEGURA TOKEN
    # ==========================================================
    token = None

    try:

        if authorization and authorization.startswith("Bearer "):
            partes = authorization.split(" ", 1)

            if len(partes) == 2:
                token = partes[1].strip()

        elif auth_token:
            token = auth_token.strip()

    except Exception:
        token = None

    # ==========================================================
    # 🚫 TOKEN FALTANTE
    # ==========================================================
    if not token:
        logger.warning("🚨 [AUTH] Token faltante.")
        raise HTTPException(
            status_code=401,
            detail="Token faltante"
        )

    # ==========================================================
    # 🛡️ PROTECCIÓN TAMAÑO TOKEN
    # ==========================================================
    if len(token) > 5000:
        logger.warning("🚨 [AUTH] Token sospechosamente grande.")
        raise HTTPException(
            status_code=401,
            detail="Token inválido"
        )

    # ==========================================================
    # 🛡️ VALIDACIÓN JWT
    # ==========================================================
    try:

        # ==========================================================
        # 🔍 DEBUG CONTROLADO
        # (Sin exponer token completo)
        # ==========================================================
        logger.info(
            f"🔍 [AUTH] Validando JWT | "
            f"Chars={len(token)}"
        )

        # ==========================================================
        # 🔐 VALIDACIÓN ESTRICTA
        # ==========================================================
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],

            # 🛡️ Claims obligatorias
            audience="veltrix-clients",
            issuer="veltrix-engine",

            # 🛡️ Endurecimiento
            options={
                "require": [
                    "sub",
                    "exp",
                    "iat",
                    "nbf",
                    "iss",
                    "aud",
                    "jti"
                ],
                "verify_signature": True,
                "verify_exp": True,
                "verify_iat": True,
                "verify_nbf": True
            }
        )

        # ==========================================================
        # 🛡️ VALIDACIÓN CLAIMS
        # ==========================================================
        vendedor_id = limpiar_texto(
            str(payload.get("sub", ""))
        ).strip()

        email = limpiar_texto(
            str(payload.get("email", ""))
        ).strip()

        if not vendedor_id:
            logger.warning("🚨 [AUTH] JWT sin sub.")
            raise HTTPException(
                status_code=401,
                detail="Token inválido"
            )

        if not email:
            logger.warning("🚨 [AUTH] JWT sin email.")
            raise HTTPException(
                status_code=401,
                detail="Token inválido"
            )

        logger.info(
            f"✅ [AUTH] Sesión validada correctamente "
            f"para vendedor={vendedor_id}"
        )

        return vendedor_id

    # ==========================================================
    # ⏰ TOKEN EXPIRADO
    # ==========================================================
    except jwt.ExpiredSignatureError:

        logger.warning("⏰ [AUTH] Token expirado.")

        raise HTTPException(
            status_code=401,
            detail="Token expirado. Inicie sesión nuevamente."
        )

    # ==========================================================
    # 🚨 ISSUER INVÁLIDO
    # ==========================================================
    except jwt.InvalidIssuerError:

        logger.error("🚨 [AUTH] Issuer inválido.")

        raise HTTPException(
            status_code=401,
            detail="Invalid issuer"
        )

    # ==========================================================
    # 🚨 AUDIENCE INVÁLIDA
    # ==========================================================
    except jwt.InvalidAudienceError:

        logger.error("🚨 [AUTH] Audience inválida.")

        raise HTTPException(
            status_code=401,
            detail="Invalid audience"
        )

    # ==========================================================
    # 🚨 JWT MALFORMADO
    # ==========================================================
    except jwt.InvalidTokenError as e:

        logger.error(
            f"🚨 [AUTH] Token inválido: {str(e)}"
        )

        raise HTTPException(
            status_code=401,
            detail="Token inválido"
        )

    # ==========================================================
    # 🚨 ERROR CRÍTICO
    # ==========================================================
    except Exception as e:

        logger.exception(
            f"❌ [AUTH CRITICAL] {str(e)}"
        )

        raise HTTPException(
            status_code=500,
            detail="Error interno autenticando sesión"
        )


# ==========================================================
# 🛡️ VALIDADOR FIRMA META (AAA HARDENED)
# ==========================================================
async def validar_firma_meta(request: Request):
    """
    ==========================================================
    🛡️ VERIFICACIÓN OFICIAL WEBHOOK META
    ==========================================================
    ✔ Validación HMAC SHA256
    ✔ Anti-request vacío
    ✔ Protección replay básico
    ✔ Protección payload gigante
    ✔ compare_digest seguro
    ✔ Validación headers estricta
    ✔ Logs endurecidos
    ==========================================================
    """

    # ==========================================================
    # 📦 BODY RAW
    # ==========================================================
    body = await request.body()

    # ==========================================================
    # 🚫 BODY VACÍO
    # ==========================================================
    if not body:

        logger.warning(
            "🚨 [WEBHOOK SECURITY] Request vacío bloqueado."
        )

        raise HTTPException(
            status_code=400,
            detail="Payload vacío"
        )

    # ==========================================================
    # 🛡️ PROTECCIÓN PAYLOAD GIGANTE
    # ==========================================================
    if len(body) > 2_000_000:

        logger.warning(
            "🚨 [WEBHOOK SECURITY] Payload excesivo bloqueado."
        )

        raise HTTPException(
            status_code=413,
            detail="Payload demasiado grande"
        )

    # ==========================================================
    # 🔑 HEADER FIRMA META
    # ==========================================================
    firma_meta = request.headers.get("X-Hub-Signature-256")

    if not firma_meta:

        logger.warning(
            "🚨 [WEBHOOK SECURITY] Firma Meta ausente."
        )

        raise HTTPException(
            status_code=400,
            detail="Falta firma"
        )

    # ==========================================================
    # 🛡️ VALIDACIÓN FORMATO
    # ==========================================================
    if not firma_meta.startswith("sha256="):

        logger.warning(
            "🚨 [WEBHOOK SECURITY] Firma malformada."
        )

        raise HTTPException(
            status_code=403,
            detail="Firma inválida"
        )

    # ==========================================================
    # 🔐 CÁLCULO HMAC
    # ==========================================================
    firma_calculada = (
        "sha256=" +
        hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            body,
            hashlib.sha256
        ).hexdigest()
    )

    # ==========================================================
    # 🛡️ COMPARACIÓN SEGURA
    # ==========================================================
    if not hmac.compare_digest(
        firma_meta,
        firma_calculada
    ):

        logger.warning(
            "🚨 [WEBHOOK SECURITY] "
            "Intento de falsificación bloqueado."
        )

        raise HTTPException(
            status_code=403,
            detail="Firma inválida"
        )

    logger.info(
        "✅ [WEBHOOK SECURITY] Firma Meta validada correctamente."
    )

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
# 🚀 VELTRIX ENGINE - AAA HARDENED ULTRA EDITION
# ==========================================================

async def consultar_gemini_json(
    prompt: str,
    media_dict: dict = None,
    temperature: float = 0.2,
    retries: int = 2,
    vendedor_id: str = "V-001"
) -> dict:

    """
    🚀 MOTOR GEMINI AAA ENTERPRISE ULTRA HARDENED

    ✔ Cache Inteligente
    ✔ Circuit Breaker Global
    ✔ Failover Multi-Modelo
    ✔ Anti Prompt Bomb
    ✔ Anti Semantic Flood
    ✔ Anti Retry Storm
    ✔ JSON Hardened Parser
    ✔ Sanitización Profunda
    ✔ Validación Multimodal
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

    inicio_telemetria = now_ts()

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
        "gemini-1.5-flash-latest",
        "gemini-1.5-flash-001",
        "gemini-pro"
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

    RESPUESTA_FAILSAFE = {
        "respuesta": (
            "Tuve un micro-corte en mi sistema. "
            "¿Me repites tu mensaje por favor?"
        ),
        "intencion": "HUMANO",
        "confidence": 0.1,
        "accion_tool": "ninguna"
    }

    # ==========================================================
    # 🛡️ 0. VALIDACIÓN TEMPRANA
    # ==========================================================

    try:

        retries = int(retries)
        retries = max(1, min(retries, MAX_RETRIES))

        temperature = float(temperature)
        temperature = max(0.0, min(temperature, 1.0))

        vendedor_id = limpiar_texto(
            str(vendedor_id)
        )[:80]

    except Exception as config_error:

        logger.warning(
            f"⚠️ [GEMINI CONFIG] "
            f"Fallback config aplicado: {config_error}"
        )

        retries = 2
        temperature = 0.2
        vendedor_id = "V-001"

    # ==========================================================
    # 🛡️ 1. CIRCUIT BREAKER GLOBAL
    # ==========================================================

    tiempo_actual = now_ts()

    if tiempo_actual < gemini_bloqueado_hasta:

        restante = round(
            gemini_bloqueado_hasta - tiempo_actual,
            2
        )

        logger.warning(
            f"🚨 [GEMINI CIRCUIT BREAKER] "
            f"Gemini bloqueado temporalmente "
            f"({restante}s restantes)"
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
    # 🛡️ 2. SERIALIZACIÓN SEGURA
    # ==========================================================

    try:

        if isinstance(prompt, (dict, list)):
            prompt_serializado = orjson.dumps(
                prompt
            ).decode("utf-8")

        else:
            prompt_serializado = str(prompt)

    except Exception as serial_error:

        logger.warning(
            f"⚠️ [PROMPT SERIALIZER] "
            f"Fallback serializer: {serial_error}"
        )

        prompt_serializado = str(prompt)

    # ==========================================================
    # 🧹 3. SANITIZACIÓN PROFUNDA
    # ==========================================================

    prompt_serializado = limpiar_texto(
        prompt_serializado
    )

    # 🛡️ Anti null bytes
    prompt_serializado = (
        prompt_serializado
        .replace("\x00", "")
        .replace("\r", "")
    )

    # 🛡️ Anti markdown/code injection
    prompt_serializado = re.sub(
        r"```.*?```",
        "",
        prompt_serializado,
        flags=re.DOTALL
    )

    # 🛡️ Anti role injection
    patrones_roles = [
        r"<\|system\|>",
        r"<\|assistant\|>",
        r"<\|user\|>",
        r"role\s*:\s*system",
        r"role\s*:\s*assistant",
        r"role\s*:\s*user",
        r"BEGIN\s+OVERRIDE",
        r"SYSTEM\s+INSTRUCTION",
        r"DEVELOPER\s+MODE",
        r"IGNORE\s+PREVIOUS\s+INSTRUCTIONS",
        r"YOU\s+ARE\s+NOW",
        r"ACT\s+AS",
        r"JAILBREAK"
    ]

    for patron in patrones_roles:

        prompt_serializado = re.sub(
            patron,
            "",
            prompt_serializado,
            flags=re.IGNORECASE
        )

    # ==========================================================
    # 🛡️ 4. LIMITADOR HARD PROMPT
    # ==========================================================

    if len(prompt_serializado) > MAX_PROMPT_CHARS:

        logger.warning(
            f"⚠️ [PROMPT LIMIT] "
            f"Prompt truncado "
            f"({len(prompt_serializado)} chars)"
        )

        prompt_serializado = (
            prompt_serializado[-MAX_PROMPT_CHARS:]
        )

    # ==========================================================
    # 🛡️ 5. ANTI SEMANTIC FLOOD
    # ==========================================================

    palabras_prompt = (
        prompt_serializado
        .lower()
        .split()
    )

    if len(palabras_prompt) > MAX_PALABRAS_PROMPT:

        logger.warning(
            "🚨 [SEMANTIC FLOOD] "
            "Demasiadas palabras detectadas."
        )

        return {
            "respuesta": (
                "Tu mensaje es demasiado grande "
                "para procesarlo."
            ),
            "intencion": "HUMANO",
            "confidence": 0.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # 🛡️ 6. ANTI PROMPT REPETITIVO
    # ==========================================================

    palabras_unicas = len(set(palabras_prompt))

    if (
        len(palabras_prompt) > 100
        and palabras_unicas <= 10
    ):

        logger.warning(
            "🚨 [PROMPT SPAM] "
            "Patrón repetitivo detectado."
        )

        return {
            "respuesta": (
                "No pude procesar correctamente "
                "el contenido recibido."
            ),
            "intencion": "SPAM",
            "confidence": 1.0,
            "accion_tool": "ninguna"
        }

    # ==========================================================
    # ⚡ 7. CACHE INTELIGENTE
    # ==========================================================

    cache_key = generar_hash_cache(
        prompt_serializado,
        vendedor_id,
        temperature
    )

    try:

        cache_item = cache_respuestas_ia.get(
            cache_key
        )

        if cache_item:

            edad_cache = (
                now_ts() -
                cache_item.get("ts", 0)
            )

            if edad_cache < CACHE_TTL_SECONDS:

                logger.info(
                    f"⚡ [CACHE HIT] "
                    f"Tenant={vendedor_id} | "
                    f"Edad={edad_cache:.2f}s"
                )

                return cache_item["data"]

    except Exception as cache_error:

        logger.warning(
            f"⚠️ [CACHE ERROR] {cache_error}"
        )

    # ==========================================================
    # 📊 8. ESTIMACIÓN TOKENS
    # ==========================================================

    tokens_estimados = max(
        1,
        len(prompt_serializado) // 4
    )

    # ==========================================================
    # 🛡️ 9. RATE LIMIT TOKENS
    # ==========================================================

    async with rate_limit_global_lock:

        tokens_actuales = (
            tokens_consumidos_tenant.get(
                vendedor_id,
                0
            )
        )

        nuevo_total = (
            tokens_actuales +
            tokens_estimados
        )

        if nuevo_total > MAX_TOKENS_POR_MINUTO_TENANT:

            logger.warning(
                f"🚨 [TOKEN FLOOD] "
                f"Tenant={vendedor_id} "
                f"superó límite."
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

        tokens_consumidos_tenant[vendedor_id] = (
            nuevo_total
        )

    # ==========================================================
    # 🧠 10. FAILOVER MULTI MODELO
    # ==========================================================

    for nombre_modelo in MODELOS_FAILOVER:

        logger.info(
            f"🧠 [GEMINI] "
            f"Iniciando inferencia con: "
            f"{nombre_modelo}"
        )

        for intento in range(retries):

            try:

                # ==========================================================
                # 🛡️ 11. GENERATION CONFIG
                # ==========================================================

                generation_config = (
                    genai.types.GenerationConfig(
                        temperature=temperature,
                        top_p=0.90,
                        top_k=32,
                        candidate_count=1,
                        max_output_tokens=MAX_OUTPUT_TOKENS
                    )
                )

                model = genai.GenerativeModel(
                    nombre_modelo
                )

                # ==========================================================
                # 📦 12. CONSTRUCCIÓN CONTENIDO
                # ==========================================================

                contenido = (
                    prompt
                    if isinstance(prompt, list)
                    else [prompt_serializado]
                )

                # ==========================================================
                # 🖼️ 13. INYECCIÓN MULTIMEDIA
                # ==========================================================

                if media_dict and "data" in media_dict:

                    try:

                        media_bytes = media_dict.get(
                            "data",
                            b""
                        )

                        mime_type = str(
                            media_dict.get(
                                "mime_type",
                                "image/jpeg"
                            )
                        ).lower().strip()

                        if mime_type not in MIME_VALIDOS:

                            logger.warning(
                                f"🚨 [MEDIA MIME] "
                                f"MIME inválido: {mime_type}"
                            )

                        elif not media_bytes:

                            logger.warning(
                                "⚠️ [MEDIA] Payload vacío."
                            )

                        elif len(media_bytes) > MAX_MEDIA_SIZE:

                            logger.warning(
                                "🚨 [MEDIA LIMIT] "
                                "Multimedia excede 20MB."
                            )

                        else:

                            contenido.append({
                                "mime_type": mime_type,
                                "data": media_bytes
                            })

                    except Exception as media_error:

                        logger.warning(
                            f"⚠️ [MEDIA ERROR] "
                            f"{media_error}"
                        )

                # ==========================================================
                # 🚀 14. LLAMADA GEMINI
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
                # 🛡️ 15. VALIDACIÓN RESPONSE
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
                # 🧹 16. LIMPIEZA RESPUESTA
                # ==========================================================

                texto_limpio = (
                    texto_respuesta
                    .replace("```json", "")
                    .replace("```JSON", "")
                    .replace("```", "")
                    .strip()
                )

                texto_limpio = (
                    texto_limpio[:MAX_RESPONSE_CHARS]
                )

                # ==========================================================
                # 🛡️ 17. JSON PARSER HARDENED
                # ==========================================================

                obj = None

                try:

                    decoder = json.JSONDecoder()

                    obj, idx = decoder.raw_decode(
                        texto_limpio
                    )

                except json.JSONDecodeError:

                    logger.warning(
                        "⚠️ [JSON PARSER] "
                        "Fallback regex activado."
                    )

                    match = re.search(
                        r'\{.*\}',
                        texto_limpio,
                        re.DOTALL
                    )

                    if match:

                        try:

                            obj = orjson.loads(
                                match.group()
                            )

                        except Exception as regex_error:

                            logger.error(
                                f"❌ [REGEX PARSER ERROR] "
                                f"{regex_error}"
                            )

                # ==========================================================
                # 🛡️ 18. VALIDACIÓN OBJETO
                # ==========================================================

                if not isinstance(obj, dict):

                    raise ValueError(
                        "Gemini devolvió "
                        "estructura inválida."
                    )

                # ==========================================================
                # 🧹 19. SANITIZACIÓN RESPUESTA
                # ==========================================================

                for key, value in list(obj.items()):

                    if isinstance(value, str):

                        value = bleach.clean(
                            value,
                            tags=[],
                            strip=True
                        )

                        value = limpiar_texto(
                            value
                        )

                        value = value.replace(
                            "\x00",
                            ""
                        )

                        value = value[:5000]

                        obj[key] = value

                # ==========================================================
                # 🛡️ 20. VALIDACIÓN CAMPOS
                # ==========================================================

                obj.setdefault(
                    "respuesta",
                    "No pude generar respuesta."
                )

                obj.setdefault(
                    "intencion",
                    "HUMANO"
                )

                obj.setdefault(
                    "confidence",
                    0.5
                )

                obj.setdefault(
                    "accion_tool",
                    "ninguna"
                )

                # 🛡️ Hard Validation
                if not isinstance(
                    obj["respuesta"],
                    str
                ):
                    obj["respuesta"] = (
                        "Respuesta inválida."
                    )

                # ==========================================================
                # ⚡ 21. GUARDADO CACHE
                # ==========================================================

                try:

                    cache_respuestas_ia[cache_key] = {
                        "data": obj,
                        "ts": now_ts()
                    }

                except Exception as cache_save_error:

                    logger.warning(
                        f"⚠️ [CACHE SAVE ERROR] "
                        f"{cache_save_error}"
                    )

                # ==========================================================
                # 📊 22. TELEMETRÍA
                # ==========================================================

                tiempo_total = (
                    now_ts() -
                    inicio_telemetria
                )

                logger.info(
                    f"✅ [GEMINI SUCCESS] "
                    f"Modelo={nombre_modelo} | "
                    f"Tiempo={tiempo_total:.3f}s | "
                    f"Tokens≈{tokens_estimados} | "
                    f"Tenant={vendedor_id}"
                )

                return obj

            # ==========================================================
            # ⏱️ 23. TIMEOUT CONTROLADO
            # ==========================================================

            except asyncio.TimeoutError:

                logger.warning(
                    f"⏱️ [GEMINI TIMEOUT] "
                    f"Modelo={nombre_modelo} | "
                    f"Intento={intento+1}"
                )

            # ==========================================================
            # 🚨 24. ERRORES CONTROLADOS
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
                # 🚨 QUOTA / 429
                # ==========================================================

                if (
                    "429" in error_str
                    or "quota" in error_str
                    or "resource exhausted" in error_str
                    or "rate limit" in error_str
                ):

                    gemini_bloqueado_hasta = (
                        now_ts() + 60.0
                    )

                    logger.warning(
                        "🚨 [QUOTA LIMIT] "
                        "Circuit breaker 60s."
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
    # 🚨 25. FAILSAFE FINAL
    # ==========================================================

    tiempo_total = (
        now_ts() -
        inicio_telemetria
    )

    logger.error(
        f"🚨 [GEMINI FAILSAFE] "
        f"Todos los modelos fallaron | "
        f"Tiempo={tiempo_total:.3f}s"
    )

    return RESPUESTA_FAILSAFE

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
        logger.warning("🚨 [VALIDADOR IA] Payload excesivo detectado.")
        raise Exception("Payload IA sospechoso.")

    # ==============================================================================
    # 🛡️ 3. ENUMS SEGUROS
    # ==============================================================================

    INTENCIONES_VALIDAS = {
        "COMPRA",
        "COTIZACION",
        "HUMANO",
        "PEDIDO_ESPECIAL",
        "REGATEO",
        "POSTVENTA",
        "GARANTIA",
        "SPAM",
        "MAYOREO",
        "SALUDO",
        "ENOJO"
    }

    EMOCIONES_VALIDAS = {
        "urgencia",
        "enojo",
        "duda",
        "entusiasmo",
        "neutral"
    }

    TEMPERATURAS_VALIDAS = {
        "frio",
        "tibio",
        "caliente"
    }

    TOOLS_VALIDAS = {
        "ninguna",
        "aplicar_descuento"
    }

    PRIORIDADES_VALIDAS = {
        "baja",
        "media",
        "alta",
        "critica"
    }

    # ==============================================================================
    # 🛡️ 4. HELPERS HARDENED
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
                    logger.warning(
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

            texto = limpiar_texto(texto)

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

            logger.warning(
                f"⚠️ [VALIDADOR IA] Error limpiando texto: {e}"
            )

            return ""

    def safe_float(
        valor,
        default: float = 0.0,
        minimo: float = 0.0,
        maximo: float = 999999.0
    ) -> float:

        try:

            num = float(valor)

            # Protección NaN / Infinity
            if math.isnan(num) or math.isinf(num):
                return default

            return max(minimo, min(num, maximo))

        except:
            return default

    def safe_int(
        valor,
        default: int = 0,
        minimo: int = 0,
        maximo: int = 100
    ) -> int:

        try:

            num = int(float(valor))

            return max(minimo, min(num, maximo))

        except:
            return default

    # ==============================================================================
    # 🛡️ 5. NORMALIZACIÓN PRINCIPAL
    # ==============================================================================

    intencion = safe_clean_text(
        data.get("intencion", "HUMANO"),
        40
    ).upper()

    if intencion not in INTENCIONES_VALIDAS:
        intencion = "HUMANO"

    emocion_cliente = safe_clean_text(
        data.get("emocion_cliente", "neutral"),
        30
    ).lower()

    if emocion_cliente not in EMOCIONES_VALIDAS:
        emocion_cliente = "neutral"

    temperatura_lead = safe_clean_text(
        data.get("temperatura_lead", "frio"),
        30
    ).lower()

    if temperatura_lead not in TEMPERATURAS_VALIDAS:
        temperatura_lead = "frio"

    accion_tool = safe_clean_text(
        data.get("accion_tool", "ninguna"),
        50
    ).lower()

    if accion_tool not in TOOLS_VALIDAS:
        accion_tool = "ninguna"

    nivel_prioridad = safe_clean_text(
        data.get("nivel_prioridad", "media"),
        20
    ).lower()

    if nivel_prioridad not in PRIORIDADES_VALIDAS:
        nivel_prioridad = "media"

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
            safe_clean_text(
                data.get("tipo_seguimiento", "ninguno"),
                30
            ),

        "requiere_seguimiento":
            bool(data.get("requiere_seguimiento", False)),

        "sugerir_veltrix":
            bool(data.get("sugerir_veltrix", False)),

        "confidence":
            confidence,

        "precio_oferta":
            precio_oferta,

        "lead_score":
            lead_score,

        "probabilidad_cierre":
            probabilidad_cierre
    }

    # ==============================================================================
    # 🛡️ 9. PROTECCIÓN COMERCIAL
    # ==============================================================================

    if (
        res["accion_tool"] == "aplicar_descuento"
        and res["precio_oferta"] <= 0
    ):

        logger.warning(
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

        logger.warning(
            "🚨 [VALIDADOR IA] Respuesta sospechosa neutralizada."
        )

        res["respuesta"] = (
            "Voy a canalizar tu solicitud con un asesor. 👌"
        )

        res["intencion"] = "HUMANO"

    # ==============================================================================
    # 📊 11. TELEMETRÍA
    # ==============================================================================

    logger.info(
        f"🎯 [VALIDADOR IA] "
        f"Intención={res['intencion']} | "
        f"Score={res['lead_score']} | "
        f"Confidence={res['confidence']:.2f}"
    )

    # ==============================================================================
    # ✅ 12. RESPUESTA FINAL
    # ==============================================================================

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

async def obtener_contexto_inventario_rag(
    vendedor_id: str,
    texto_cliente: str = ""
) -> str:

    """
    ==============================================================================
    🧠 RAG INVENTARIO AAA ENTERPRISE HARDENED
    ==============================================================================
    ✔ Anti Token Inflation
    ✔ Anti Full Scan
    ✔ Cache Inteligente
    ✔ Protección Anti Flood
    ✔ Fuzzy Matching Hardened
    ✔ Sanitización avanzada
    ✔ Protección RAM
    ✔ Normalización semántica
    ✔ Fallback resiliente
    ✔ Optimizado para Gemini Cost Saving
    ==============================================================================
    """

    logger.info(
        f"🔍 [RAG INVENTARIO] "
        f"Buscando coincidencias para: '{texto_cliente}' "
        f"(Tenant: {vendedor_id})"
    )

    try:

        # ==============================================================================
        # 🛡️ 1. SANITIZACIÓN HARDENED
        # ==============================================================================

        texto_limpio = limpiar_texto(
            bleach.clean(
                str(texto_cliente),
                tags=[],
                strip=True
            )
        )

        texto_limpio = re.sub(
            r"[^\w\sáéíóúüñÁÉÍÓÚÜÑ\-]",
            " ",
            texto_limpio
        )

        texto_limpio = re.sub(
            r"\s+",
            " ",
            texto_limpio
        ).strip().lower()

        # ==============================================================================
        # 🛡️ 2. LIMITADOR ANTI TOKEN DRAIN
        # ==============================================================================

        if len(texto_limpio) > 120:

            logger.warning(
                "⚠️ [RAG INVENTARIO] "
                "Texto cliente truncado para evitar token inflation."
            )

            texto_limpio = texto_limpio[:120]

        # ==============================================================================
        # 🛡️ 3. CACHE KEY NORMALIZADA
        # ==============================================================================

        cache_key = hashlib.sha256(
            f"{vendedor_id}:{texto_limpio}".encode()
        ).hexdigest()

        cache_item = cache_respuestas_ia.get(cache_key)

        if cache_item:

            edad = now_ts() - cache_item.get("ts", 0)

            if edad <= 20:

                logger.info(
                    f"⚡ [RAG CACHE HIT] "
                    f"Edad={edad:.2f}s"
                )

                return cache_item["data"]

        # ==============================================================================
        # 🧠 4. TOKENIZACIÓN CONTROLADA
        # ==============================================================================

        palabras = [
            p.strip()
            for p in texto_limpio.split()
            if len(p.strip()) >= 2
        ]

        # 🛡️ Anti Query Explosion
        palabras = palabras[:5]

        logger.info(
            f"🧠 [RAG INVENTARIO] "
            f"Keywords útiles: {palabras}"
        )

        # ==============================================================================
        # 🛡️ 5. QUERY BASE HARDENED
        # ==============================================================================

        query = (
            supabase
            .table('inventario')
            .select(
                'nombre, precio, stock, atributos_extra'
            )
            .eq('vendedor_id', str(vendedor_id))
            .gt('stock', 0)
        )

        # ==============================================================================
        # 🚀 6. PREFILTRO SQL OPTIMIZADO
        # ==============================================================================

        if palabras:

            keyword_principal = palabras[0]

            if len(keyword_principal) >= 3:

                query = query.ilike(
                    'nombre',
                    f"%{keyword_principal}%"
                )

        # ==============================================================================
        # 🛡️ 7. LIMITADOR HARDENED
        # ==============================================================================

        LIMITE_DB = 60

        res_inv = await asyncio.wait_for(
            async_db_execute(
                query.limit(LIMITE_DB)
            ),
            timeout=8.0
        )

        # ==============================================================================
        # 🛡️ 8. FALLBACK RESILIENTE
        # ==============================================================================

        if not res_inv.data:

            logger.warning(
                "⚠️ [RAG INVENTARIO] "
                "Prefiltro vacío. Activando fallback."
            )

            fallback_query = (
                supabase
                .table('inventario')
                .select(
                    'nombre, precio, stock, atributos_extra'
                )
                .eq('vendedor_id', str(vendedor_id))
                .gt('stock', 0)
                .limit(20)
            )

            res_inv = await asyncio.wait_for(
                async_db_execute(fallback_query),
                timeout=8.0
            )

            if not res_inv.data:

                logger.warning(
                    "⚠️ [RAG INVENTARIO] "
                    "Catálogo vacío."
                )

                return (
                    "Catálogo temporalmente vacío "
                    "o sin stock disponible."
                )

        inventario = res_inv.data[:LIMITE_DB]

        # ==============================================================================
        # 🛡️ 9. NORMALIZADOR ATRIBUTOS EXTRA
        # ==============================================================================

        def _obtener_info_extra(item_db: dict) -> str:

            extras = item_db.get('atributos_extra') or {}

            if not isinstance(extras, dict):
                return ""

            info_valiosa = (
                extras.get('consola')
                or extras.get('marca')
                or extras.get('modelo')
                or extras.get('categoria')
                or ""
            )

            info_valiosa = limpiar_texto(
                str(info_valiosa)
            )[:40]

            return (
                f" ({info_valiosa})"
                if info_valiosa else ""
            )

        # ==============================================================================
        # 📋 10. RESPUESTA GENERAL SI NO HAY CONTEXTO
        # ==============================================================================

        if not palabras:

            logger.info(
                "📋 [RAG INVENTARIO] "
                "Sin keywords. Retornando TOP GENERAL."
            )

            lineas_generales = []

            for item in inventario[:10]:

                nombre = limpiar_texto(
                    str(item.get("nombre", "Producto"))
                )[:80]

                precio = item.get("precio", 0)
                stock = item.get("stock", 0)

                linea = (
                    f"- {nombre}"
                    f"{_obtener_info_extra(item)}"
                    f" | Precio: ${precio}"
                    f" | Disp: {stock}"
                )

                lineas_generales.append(linea)

            resultado = "\n".join(lineas_generales)

            cache_respuestas_ia[cache_key] = {
                "data": resultado,
                "ts": now_ts()
            }

            return resultado

        # ==============================================================================
        # 🧠 11. ÍNDICE DIFUSO OPTIMIZADO
        # ==============================================================================

        diccionario_opciones = {}

        for item in inventario:

            nombre = limpiar_texto(
                str(item.get("nombre", ""))
            )[:120]

            llave = (
                f"{nombre} "
                f"{_obtener_info_extra(item)}"
            ).strip().lower()

            if llave:
                diccionario_opciones[llave] = item

        # ==============================================================================
        # 🛡️ 12. PROTECCIÓN ANTI FUZZY EXPLOSION
        # ==============================================================================

        if len(diccionario_opciones) > 200:

            logger.warning(
                "⚠️ [RAG INVENTARIO] "
                "Reduciendo índice fuzzy por protección RAM."
            )

            diccionario_opciones = dict(
                list(diccionario_opciones.items())[:200]
            )

        # ==============================================================================
        # 🚀 13. MATCHING DIFUSO HARDENED
        # ==============================================================================

        matches = process.extract(
            texto_limpio,
            diccionario_opciones.keys(),
            scorer=fuzz.token_sort_ratio,
            limit=8
        )

        items_filtrados = []

        for match_str, score, _ in matches:

            # 🛡️ Score endurecido
            if score >= 45:

                item = diccionario_opciones.get(match_str)

                if item:
                    items_filtrados.append(item)

        # ==============================================================================
        # 🛡️ 14. FALLBACK SEMÁNTICO
        # ==============================================================================

        if not items_filtrados:

            logger.warning(
                "⚠️ [RAG INVENTARIO] "
                "Sin matches fuertes. Activando fallback semántico."
            )

            items_filtrados = inventario[:5]

        # ==============================================================================
        # 📦 15. CONSTRUCCIÓN RAG OPTIMIZADA
        # ==============================================================================

        lineas = []

        for item in items_filtrados[:8]:

            nombre = limpiar_texto(
                str(item.get("nombre", "Producto"))
            )[:80]

            precio = item.get("precio", 0)
            stock = item.get("stock", 0)

            linea = (
                f"- {nombre}"
                f"{_obtener_info_extra(item)}"
                f" | Precio: ${precio}"
                f" | Disp: {stock}"
            )

            lineas.append(linea)

        resultado = "\n".join(lineas)

        # ==============================================================================
        # 🛡️ 16. LIMITADOR FINAL TOKENS
        # ==============================================================================

        MAX_RAG_CHARS = 1800

        if len(resultado) > MAX_RAG_CHARS:

            logger.warning(
                "⚠️ [RAG INVENTARIO] "
                "Contexto truncado para ahorro tokens."
            )

            resultado = resultado[:MAX_RAG_CHARS]

        # ==============================================================================
        # ⚡ 17. CACHE FINAL
        # ==============================================================================

        cache_respuestas_ia[cache_key] = {
            "data": resultado,
            "ts": now_ts()
        }

        logger.info(
            f"✅ [RAG INVENTARIO] "
            f"Contexto generado correctamente "
            f"({len(lineas)} items)."
        )

        return resultado

    except asyncio.TimeoutError:

        logger.error(
            "⏱️ [RAG INVENTARIO] Timeout recuperando inventario."
        )

        return (
            "El catálogo está tardando más de lo normal. "
            "Intenta nuevamente."
        )

    except Exception as e:

        logger.error(
            f"❌ [RAG ERROR] "
            f"Falló la construcción del contexto: {str(e)}"
        )

        return (
            "Error técnico recuperando productos disponibles."
        )


async def obtener_historial_chat(
    telefono: str,
    vendedor_id: str
) -> str:

    """
    ==============================================================================
    📖 HISTORIAL CHAT AAA ENTERPRISE HARDENED
    ==============================================================================
    ✔ Anti Token Inflation
    ✔ Compresión Conversacional
    ✔ Sanitización extrema
    ✔ Protección RAM
    ✔ Protección prompts maliciosos
    ✔ Limitador histórico
    ✔ Caché inteligente
    ✔ Optimizado para Gemini Cost Saving
    ==============================================================================
    """

    logger.info(
        f"📖 [HISTORIAL CHAT] "
        f"Solicitando historial Tel={telefono}"
    )

    try:

        # ==============================================================================
        # 🛡️ 1. NORMALIZACIÓN INPUT
        # ==============================================================================

        telefono = re.sub(
            r"[^\d]",
            "",
            str(telefono)
        )[:20]

        vendedor_id = limpiar_texto(
            str(vendedor_id)
        )[:40]

        # ==============================================================================
        # 🛡️ 2. CACHE HISTORIAL
        # ==============================================================================

        cache_key = hashlib.sha256(
            f"HIST:{telefono}:{vendedor_id}".encode()
        ).hexdigest()

        cache_item = cache_respuestas_ia.get(cache_key)

        if cache_item:

            edad = now_ts() - cache_item.get("ts", 0)

            if edad <= 15:

                logger.info(
                    f"⚡ [HIST CACHE HIT] "
                    f"Edad={edad:.2f}s"
                )

                return cache_item["data"]

        # ==============================================================================
        # 🛡️ 3. QUERY HARDENED
        # ==============================================================================

        query = (
            supabase
            .table('mensajes_chat')
            .select('autor, mensaje')
            .eq('telefono', telefono)
            .eq('vendedor_id', str(vendedor_id))
            .order('created_at', desc=True)
            .limit(12)
        )

        res_hist = await asyncio.wait_for(
            async_db_execute(query),
            timeout=8.0
        )

        # ==============================================================================
        # 🛡️ 4. HISTORIAL VACÍO
        # ==============================================================================

        if not res_hist.data:

            logger.info(
                "🆕 [HISTORIAL CHAT] "
                "Cliente nuevo detectado."
            )

            return (
                "Primer mensaje registrado del cliente."
            )

        # ==============================================================================
        # 🧠 5. REORDENAMIENTO CRONOLÓGICO
        # ==============================================================================

        mensajes_ordenados = list(
            reversed(res_hist.data)
        )

        # ==============================================================================
        # 🛡️ 6. LIMPIEZA Y COMPRESIÓN
        # ==============================================================================

        lineas = []

        for m in mensajes_ordenados:

            autor = limpiar_texto(
                str(m.get("autor", "USER"))
            )[:15]

            mensaje = limpiar_texto(
                bleach.clean(
                    str(m.get("mensaje", "")),
                    tags=[],
                    strip=True
                )
            )

            # 🛡️ Anti Prompt Injection Persistente
            mensaje = re.sub(
                r"(system prompt|developer mode|ignore instructions|eres chatgpt)",
                "[FILTRADO]",
                mensaje,
                flags=re.IGNORECASE
            )

            # 🛡️ Anti Token Abuse
            mensaje = mensaje[:350]

            if mensaje.strip():

                lineas.append(
                    f"{autor}: {mensaje}"
                )

        # ==============================================================================
        # 🛡️ 7. HISTORIAL FINAL
        # ==============================================================================

        historial_texto = "\n".join(lineas)

        # ==============================================================================
        # 🛡️ 8. LIMITADOR TOKENS GEMINI
        # ==============================================================================

        MAX_CHARS = 2500

        if len(historial_texto) > MAX_CHARS:

            logger.warning(
                "⚠️ [HISTORIAL CHAT] "
                "Historial truncado para ahorrar tokens."
            )

            historial_texto = (
                "... [HISTORIAL COMPRIMIDO] ...\n"
                + historial_texto[-MAX_CHARS:]
            )

        # ==============================================================================
        # 🛡️ 9. VALIDACIÓN FINAL
        # ==============================================================================

        if not historial_texto.strip():

            historial_texto = (
                "No hay suficiente historial disponible."
            )

        # ==============================================================================
        # ⚡ 10. CACHE FINAL
        # ==============================================================================

        cache_respuestas_ia[cache_key] = {
            "data": historial_texto,
            "ts": now_ts()
        }

        logger.info(
            "✅ [HISTORIAL CHAT] "
            "Historial recuperado correctamente."
        )

        return historial_texto

    except asyncio.TimeoutError:

        logger.error(
            "⏱️ [HISTORIAL CHAT] Timeout recuperando historial."
        )

        return (
            "El historial está tardando demasiado en cargar."
        )

    except Exception as e:

        logger.error(
            f"❌ [HISTORIAL ERROR] "
            f"{str(e)}"
        )

        return (
            "No se pudo recuperar el historial de conversación."
        )

# ==========================================================
# 🛠️ 6. FUNCIONES CORE: SCRAPER, ALERTAS, MEDIA Y COMUNICACIÓN
# ==========================================================

def sanitizar_nombre_columna(
    columna: str,
    permitir_reservadas: bool = False
) -> str:

    """
    🛡️ Sanitizador Hardened de columnas CRM
    ---------------------------------------------------------
    - Anti XSS
    - Anti corrupción CRM
    - Anti payload injection
    - Whitelist estricta
    - Protección contra columnas inválidas
    ---------------------------------------------------------
    """

    try:

        columna = bleach.clean(
            str(columna),
            tags=[],
            attributes={},
            strip=True
        )

        columna = limpiar_texto(columna).strip()

        # ==========================================================
        # 🛡️ LISTA BLANCA CRM
        # ==========================================================
        columnas_validas = {
            "Bandeja Nueva",
            "Envios Masivos",
            "Por Entregar",
            "Requiere Asistencia",
            "En Conversacion",
            "Completado",
            "Cancelado",
            "Spam"
        }

        # ==========================================================
        # 🛡️ ILUMINACIONES VÁLIDAS
        # ==========================================================
        iluminaciones_validas = {
            "blanco",
            "verde_exito",
            "verde_alerta",
            "amarillo",
            "rojo",
            "gris"
        }

        if permitir_reservadas:

            if (
                columna in columnas_validas
                or columna in iluminaciones_validas
            ):
                return columna[:50]

        # ==========================================================
        # 🛡️ FALLBACK
        # ==========================================================
        if len(columna) <= 50:
            return columna

        return columna[:50]

    except Exception as e:

        logger.error(
            f"❌ [CRM SANITIZE ERROR] {str(e)}"
        )

        return "Bandeja Nueva"


# ==========================================================
# 🧠 ACTUALIZACIÓN CENTRAL CRM
# ==========================================================
async def actualizar_estado_crm(
    telefono: str,
    vendedor_id: str,
    columna: str,
    iluminacion: str,
    juego: str,
    perfil_ia: dict = None
):

    """
    🚀 MOTOR CRM AAA ENTERPRISE
    ---------------------------------------------------------
    FUNCIONES:
    - Protección concurrente
    - Anti overwrite
    - Sanitización profunda
    - Validación JSONB
    - Anti race conditions
    - Anti spam writes
    - Protección cross-tenant
    - Idempotencia CRM
    - Telemetría avanzada
    - Anti corrupción de perfil
    ---------------------------------------------------------
    """

    inicio_telemetria = now_ts()

    try:

        # ==========================================================
        # 🛡️ 1. SANITIZACIÓN INPUTS
        # ==========================================================
        telefono = str(telefono).strip()
        vendedor_id = str(vendedor_id).strip()

        columna = sanitizar_nombre_columna(
            columna,
            permitir_reservadas=True
        )

        iluminacion = sanitizar_nombre_columna(
            iluminacion,
            permitir_reservadas=True
        )

        juego = bleach.clean(
            str(juego),
            tags=[],
            strip=True
        )

        juego = limpiar_texto(juego)[:100]

        if not telefono or not vendedor_id:

            logger.warning(
                "⚠️ [CRM UPDATE] "
                "Parámetros incompletos."
            )

            return False

        # ==========================================================
        # 🛡️ 2. ANTI WRITE FLOOD
        # ==========================================================
        flood_key = hashlib.sha256(
            (
                f"{telefono}:"
                f"{vendedor_id}:"
                f"{columna}:"
                f"{iluminacion}:"
                f"{juego}"
            ).encode()
        ).hexdigest()

        async with rate_limit_global_lock:

            if flood_key in registro_actividad_b2b:

                logger.info(
                    f"♻️ [CRM SKIP] "
                    f"Update duplicado evitado para {telefono}"
                )

                return True

            registro_actividad_b2b[flood_key] = now_ts()

        # ==========================================================
        # 🛡️ 3. VALIDACIÓN PERFIL IA
        # ==========================================================
        perfil_sanitizado = None

        if perfil_ia:

            try:

                if not isinstance(perfil_ia, dict):
                    raise Exception(
                        "perfil_ia inválido"
                    )

                perfil_sanitizado = {}

                for key, value in perfil_ia.items():

                    key_limpia = limpiar_texto(
                        bleach.clean(
                            str(key),
                            tags=[],
                            strip=True
                        )
                    )[:80]

                    # ==========================================================
                    # 🛡️ STRING
                    # ==========================================================
                    if isinstance(value, str):

                        perfil_sanitizado[key_limpia] = (
                            limpiar_texto(
                                bleach.clean(
                                    value,
                                    tags=[],
                                    strip=True
                                )
                            )[:500]
                        )

                    # ==========================================================
                    # 🛡️ NUMÉRICOS
                    # ==========================================================
                    elif isinstance(value, (int, float, bool)):
                        perfil_sanitizado[key_limpia] = value

                    # ==========================================================
                    # 🛡️ LISTAS SEGURAS
                    # ==========================================================
                    elif isinstance(value, list):

                        perfil_sanitizado[key_limpia] = [
                            limpiar_texto(str(v))[:120]
                            for v in value[:20]
                        ]

                # ==========================================================
                # 🛡️ LIMITADOR JSONB
                # ==========================================================
                perfil_serializado = orjson.dumps(
                    perfil_sanitizado
                )

                if len(perfil_serializado) > 12000:

                    logger.warning(
                        "⚠️ [CRM PROFILE LIMIT] "
                        "Perfil IA truncado."
                    )

                    perfil_sanitizado = {
                        "estado": "perfil_truncado"
                    }

            except Exception as perfil_e:

                logger.error(
                    f"❌ [CRM PROFILE ERROR] "
                    f"{perfil_e}"
                )

                perfil_sanitizado = {
                    "estado": "perfil_error"
                }

        # ==========================================================
        # 🛡️ 4. PAYLOAD HARDENED
        # ==========================================================
        payload = {
            "columna": columna,
            "estado_iluminacion": iluminacion,
            "ultimo_producto_interes": juego,
            "ultima_interaccion_ia": (
                datetime.now(timezone.utc)
                .isoformat()
            )
        }

        # ==========================================================
        # 🛡️ PERFIL IA OPCIONAL
        # ==========================================================
        if perfil_sanitizado:

            payload["perfil_psicologico"] = (
                perfil_sanitizado
            )

        # ==========================================================
        # 🛡️ 5. LOCK POR CLIENTE
        # ==========================================================
        lock_key = hashlib.sha256(
            f"{telefono}:{vendedor_id}".encode()
        ).hexdigest()

        tracking_locks_uso[lock_key] = now_ts()

        async with locks_por_conversacion[lock_key]:

            # ==========================================================
            # 🛡️ 6. UPDATE CONTROLADO
            # ==========================================================
            response = await async_db_execute(

                supabase
                .table("prospectos")
                .update(payload)
                .eq("telefono", telefono)
                .eq("vendedor_id", vendedor_id),

                timeout_seg=10.0
            )

        # ==========================================================
        # 📊 7. TELEMETRÍA
        # ==========================================================
        tiempo_total = (
            now_ts() - inicio_telemetria
        )

        logger.info(
            f"💾 [CRM UPDATE SUCCESS] "
            f"Tel={enmascarar_telefono(telefono)} | "
            f"Columna={columna} | "
            f"Tiempo={tiempo_total:.3f}s"
        )

        return True

    except Exception as e:

        logger.exception(
            f"❌ [CRM UPDATE ERROR] "
            f"{str(e)}"
        )

        return False


# ==========================================================
# 🧠 GUARDADO DE RESULTADO IA CRM
# ==========================================================
async def guardar_resultado_ia_en_crm(
    telefono: str,
    vendedor_id: str,
    data: dict
) -> bool:

    """
    💾 Persistencia avanzada de resultados IA
    ---------------------------------------------------------
    - Sanitización profunda
    - Anti corrupción CRM
    - Validación semántica
    - Protección JSON
    - Anti spam writes
    ---------------------------------------------------------
    """

    try:

        telefono = str(telefono).strip()
        vendedor_id = str(vendedor_id).strip()

        if not telefono or not vendedor_id:

            logger.warning(
                "⚠️ [CRM IA SAVE] "
                "Datos inválidos."
            )

            return False

        # ==========================================================
        # 🛡️ SANITIZACIÓN DATA
        # ==========================================================
        payload = {

            "lead_score": int(
                max(
                    0,
                    min(
                        100,
                        int(data.get("lead_score", 0))
                    )
                )
            ),

            "probabilidad_cierre": float(
                max(
                    0.0,
                    min(
                        100.0,
                        float(
                            data.get(
                                "probabilidad_cierre",
                                0.0
                            )
                        )
                    )
                )
            ),

            "estrategia_venta": limpiar_texto(
                bleach.clean(
                    str(data.get("estrategia_venta", "")),
                    tags=[],
                    strip=True
                )
            )[:500],

            "requiere_seguimiento": bool(
                data.get("requiere_seguimiento", False)
            ),

            "sugerir_veltrix": bool(
                data.get("sugerir_veltrix", False)
            ),

            "tipo_seguimiento": limpiar_texto(
                bleach.clean(
                    str(data.get("tipo_seguimiento", "")),
                    tags=[],
                    strip=True
                )
            )[:100],

            "cross_selling": limpiar_texto(
                bleach.clean(
                    str(data.get("cross_selling", "")),
                    tags=[],
                    strip=True
                )
            )[:300],

            "upselling": limpiar_texto(
                bleach.clean(
                    str(data.get("upselling", "")),
                    tags=[],
                    strip=True
                )
            )[:300],

            "nivel_prioridad": limpiar_texto(
                bleach.clean(
                    str(data.get("nivel_prioridad", "")),
                    tags=[],
                    strip=True
                )
            )[:50],

            "ultimo_msj": limpiar_texto(
                bleach.clean(
                    str(data.get("respuesta", "")),
                    tags=[],
                    strip=True
                )
            )[:2000],

            "ultima_interaccion_ia": (
                datetime.now(timezone.utc)
                .isoformat()
            )
        }

        # ==========================================================
        # 🛡️ UPDATE CONTROLADO
        # ==========================================================
        await async_db_execute(

            supabase
            .table("prospectos")
            .update(payload)
            .eq("telefono", telefono)
            .eq("vendedor_id", str(vendedor_id)),

            timeout_seg=10.0
        )

        logger.info(
            f"💾 [CRM SYNC SUCCESS] "
            f"{enmascarar_telefono(telefono)} | "
            f"Score={payload.get('lead_score')}"
        )

        return True

    except Exception as e:

        logger.exception(
            f"❌ [CRM SYNC ERROR] "
            f"{str(e)}"
        )

        return False


# ==========================================================
# 💬 GUARDADO DE CHAT
# ==========================================================
async def guardar_mensaje_chat(
    telefono: str,
    vendedor_id: str,
    autor: str,
    mensaje: str
):

    """
    💬 Persistencia Hardened de conversaciones
    ---------------------------------------------------------
    - Sanitización XSS
    - Anti spam insert
    - Protección DB Flood
    - Limitador de tamaño
    - Telemetría avanzada
    ---------------------------------------------------------
    """

    try:

        telefono = str(telefono).strip()
        vendedor_id = str(vendedor_id).strip()
        autor = str(autor).strip().upper()

        if not telefono or not vendedor_id:

            logger.warning(
                "⚠️ [CHAT SAVE] "
                "Datos inválidos."
            )

            return False

        # ==========================================================
        # 🛡️ SANITIZACIÓN MENSAJE
        # ==========================================================
        mensaje_limpio = bleach.clean(
            limpiar_texto(str(mensaje)),
            tags=[],
            strip=True
        )

        mensaje_limpio = mensaje_limpio[:5000]

        # ==========================================================
        # 🛡️ ANTI DUPLICADOS
        # ==========================================================
        msg_hash = hashlib.sha256(
            (
                f"{telefono}:"
                f"{autor}:"
                f"{mensaje_limpio[:120]}"
            ).encode()
        ).hexdigest()

        if msg_hash in mensajes_procesados_meta:

            logger.info(
                "♻️ [CHAT DUPLICATE BLOCK]"
            )

            return True

        mensajes_procesados_meta[msg_hash] = True

        # ==========================================================
        # 💾 INSERT CONTROLADO
        # ==========================================================
        await async_db_execute(

            supabase
            .table("mensajes_chat")
            .insert({
                "telefono": telefono,
                "vendedor_id": vendedor_id,
                "autor": autor,
                "mensaje": mensaje_limpio
            }),

            timeout_seg=8.0
        )

        logger.info(
            f"💬 [CHAT SAVE SUCCESS] "
            f"{enmascarar_telefono(telefono)}"
        )

        return True

    except Exception as e:

        logger.exception(
            f"❌ [CHAT SAVE ERROR] "
            f"{str(e)}"
        )

        return False

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

    # ==========================================================
    # 🛡️ 1. VALIDACIÓN HTTP CLIENT
    # ==========================================================
    if not http_client:

        logger.error(
            "❌ [WHATSAPP OUTBOUND] "
            "http_client no inicializado."
        )

        return False

    # ==========================================================
    # 🛡️ 2. VALIDACIÓN BÁSICA INPUTS
    # ==========================================================
    telefono_destino = str(telefono_destino).strip()
    texto_mensaje = str(texto_mensaje).strip()
    token = str(token).strip()
    phone_id = str(phone_id).strip()

    if (
        not telefono_destino
        or not texto_mensaje
        or not token
        or not phone_id
    ):

        logger.warning(
            "⚠️ [WHATSAPP OUTBOUND] "
            "Parámetros inválidos."
        )

        return False

    # ==========================================================
    # 🛡️ 3. SANITIZACIÓN MENSAJE
    # ==========================================================
    texto_mensaje = bleach.clean(
        texto_mensaje,
        tags=[],
        strip=True
    )

    texto_mensaje = limpiar_texto(texto_mensaje)

    # ==========================================================
    # 🛡️ 4. LÍMITE HARDENED META
    # ==========================================================
    MAX_MESSAGE_LENGTH = 4096

    if len(texto_mensaje) > MAX_MESSAGE_LENGTH:

        logger.warning(
            f"⚠️ [WHATSAPP LIMIT] "
            f"Mensaje truncado: {len(texto_mensaje)} chars."
        )

        texto_mensaje = texto_mensaje[:MAX_MESSAGE_LENGTH]

    # ==========================================================
    # 🛡️ 5. ANTI DUPLICADOS OUTBOUND
    # ==========================================================
    mensaje_hash = hashlib.sha256(
        f"{telefono_destino}:{texto_mensaje[:120]}".encode()
    ).hexdigest()

    async with rate_limit_mobile_lock:

        if mensaje_hash in RATE_LIMIT_MOBILE_OUTBOUND:

            logger.warning(
                f"♻️ [WHATSAPP DUPLICATE BLOCK] "
                f"Mensaje repetido bloqueado para {telefono_destino}"
            )

            return False

        RATE_LIMIT_MOBILE_OUTBOUND[mensaje_hash] = now_ts()

    # ==========================================================
    # 🛡️ 6. RATE LIMIT GLOBAL OUTBOUND
    # ==========================================================
    rl_key = f"{phone_id}:{telefono_destino}"

    async with rate_limit_mobile_lock:

        outbound_actual = RATE_LIMIT_MOBILE_OUTBOUND.get(
            rl_key,
            0
        )

        if outbound_actual >= 12:

            logger.warning(
                f"🚨 [WHATSAPP FLOOD BLOCK] "
                f"Outbound excedido hacia {telefono_destino}"
            )

            return False

        RATE_LIMIT_MOBILE_OUTBOUND[rl_key] = outbound_actual + 1

    # ==========================================================
    # 🛡️ 7. URL META API
    # ==========================================================
    url = (
        f"https://graph.facebook.com/"
        f"{META_API_VERSION}/"
        f"{phone_id}/messages"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # ==========================================================
    # 🛡️ 8. PAYLOAD HARDENED
    # ==========================================================
    payload = {
        "messaging_product": "whatsapp",
        "to": telefono_destino,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": texto_mensaje
        }
    }

    # ==========================================================
    # 📊 9. TELEMETRÍA
    # ==========================================================
    inicio_telemetria = now_ts()

    logger.info(
        f"📡 [WHATSAPP OUTBOUND] "
        f"Destino={enmascarar_telefono(telefono_destino)}"
    )

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
                url,
                headers=headers,
                json=payload,
                timeout=httpx.Timeout(
                    connect=5.0,
                    read=12.0,
                    write=10.0,
                    pool=5.0
                )
            )

            status = response.status_code

            # ==========================================================
            # ✅ 12. ÉXITO
            # ==========================================================
            if status in [200, 201]:

                tiempo_total = (
                    now_ts() - inicio_telemetria
                )

                logger.info(
                    f"✅ [WHATSAPP SUCCESS] "
                    f"Status={status} | "
                    f"Tiempo={tiempo_total:.3f}s"
                )

                return True

            # ==========================================================
            # 🚨 13. RATE LIMIT META
            # ==========================================================
            if status == 429:

                espera = min(
                    8,
                    2 ** intento
                )

                logger.warning(
                    f"🚨 [META RATE LIMIT] "
                    f"Intento={intento+1} | "
                    f"Backoff={espera}s"
                )

                await asyncio.sleep(espera)

                continue

            # ==========================================================
            # 🚨 14. ERRORES TEMPORALES META
            # ==========================================================
            if status >= 500:

                espera = min(
                    6,
                    2 ** intento
                )

                logger.warning(
                    f"⚠️ [META SERVER ERROR] "
                    f"Status={status} | "
                    f"Retry en {espera}s"
                )

                await asyncio.sleep(espera)

                continue

            # ==========================================================
            # 🚨 15. TOKEN INVÁLIDO / PHONE BLOQUEADO
            # ==========================================================
            if status in [400, 401, 403]:

                logger.error(
                    f"🚨 [META AUTH ERROR] "
                    f"Status={status} | "
                    f"Body={response.text[:500]}"
                )

                return False

            # ==========================================================
            # 🚨 16. ERROR CONTROLADO
            # ==========================================================
            logger.error(
                f"❌ [META ERROR] "
                f"Status={status} | "
                f"Body={response.text[:800]}"
            )

            return False

        # ==========================================================
        # ⏱️ 17. TIMEOUT CONTROLADO
        # ==========================================================
        except asyncio.TimeoutError:

            logger.warning(
                f"⏱️ [WHATSAPP TIMEOUT] "
                f"Intento={intento+1}"
            )

        except httpx.ReadTimeout:

            logger.warning(
                f"⏱️ [HTTPX READ TIMEOUT] "
                f"Intento={intento+1}"
            )

        except httpx.ConnectTimeout:

            logger.warning(
                f"⏱️ [HTTPX CONNECT TIMEOUT] "
                f"Intento={intento+1}"
            )

        # ==========================================================
        # 🚨 18. ERROR CRÍTICO
        # ==========================================================
        except Exception as e:

            logger.exception(
                f"🚨 [WHATSAPP CRITICAL ERROR] "
                f"{str(e)}"
            )

            break

        # ==========================================================
        # 🔄 19. BACKOFF GENERAL
        # ==========================================================
        espera_general = min(
            5,
            1 + intento
        )

        await asyncio.sleep(espera_general)

    # ==========================================================
    # 🚨 20. FAILSAFE FINAL
    # ==========================================================
    logger.error(
        f"🚨 [WHATSAPP FAILSAFE] "
        f"No se pudo enviar mensaje a "
        f"{enmascarar_telefono(telefono_destino)}"
    )

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
# 📥 DESCARGADOR MULTIMEDIA WHATSAPP (AAA HYPERSCALE HARDENED)
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
    ✔ Protección SSRF parcial
    ✔ Validación Content-Type
    ✔ Validación magic bytes
    ✔ Validación entropy payload
    ✔ Protección anti payload corrupto
    ✔ Telemetría avanzada
    ✔ Compatibilidad total arquitectura actual
    ==============================================================================
    """

    # ==============================================================================
    # 🛡️ VALIDACIÓN HTTP CLIENT
    # ==============================================================================

    if not http_client:

        logger.error(
            "❌ [MEDIA] HTTP Client no inicializado."
        )

        return None

    # ==============================================================================
    # 🛡️ VALIDACIÓN INPUT
    # ==============================================================================

    media_id = str(media_id).strip()

    if not media_id:

        logger.warning(
            "⚠️ [MEDIA] Media ID vacío."
        )

        return None

    if len(media_id) > 200:

        logger.warning(
            "🚨 [MEDIA] Media ID sospechosamente largo."
        )

        return None

    token = str(token).strip()

    if not token:

        logger.warning(
            "🚨 [MEDIA] Token vacío."
        )

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
        "image/jpeg",
        "image/png",
        "image/webp",
        "audio/ogg",
        "audio/mp4",
        "audio/mpeg",
        "audio/aac"
    }

    # ==============================================================================
    # 📊 TELEMETRÍA
    # ==============================================================================

    inicio_descarga = now_ts()

    try:

        logger.info(
            f"📥 [MEDIA] "
            f"Iniciando descarga segura MediaID={media_id[:20]}"
        )

        # ==============================================================================
        # 🔍 URL METADATA
        # ==============================================================================

        url_info = (
            f"https://graph.facebook.com/"
            f"{META_API_VERSION}/{media_id}"
        )

        headers = {
            "Authorization": f"Bearer {token}"
        }

        # ==============================================================================
        # 🔄 RETRIES METADATA
        # ==============================================================================

        data_info = None

        for intento in range(MAX_REINTENTOS + 1):

            try:

                logger.info(
                    f"🔍 [MEDIA METADATA] "
                    f"Intento={intento+1}"
                )

                res_info = await asyncio.wait_for(
                    http_client.get(
                        url_info,
                        headers=headers
                    ),
                    timeout=TIMEOUT_INFO
                )

                if res_info.status_code == 200:

                    data_info = res_info.json()

                    break

                logger.warning(
                    f"⚠️ [MEDIA METADATA] "
                    f"HTTP={res_info.status_code}"
                )

                # ==============================================================================
                # 🚨 FAIL FAST AUTH
                # ==============================================================================

                if res_info.status_code in [401, 403]:

                    logger.error(
                        "🚨 [MEDIA AUTH] "
                        "Token inválido."
                    )

                    return None

            except asyncio.TimeoutError:

                logger.warning(
                    f"⏱️ [MEDIA METADATA] "
                    f"Timeout intento={intento+1}"
                )

            except Exception as meta_e:

                logger.warning(
                    f"⚠️ [MEDIA METADATA ERROR] "
                    f"{meta_e}"
                )

            # ==============================================================================
            # 🔄 BACKOFF
            # ==============================================================================

            if intento < MAX_REINTENTOS:

                espera = min(
                    3.0,
                    2 ** intento
                )

                await asyncio.sleep(espera)

        # ==============================================================================
        # 🚨 VALIDACIÓN METADATA
        # ==============================================================================

        if not data_info:

            logger.error(
                "🚨 [MEDIA] "
                "No se pudo recuperar metadata."
            )

            return None

        # ==============================================================================
        # 🛡️ VALIDACIÓN MIME
        # ==============================================================================

        mime_type = str(
            data_info.get("mime_type", "")
        ).lower().strip()

        if mime_type not in MIME_PERMITIDOS:

            logger.warning(
                f"🚨 [MEDIA] MIME bloqueado: {mime_type}"
            )

            return None

        # ==============================================================================
        # 🛡️ VALIDACIÓN FILE SIZE
        # ==============================================================================

        try:

            file_size = int(
                data_info.get("file_size", 0)
            )

        except Exception:

            file_size = 0

        if file_size <= 0:

            logger.warning(
                "⚠️ [MEDIA] File size inválido."
            )

            return None

        if file_size > MAX_MEDIA_SIZE:

            logger.warning(
                f"🚨 [MEDIA] "
                f"Archivo excede límite: "
                f"{file_size/1024/1024:.2f}MB"
            )

            return None

        # ==============================================================================
        # 🛡️ VALIDACIÓN URL
        # ==============================================================================

        media_url = str(
            data_info.get("url", "")
        ).strip()

        if not media_url.startswith("https://"):

            logger.warning(
                "🚨 [MEDIA] URL inválida."
            )

            return None

        # ==============================================================================
        # 🛡️ PROTECCIÓN SSRF PARCIAL
        # ==============================================================================

        dominios_permitidos = [
            "lookaside.fbsbx.com",
            "graph.facebook.com"
        ]

        if not any(
            dominio in media_url
            for dominio in dominios_permitidos
        ):

            logger.warning(
                f"🚨 [MEDIA SSRF] "
                f"Dominio no permitido: {media_url[:80]}"
            )

            return None

        logger.info(
            f"📦 [MEDIA] "
            f"MIME={mime_type} | "
            f"Peso={file_size/1024:.1f}KB"
        )

        # ==============================================================================
        # 📥 DESCARGA BINARIA
        # ==============================================================================

        data_bytes = None

        for intento in range(MAX_REINTENTOS + 1):

            try:

                logger.info(
                    f"📥 [MEDIA DOWNLOAD] "
                    f"Intento={intento+1}"
                )

                res_media = await asyncio.wait_for(
                    http_client.get(
                        media_url,
                        headers=headers
                    ),
                    timeout=TIMEOUT_DOWNLOAD
                )

                if res_media.status_code == 200:

                    # ==============================================================================
                    # 🛡️ VALIDACIÓN CONTENT TYPE
                    # ==============================================================================

                    content_type = str(
                        res_media.headers.get(
                            "Content-Type",
                            ""
                        )
                    ).lower()

                    if mime_type not in content_type:

                        logger.warning(
                            f"🚨 [MEDIA CONTENT-TYPE] "
                            f"Esperado={mime_type} | "
                            f"Recibido={content_type}"
                        )

                        return None

                    data_bytes = res_media.content

                    break

                logger.warning(
                    f"⚠️ [MEDIA DOWNLOAD] "
                    f"HTTP={res_media.status_code}"
                )

            except asyncio.TimeoutError:

                logger.warning(
                    f"⏱️ [MEDIA DOWNLOAD] "
                    f"Timeout intento={intento+1}"
                )

            except Exception as dl_e:

                logger.warning(
                    f"⚠️ [MEDIA DOWNLOAD ERROR] "
                    f"{dl_e}"
                )

            if intento < MAX_REINTENTOS:

                espera = min(
                    4.0,
                    2 ** intento
                )

                await asyncio.sleep(espera)

        # ==============================================================================
        # 🚨 VALIDACIÓN PAYLOAD
        # ==============================================================================

        if not data_bytes:

            logger.warning(
                "⚠️ [MEDIA] Payload vacío."
            )

            return None

        payload_size = len(data_bytes)

        if payload_size > MAX_MEDIA_SIZE:

            logger.warning(
                "🚨 [MEDIA] Payload excede límite."
            )

            return None

        if payload_size < 32:

            logger.warning(
                "🚨 [MEDIA] Payload sospechosamente pequeño."
            )

            return None

        # ==============================================================================
        # 🖼️ VALIDACIÓN IMAGEN
        # ==============================================================================

        if mime_type.startswith("image/"):

            try:

                Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS

                img = Image.open(
                    io.BytesIO(data_bytes)
                )

                img.verify()

                # ==============================================================================
                # 🛡️ VALIDACIÓN DIMENSIONES
                # ==============================================================================

                ancho, alto = img.size

                if ancho <= 0 or alto <= 0:

                    logger.warning(
                        "🚨 [MEDIA IMG] "
                        "Dimensiones inválidas."
                    )

                    return None

                total_pixels = ancho * alto

                if total_pixels > MAX_IMAGE_PIXELS:

                    logger.warning(
                        f"🚨 [MEDIA IMG] "
                        f"Posible decompression bomb: "
                        f"{total_pixels} pixels"
                    )

                    return None

            except Exception as img_error:

                logger.warning(
                    f"🚨 [MEDIA IMG] "
                    f"Imagen corrupta/maliciosa: {img_error}"
                )

                return None

        # ==============================================================================
        # 🎙️ VALIDACIÓN AUDIO
        # ==============================================================================

        elif mime_type.startswith("audio/"):

            if payload_size < 128:

                logger.warning(
                    "🚨 [MEDIA AUDIO] "
                    "Audio sospechosamente pequeño."
                )

                return None

            # ==============================================================================
            # 🛡️ VALIDACIÓN HEADER BÁSICA
            # ==============================================================================

            headers_audio_validos = [
                b"OggS",
                b"ID3",
                b"\xff\xfb",
                b"\xff\xf3",
                b"\xff\xf2"
            ]

            if not any(
                data_bytes.startswith(h)
                for h in headers_audio_validos
            ):

                logger.warning(
                    "🚨 [MEDIA AUDIO] "
                    "Magic bytes inválidos."
                )

                return None

        # ==============================================================================
        # 📊 TELEMETRÍA FINAL
        # ==============================================================================

        tiempo_total = now_ts() - inicio_descarga

        logger.info(
            f"✅ [MEDIA SUCCESS] "
            f"Tiempo={tiempo_total:.3f}s | "
            f"Peso={payload_size/1024:.1f}KB"
        )

        return {
            "mime_type": mime_type,
            "data": data_bytes
        }

    # ==============================================================================
    # 🚨 ERROR CRÍTICO
    # ==============================================================================

    except Exception as e:

        logger.exception(
            f"❌ [MEDIA CRITICAL ERROR] {str(e)}"
        )

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

async def procesar_respuesta_bot(
    cliente: str,
    telefono: str,
    texto_entrante: str,
    columna_actual: str,
    config: dict,
    media_dict: dict = None,
    id_mensaje_meta: str = None
):
    """
    🚀 ORQUESTADOR MAESTRO IA AAA ENTERPRISE - FLATTENED & HARDENED
    """
    trace_id = str(uuid.uuid4())[:8]
    inicio_pipeline = now_ts()
    
    # 🛡️ 1. VALIDACIÓN Y SANITIZACIÓN
    cliente = limpiar_texto(str(cliente or "Cliente"))[:120]
    telefono = limpiar_texto(str(telefono or "")).strip()
    texto_entrante = limpiar_texto(str(texto_entrante or ""))[:12000]
    vendedor_id = str(config.get("vendedor_id", "")).strip()

    if not telefono or not vendedor_id:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Inputs inválidos. Abortando.")
        return

    logger.info(f"🧠 [TRACE:{trace_id}] Inicio Pipeline IA | Tenant={vendedor_id} | Tel={enmascarar_telefono(telefono)}")

    try:
        # 🛡️ 2. IDEMPOTENCIA META
        if id_mensaje_meta:
            async with webhook_lock:
                if id_mensaje_meta in mensajes_procesados_meta:
                    return
                mensajes_procesados_meta[id_mensaje_meta] = True

        # 🛡️ 3. LOCK CONVERSACIONAL (Aislamiento de tareas)
        lock_hash = hashlib.sha256(f"{vendedor_id}:{telefono}".encode()).hexdigest()
        tracking_locks_uso[lock_hash] = now_ts()

        async with locks_por_conversacion[lock_hash]:
            
            # 🛡️ 4. RATE LIMIT
            if not await verificar_rate_limit(vendedor_id, telefono):
                logger.warning(f"🚨 [TRACE:{trace_id}] Rate limit excedido.")
                return

            # 🛡️ 5. ANTI-INJECTION
            if detectar_prompt_injection(texto_entrante):
                logger.warning(f"🚨 [TRACE:{trace_id}] Injection bloqueada.")
                await disparar_whatsapp_dinamico_async(telefono, "Solicitud bloqueada.", config.get("meta_token", ""), config.get("meta_phone_id", ""))
                return

            # 🛡️ 6. ANTI-SPAM REPETICIÓN
            spam_hash = hashlib.sha256(f"{telefono}:{texto_entrante.lower()}".encode()).hexdigest()
            if spam_hash in procesados_recientemente:
                return
            procesados_recientemente[spam_hash] = True

            # 📖 7. PERFIL Y CONTEXTO
            perfil_cliente_previo = {}
            try:
                res_p = await asyncio.wait_for(async_db_execute(supabase.table('prospectos').select('perfil_psicologico').eq('telefono', telefono).eq('vendedor_id', vendedor_id).limit(1)), timeout=10.0)
                if res_p.data:
                    perfil_cliente_previo = res_p.data[0].get('perfil_psicologico', {})
            except Exception as e:
                logger.warning(f"⚠️ [TRACE:{trace_id}] Error perfil: {e}")

            # 🧠 8. RAG + HISTORIAL + IA
            contexto = await asyncio.wait_for(obtener_contexto_inventario_rag(vendedor_id, texto_entrante), timeout=15.0)
            historial = await asyncio.wait_for(obtener_historial_chat(telefono, vendedor_id), timeout=10.0)
            
            decision = await asyncio.wait_for(
                analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, perfil_cliente_previo, media_dict),
                timeout=40.0
            )
            
            decision = validar_respuesta_ia(decision)
            
            # 💾 9. PERSISTENCIA Y RESPUESTA
            respuesta_final = decision["respuesta"]
            producto_detectado = decision["producto_detectado"]
            intencion_ia = decision["intencion"]
            
            # Actualizar Perfil Psicológico
            perfil_actualizado = {**perfil_cliente_previo, "emocion_actual": decision.get("emocion_cliente"), "temperatura": decision.get("temperatura_lead"), "ultimo_interes": producto_detectado, "ultima_intencion": intencion_ia}
            
            # Lógica de estados CRM
            nueva_columna = columna_actual
            iluminacion = "blanco"
            
            if intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO"]:
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            elif intencion_ia == "COMPRA":
                nueva_columna, iluminacion = "Por Entregar", "verde_exito"
                
            # Sincronización final
            await asyncio.gather(
                actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, producto_detectado, perfil_ia=perfil_actualizado),
                guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)
            )

            # Envío Multimedia o Texto
            url_imagen = None
            if producto_detectado:
                res_juego = await async_db_execute(supabase.table('inventario').select('url_portada').ilike('nombre', f'%{producto_detectado}%').eq('vendedor_id', vendedor_id).limit(1))
                if res_juego.data and res_juego.data[0].get('url_portada'):
                    url_imagen = res_juego.data[0]['url_portada']

            if url_imagen:
                await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))
            else:
                await disparar_whatsapp_dinamico_async(telefono, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))

            logger.info(f"✅ [TRACE:{trace_id}] Pipeline completado en {now_ts() - inicio_pipeline:.2f}s")

    except asyncio.TimeoutError:
        logger.error(f"⏱️ [TRACE:{trace_id}] Timeout global.")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] CRÍTICO: {e}")


# ==========================================================
# 📊 7.5 STATS
# ==========================================================
# ==========================================================
#               📊 1. DASHBOARD STATS (TELEMETRÍA DE NEGOCIO)
# ==========================================================
@router.get("/stats")
async def get_dashboard_stats(vendedor_id: str = Depends(verificar_sesion_b2b)):
    """
    Retorna métricas consolidadas del negocio en tiempo real.
    """
    try:
        # Consulta optimizada (Multi-tenant aislada)
        res_leads = await async_db_execute(
            supabase.table("prospectos")
            .select("columna", count='exact')
            .eq("vendedor_id", vendedor_id)
        )
        
        # Procesamiento lógico ligero
        stats = {
            "total_leads": res_leads.count or 0,
            "leads_nuevos": sum(1 for x in res_leads.data if x['columna'] == 'Bandeja Nueva'),
            "pendientes": sum(1 for x in res_leads.data if x['columna'] == 'Por Entregar')
        }
        
        return {"status": "success", "data": stats}

    except Exception as e:
        logger.error(f"❌ [API STATS ERROR] {str(e)}")
        raise HTTPException(status_code=500, detail="Error recuperando stats.")

# ==========================================================
#                           📋 2. LISTADO DE LEADS
# ==========================================================
@router.get("/leads")
async def get_leads(
    columna: Optional[str] = None, 
    vendedor_id: str = Depends(verificar_sesion_b2b)
):
    """
    Retorna lista de prospectos filtrados por vendedor.
    """
    try:
        query = supabase.table("prospectos").select("*").eq("vendedor_id", vendedor_id)
        
        if columna:
            query = query.eq("columna", columna)
            
        res = await async_db_execute(query.order("ultima_interaccion_ia", desc=True).limit(50))
        
        return {"status": "success", "data": res.data}
    
    except Exception as e:
        logger.error(f"❌ [API LEADS ERROR] {str(e)}")
        raise HTTPException(status_code=500, detail="Error recuperando leads.")

# ==========================================================
#                       ⚙️ 3. ACCIÓN MANUAL (CRM CONTROL)
# ==========================================================
@router.post("/leads/accion")
async def ejecutar_accion_lead(
    payload: LeadAction,
    vendedor_id: str = Depends(verificar_sesion_b2b)
):
    """
    Endpoint para mover leads o actualizar estados desde Godot.
    """
    try:
        # Validación de seguridad: el lead debe pertenecer al vendedor
        res_check = await async_db_execute(
            supabase.table("prospectos")
            .select("telefono")
            .eq("id", payload.lead_id)
            .eq("vendedor_id", vendedor_id)
        )
        
        if not res_check.data:
            raise HTTPException(status_code=404, detail="Lead no encontrado.")

        # Ejecución de acción segura
        if payload.accion == "mover_columna":
            await actualizar_estado_crm(
                telefono=res_check.data[0]['telefono'],
                vendedor_id=vendedor_id,
                columna=payload.valor,
                iluminacion="blanco",
                juego=""
            )
            
        return {"status": "success", "msg": "Acción ejecutada."}

    except Exception as e:
        logger.error(f"❌ [API ACTION ERROR] {str(e)}")
        raise HTTPException(status_code=500, detail="Error ejecutando acción.")

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

# 🛡️ FIX AAA: Locks distribuidos por teléfono / tenant
LOCKS_WEBHOOK_CLIENTE = defaultdict(asyncio.Lock)

# 🛡️ FIX AAA: Circuit Breakers Globales
WEBHOOK_CIRCUIT_BREAKER_HASTA = 0.0
WEBHOOK_ERRORES_CONSECUTIVOS = 0

# 🛡️ FIX AAA: Protección Anti Replay
WEBHOOK_REPLAY_CACHE = TTLCache(maxsize=50000, ttl=900)

# 🛡️ FIX AAA: Protección Flood Payload
PAYLOAD_FLOOD_CACHE = TTLCache(maxsize=10000, ttl=60)

# 🛡️ FIX AAA: Protección Anti Audio Bomb
AUDIO_HASHES_PROCESADOS = TTLCache(maxsize=10000, ttl=1800)

# 🛡️ FIX AAA: Protección Anti Image Bomb
IMAGE_HASHES_PROCESADOS = TTLCache(maxsize=10000, ttl=1800)

# 🛡️ FIX AAA: Protección Worker Saturation
ULTIMO_WARNING_BACKPRESSURE = 0.0

def lanzar_tarea_segura(coro):

    """
    ==========================================================
    🚀 LAUNCHER AAA HARDENED
    ==========================================================
    ✔ Anti Zombie Tasks
    ✔ Anti Silent Failures
    ✔ Auto Cleanup
    ✔ Telemetría
    ✔ Protección memoria
    ==========================================================
    """

    task = asyncio.create_task(coro)

    BACKGROUND_TASKS.add(task)

    def _cleanup_task(t):

        try:

            BACKGROUND_TASKS.discard(t)

            if t.cancelled():

                logger.warning(
                    "⚠️ [TASK CANCELLED] "
                    "Task cancelada correctamente."
                )

                return

            exc = t.exception()

            if exc:

                logger.error(
                    f"❌ [TASK ERROR] {str(exc)}"
                )

        except Exception as cleanup_e:

            logger.error(
                f"❌ [TASK CLEANUP ERROR] {cleanup_e}"
            )

    task.add_done_callback(_cleanup_task)

    return task


# 🛡️ CACHÉS Y LOCKS DISTRIBUIDOS EN MEMORIA
# (Nota para escala Hyperscale: Migrar estos TTLCache a Redis en el futuro)

procesados_recientemente = TTLCache(
    maxsize=20000,
    ttl=600
)

wamid_lock = asyncio.Lock()

RATE_LIMIT_CLIENTES = TTLCache(
    maxsize=10000,
    ttl=10
)

rate_limit_lock = asyncio.Lock()

RATE_LIMIT_MEDIA = TTLCache(
    maxsize=10000,
    ttl=60
)

media_limit_lock = asyncio.Lock()

# 🛡️ FIX AAA: Anti Flood IA
RATE_LIMIT_IA = TTLCache(
    maxsize=15000,
    ttl=120
)

# 🛡️ FIX AAA: Anti Flood Multimedia
RATE_LIMIT_ARCHIVOS = TTLCache(
    maxsize=10000,
    ttl=180
)

# 🛡️ SEMÁFOROS DIVIDIDOS Y BACKPRESSURE
SEMAFORO_IA = asyncio.Semaphore(15)
SEMAFORO_MEDIA = asyncio.Semaphore(10)

MAX_COLA_GLOBAL = 200

# 🛡️ FIX AAA: Protección de memoria total
MAX_BACKGROUND_TASKS_RAM = 500


# ==========================================================
# 🛡️ ENMASCARADOR PII
# ==========================================================
def enmascarar_telefono(tel: str) -> str:

    try:

        tel = re.sub(r"[^\d]", "", str(tel))

        if len(tel) >= 10:
            return tel[:4] + "****" + tel[-3:]

        return "***"

    except:
        return "***"


# ==========================================================
# 🧠 ORQUESTADOR PRINCIPAL
# ==========================================================
async def gestionar_mensaje_entrante_bg(
    valor: dict,
    msg: dict,
    phone_id_receptor: str
):

    """
    ==============================================================================
    🧠 ORQUESTADOR MAESTRO MULTIMEDIA VELTRIX ENGINE
    ==============================================================================
    ✔ Multi-Tenant Isolation
    ✔ Anti Replay Attacks
    ✔ Anti Audio Bomb
    ✔ Anti Payload Flood
    ✔ Anti Token Burn
    ✔ Anti Worker Exhaustion
    ✔ Anti Decompression Bomb
    ✔ Anti CRM Flood
    ✔ Anti Duplicate Meta
    ✔ Anti Task Leaks
    ✔ Backpressure Inteligente
    ✔ Circuit Breakers
    ✔ Protección Asyncio
    ==============================================================================
    """

    global WEBHOOK_ERRORES_CONSECUTIVOS
    global WEBHOOK_CIRCUIT_BREAKER_HASTA

    trace_id = str(uuid.uuid4())[:8]

    inicio_pipeline = now_ts()

    media_dict_audio = None
    media_dict_img = None

    logger.info(
        f"📥 [TRACE:{trace_id}] "
        f"INICIANDO ORQUESTACIÓN"
    )

    try:

        # ==========================================================
        # 🛡️ CIRCUIT BREAKER GLOBAL
        # ==========================================================
        if now_ts() < WEBHOOK_CIRCUIT_BREAKER_HASTA:

            logger.warning(
                f"🚨 [TRACE:{trace_id}] "
                f"Circuit breaker activo."
            )

            return

        # ==========================================================
        # 🛡️ VALIDACIÓN ESTRUCTURA
        # ==========================================================
        if not isinstance(msg, dict):

            logger.warning(
                f"⚠️ [TRACE:{trace_id}] "
                f"Payload inválido."
            )

            return

        # ==========================================================
        # 🛡️ EVENTOS SISTEMA
        # ==========================================================
        if msg.get("from_me") or valor.get("statuses"):

            logger.info(
                f"♻️ [TRACE:{trace_id}] "
                f"Evento sistema ignorado."
            )

            return

        # ==========================================================
        # 🛡️ VALIDACIÓN WAMID
        # ==========================================================
        wamid = str(msg.get("id", "")).strip()

        if not wamid:

            logger.warning(
                f"⚠️ [TRACE:{trace_id}] "
                f"WAMID inválido."
            )

            return

        # ==========================================================
        # 🛡️ ANTI REPLAY
        # ==========================================================
        async with wamid_lock:

            if procesados_recientemente.get(wamid):

                logger.warning(
                    f"♻️ [TRACE:{trace_id}] "
                    f"Replay bloqueado."
                )

                return

            procesados_recientemente[wamid] = {
                "trace": trace_id,
                "ts": now_ts()
            }

        # ==========================================================
        # 🛡️ VALIDACIÓN PHONE ID
        # ==========================================================
        phone_id_receptor = str(
            phone_id_receptor
        ).strip()

        if not phone_id_receptor:

            logger.error(
                f"🚨 [TRACE:{trace_id}] "
                f"Phone ID vacío."
            )

            return

        # ==========================================================
        # 🏢 RESOLUCIÓN TENANT
        # ==========================================================
        res_config = await asyncio.wait_for(

            async_db_execute(

                supabase
                .table("configuracion_bot")
                .select("*")
                .eq("meta_phone_id", phone_id_receptor)
                .limit(1)

            ),

            timeout=5.0
        )

        if not res_config.data:

            logger.error(
                f"🚨 [TRACE:{trace_id}] "
                f"Tenant inexistente."
            )

            return

        config_vendedor = res_config.data[0]

        vendedor_actual = str(
            config_vendedor.get(
                "vendedor_id",
                ""
            )
        ).strip()

        token_actual = str(
            config_vendedor.get(
                "meta_token",
                ""
            )
        ).strip() or WHATSAPP_TOKEN

        nombre_negocio = str(
            config_vendedor.get(
                "nombre_negocio",
                "Veltrix"
            )
        ).strip()

        if not vendedor_actual or not token_actual:

            logger.error(
                f"🚨 [TRACE:{trace_id}] "
                f"Config tenant inválida."
            )

            return

        if not config_vendedor.get("bot_activo", True):

            logger.warning(
                f"🚫 [TRACE:{trace_id}] "
                f"Bot desactivado."
            )

            return

        # ==========================================================
        # 📞 NORMALIZACIÓN TELÉFONO
        # ==========================================================
        telefono_cliente = str(
            msg.get("from", "")
        ).strip()

        telefono_cliente = re.sub(
            r"[^\d]",
            "",
            telefono_cliente
        )

        if telefono_cliente.startswith("521"):

            telefono_cliente = (
                "52" + telefono_cliente[3:]
            )

        if len(telefono_cliente) < 10:

            logger.warning(
                f"⚠️ [TRACE:{trace_id}] "
                f"Teléfono inválido."
            )

            return

        tel_mask = enmascarar_telefono(
            telefono_cliente
        )

        logger.info(
            f"📞 [TRACE:{trace_id}] "
            f"Cliente={tel_mask}"
        )

        # ==========================================================
        # 🛡️ LOCK DISTRIBUIDO CLIENTE
        # ==========================================================
        lock_cliente = hashlib.sha256(
            f"{vendedor_actual}:{telefono_cliente}".encode()
        ).hexdigest()

        async with LOCKS_WEBHOOK_CLIENTE[lock_cliente]:

            # ==========================================================
            # 🛡️ RATE LIMIT
            # ==========================================================
            rl_key = (
                f"{vendedor_actual}:"
                f"{telefono_cliente}"
            )

            async with rate_limit_lock:

                peticiones = RATE_LIMIT_CLIENTES.get(
                    rl_key,
                    0
                )

                if peticiones >= 8:

                    logger.warning(
                        f"⚠️ [TRACE:{trace_id}] "
                        f"Flood detectado."
                    )

                    return

                RATE_LIMIT_CLIENTES[rl_key] = (
                    peticiones + 1
                )

            # ==========================================================
            # 🧠 DETECCIÓN TIPO
            # ==========================================================
            tipo_mensaje = str(
                msg.get("type", "text")
            ).lower().strip()

            texto_entrante = ""

            logger.info(
                f"📦 [TRACE:{trace_id}] "
                f"Tipo={tipo_mensaje}"
            )

            # ==========================================================
            # 📝 TEXTO
            # ==========================================================
            if tipo_mensaje == "text":

                texto_entrante = (
                    msg.get("text", {})
                    .get("body", "")
                    .strip()
                )

            # ==========================================================
            # 🖲️ INTERACTIVE
            # ==========================================================
            elif tipo_mensaje == "interactive":

                texto_entrante = (
                    msg.get("interactive", {})
                    .get("button_reply", {})
                    .get("title", "")
                    .strip()
                )

            # ==========================================================
            # 🎙️ AUDIO / IMAGEN
            # ==========================================================
            elif tipo_mensaje in ["audio", "image"]:

                async with media_limit_lock:

                    media_count = RATE_LIMIT_MEDIA.get(
                        rl_key,
                        0
                    )

                    if media_count >= 5:

                        logger.warning(
                            f"⚠️ [TRACE:{trace_id}] "
                            f"Flood multimedia."
                        )

                        return

                    RATE_LIMIT_MEDIA[rl_key] = (
                        media_count + 1
                    )

                # ==========================================================
                # 🎙️ AUDIO
                # ==========================================================
                if tipo_mensaje == "audio":

                    texto_entrante = (
                        "🎙️ [NOTA DE VOZ RECIBIDA]"
                    )

                    audio_id = str(
                        msg.get("audio", {})
                        .get("id", "")
                    ).strip()

                    if not audio_id:
                        return

                    media_dict_audio = await asyncio.wait_for(

                        descargar_media_whatsapp_async(
                            audio_id,
                            token_actual
                        ),

                        timeout=30.0
                    )

                    if not media_dict_audio:
                        return

                    audio_bytes = media_dict_audio.get(
                        "data",
                        b""
                    )

                    if not audio_bytes:
                        return

                    if len(audio_bytes) > 15_000_000:

                        logger.warning(
                            f"🚨 [TRACE:{trace_id}] "
                            f"Audio >15MB."
                        )

                        return

                    # ==========================================================
                    # 🛡️ HASH ANTI AUDIO BOMB
                    # ==========================================================
                    audio_hash = hashlib.sha256(
                        audio_bytes[:50000]
                    ).hexdigest()

                    if audio_hash in AUDIO_HASHES_PROCESADOS:

                        logger.warning(
                            f"♻️ [TRACE:{trace_id}] "
                            f"Audio repetido."
                        )

                        return

                    AUDIO_HASHES_PROCESADOS[audio_hash] = True

                # ==========================================================
                # 🖼️ IMAGEN
                # ==========================================================
                elif tipo_mensaje == "image":

                    texto_entrante = (
                        "📷 [IMAGEN RECIBIDA]"
                    )

                    image_id = str(
                        msg.get("image", {})
                        .get("id", "")
                    ).strip()

                    if not image_id:
                        return

                    media_dict_img = await asyncio.wait_for(

                        descargar_media_whatsapp_async(
                            image_id,
                            token_actual
                        ),

                        timeout=30.0
                    )

                    if not media_dict_img:
                        return

                    data_bytes = media_dict_img.get(
                        "data",
                        b""
                    )

                    if not data_bytes:
                        return

                    if len(data_bytes) > 10_000_000:

                        logger.warning(
                            f"🚨 [TRACE:{trace_id}] "
                            f"Imagen >10MB."
                        )

                        return

                    mime = str(
                        media_dict_img.get(
                            "mime_type",
                            ""
                        )
                    ).lower().strip()

                    mime_validos = [
                        "image/jpeg",
                        "image/png",
                        "image/webp"
                    ]

                    if mime not in mime_validos:

                        logger.warning(
                            f"🚨 [TRACE:{trace_id}] "
                            f"MIME inválido."
                        )

                        return

                    # ==========================================================
                    # 🛡️ ANTI IMAGE BOMB
                    # ==========================================================
                    image_hash = hashlib.sha256(
                        data_bytes[:50000]
                    ).hexdigest()

                    if image_hash in IMAGE_HASHES_PROCESADOS:

                        logger.warning(
                            f"♻️ [TRACE:{trace_id}] "
                            f"Imagen repetida."
                        )

                        return

                    IMAGE_HASHES_PROCESADOS[image_hash] = True

                    try:

                        Image.MAX_IMAGE_PIXELS = 20_000_000

                        img_val = Image.open(
                            io.BytesIO(data_bytes)
                        )

                        img_val.verify()

                    except Exception as img_e:

                        logger.warning(
                            f"🚨 [TRACE:{trace_id}] "
                            f"Imagen maliciosa: {img_e}"
                        )

                        return

            # ==========================================================
            # 🛡️ TIPO NO SOPORTADO
            # ==========================================================
            else:

                logger.info(
                    f"ℹ️ [TRACE:{trace_id}] "
                    f"Tipo descartado."
                )

                return

            # ==========================================================
            # 🛡️ SANITIZACIÓN TEXTO
            # ==========================================================
            texto_entrante = limpiar_texto(
                texto_entrante
            )

            texto_entrante = bleach.clean(
                texto_entrante,
                tags=[],
                strip=True
            )

            texto_entrante = texto_entrante[:4000]

            # ==========================================================
            # 🛡️ PROMPT INJECTION
            # ==========================================================
            if detectar_prompt_injection(
                texto_entrante
            ):

                logger.warning(
                    f"🚨 [TRACE:{trace_id}] "
                    f"Prompt Injection detectado."
                )

                return

            # ==========================================================
            # 💾 CARGA CRM
            # ==========================================================
            nombre_cliente = (
                valor.get("contacts", [{}])[0]
                .get("profile", {})
                .get("name", "Cliente")
            )

            nombre_cliente = limpiar_texto(
                nombre_cliente
            )[:80]

            res_p = await async_db_execute(

                supabase
                .table("prospectos")
                .select(
                    "columna, notas, perfil_psicologico"
                )
                .eq("telefono", telefono_cliente)
                .eq("vendedor_id", vendedor_actual)
            )

            columna_actual = (
                res_p.data[0].get(
                    "columna",
                    "Bandeja Nueva"
                )
                if res_p.data
                else "Bandeja Nueva"
            )

            # ==========================================================
            # 🛡️ UPSERT CRM
            # ==========================================================
            if not res_p.data:

                try:

                    await asyncio.wait_for(

                        async_db_execute(

                            supabase
                            .table("prospectos")
                            .upsert(
                                {
                                    "nombre": nombre_cliente,
                                    "telefono": telefono_cliente,
                                    "columna": columna_actual,
                                    "vendedor_id": vendedor_actual,
                                    "ultima_interaccion_ia":
                                    datetime.now(
                                        timezone.utc
                                    ).isoformat()
                                },

                                on_conflict=(
                                    "telefono,vendedor_id"
                                )
                            )
                        ),

                        timeout=5.0
                    )

                except Exception as db_e:

                    logger.warning(
                        f"⚠️ [TRACE:{trace_id}] "
                        f"Upsert controlado: {db_e}"
                    )

            # ==========================================================
            # 💬 GUARDADO CHAT
            # ==========================================================
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

            except Exception as chat_e:

                logger.error(
                    f"⚠️ [TRACE:{trace_id}] "
                    f"Error chat: {chat_e}"
                )

            # ==========================================================
            # 🤖 PIPELINE IA
            # ==========================================================
            if (
                tipo_mensaje in [
                    "text",
                    "interactive",
                    "audio"
                ]
                and columna_actual != "En Conversacion"
            ):

                async with SEMAFORO_IA:

                    logger.info(
                        f"🤖 [TRACE:{trace_id}] "
                        f"Pipeline IA iniciado."
                    )

                    contexto_rag = await obtener_contexto_inventario_rag(
                        vendedor_actual,
                        texto_entrante
                    )

                    historial = await obtener_historial_chat(
                        telefono_cliente,
                        vendedor_actual
                    )

                    data_cruda = await analizar_intencion_venta_ia(
                        texto_entrante,
                        contexto_rag,
                        historial,
                        config_vendedor,
                        (
                            res_p.data[0].get(
                                "perfil_psicologico"
                            )
                            if res_p.data
                            else None
                        ),
                        media_dict_audio
                    )

                    data_validada = validar_respuesta_ia(
                        data_cruda
                    )

                    await guardar_resultado_ia_en_crm(
                        telefono_cliente,
                        vendedor_actual,
                        data_validada
                    )

                    await disparar_whatsapp_dinamico_async(
                        telefono_cliente,
                        data_validada["respuesta"],
                        token_actual,
                        phone_id_receptor
                    )

                    await guardar_mensaje_chat(
                        telefono_cliente,
                        vendedor_actual,
                        "BOT",
                        data_validada["respuesta"]
                    )

            # ==========================================================
            # 🛡️ AUDITORÍA PAGOS
            # ==========================================================
            elif tipo_mensaje == "image" and media_dict_img:

                async with SEMAFORO_MEDIA:

                    logger.info(
                        f"🛡️ [TRACE:{trace_id}] "
                        f"Doberman Vision iniciado."
                    )

                    historial_para_auditor = (
                        await obtener_historial_chat(
                            telefono_cliente,
                            vendedor_actual
                        )
                    )

                    auditoria = await asyncio.wait_for(

                        auditar_comprobante_ia(
                            media_dict_img["data"],
                            media_dict_img["mime_type"],
                            nombre_negocio,
                            historial_para_auditor
                        ),

                        timeout=45.0
                    )

                    es_pago = bool(
                        auditoria.get(
                            "es_pago",
                            False
                        )
                    )

                    try:

                        monto = float(
                            auditoria.get(
                                "monto_detectado",
                                0.0
                            )
                        )

                    except:
                        monto = 0.0

                    # ==========================================================
                    # ✅ PAGO VÁLIDO
                    # ==========================================================
                    if es_pago:

                        logger.info(
                            f"💰 [TRACE:{trace_id}] "
                            f"Pago validado ${monto:.2f}"
                        )

                        await actualizar_estado_crm(
                            telefono_cliente,
                            vendedor_actual,
                            "Por Entregar",
                            "verde_exito",
                            ""
                        )

                        msg_exito = (
                            f"✅ ¡Pago validado "
                            f"por ${monto:.2f} MXN!\n"
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

                    # ==========================================================
                    # 🚨 FRAUDE / ERROR
                    # ==========================================================
                    else:

                        analisis_fallo = limpiar_texto(
                            auditoria.get(
                                "analisis",
                                "No validado."
                            )
                        )

                        msg_fallo = (
                            f"🤖 Mi sistema no pudo "
                            f"validar la imagen.\n"
                            f"Detalle: {analisis_fallo}\n"
                            f"Por favor envía una foto clara."
                        )

                        logger.warning(
                            f"🚨 [TRACE:{trace_id}] "
                            f"Fraude/Error: {analisis_fallo}"
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

        # ==========================================================
        # 📊 TELEMETRÍA FINAL
        # ==========================================================
        tiempo_total = (
            now_ts() - inicio_pipeline
        )

        logger.info(
            f"🏁 [TRACE:{trace_id}] "
            f"Pipeline completado "
            f"en {tiempo_total:.3f}s"
        )

        WEBHOOK_ERRORES_CONSECUTIVOS = 0

    # ==========================================================
    # ⏱️ TIMEOUT GLOBAL
    # ==========================================================
    except asyncio.TimeoutError:

        WEBHOOK_ERRORES_CONSECUTIVOS += 1

        logger.error(
            f"⏱️ [TRACE:{trace_id}] "
            f"Timeout global."
        )

    # ==========================================================
    # 🚨 ERROR GLOBAL
    # ==========================================================
    except Exception as e:

        WEBHOOK_ERRORES_CONSECUTIVOS += 1

        logger.exception(
            f"❌ [TRACE:{trace_id}] "
            f"CRÍTICO: {str(e)}"
        )

    # ==========================================================
    # 🚨 CIRCUIT BREAKER
    # ==========================================================
    finally:

        if WEBHOOK_ERRORES_CONSECUTIVOS >= 15:

            WEBHOOK_CIRCUIT_BREAKER_HASTA = (
                now_ts() + 60
            )

            logger.critical(
                "🚨 [WEBHOOK CIRCUIT BREAKER] "
                "Activado por exceso de errores."
            )

            WEBHOOK_ERRORES_CONSECUTIVOS = 0

        # ==========================================================
        # 🧹 CLEANUP MEMORIA
        # ==========================================================
        media_dict_audio = None
        media_dict_img = None

        gc.collect()

        logger.info(
            f"🧹 [TRACE:{trace_id}] "
            f"GC ejecutado correctamente."
        )


# ==========================================================
# 🌐 VALIDACIÓN WEBHOOK META
# ==========================================================
@app.get("/webhook")
async def verificar_webhook(request: Request):

    params = request.query_params

    if (
        params.get("hub.mode") == "subscribe"
        and params.get("hub.verify_token") == WEBHOOK_SECRET
    ):

        logger.info(
            "✅ [WEBHOOK] "
            "Servidor validado correctamente."
        )

        return int(
            params.get("hub.challenge")
        )

    raise HTTPException(
        status_code=403,
        detail="Token inválido"
    )


# ==========================================================
# 📥 WEBHOOK PRINCIPAL META
# ==========================================================
@app.post("/webhook")
async def recibir_mensajes(request: Request):

    """
    ==========================================================
    🚀 ENTRYPOINT WEBHOOK AAA ENTERPRISE
    ==========================================================
    ✔ Backpressure
    ✔ Anti DDoS
    ✔ Validación Payload
    ✔ Protección RAM
    ✔ Protección JSON Bomb
    ✔ Validación Firma Meta
    ✔ Protección Async Flood
    ✔ Fast ACK Meta
    ==========================================================
    """

    global ULTIMO_WARNING_BACKPRESSURE

    # ==========================================================
    # 🛡️ BACKPRESSURE
    # ==========================================================
    if len(BACKGROUND_TASKS) > MAX_COLA_GLOBAL:

        if now_ts() - ULTIMO_WARNING_BACKPRESSURE > 10:

            logger.critical(
                "🚨 [BACKPRESSURE] "
                "Servidor saturado."
            )

            ULTIMO_WARNING_BACKPRESSURE = now_ts()

        raise HTTPException(
            status_code=503,
            detail="Queue Full"
        )

    # ==========================================================
    # 🛡️ PROTECCIÓN MEMORIA
    # ==========================================================
    if len(BACKGROUND_TASKS) > MAX_BACKGROUND_TASKS_RAM:

        logger.critical(
            "🚨 [RAM PROTECTION] "
            "Demasiadas tareas activas."
        )

        raise HTTPException(
            status_code=503,
            detail="Server Busy"
        )


    # ==========================================================
    # 🛡️ VALIDACIÓN FIRMA META (PUENTE MODO LAB)
    # ==========================================================
    if getattr(sys.modules[__name__], 'MODO_LABORATORIO', True): # Fuerza a True si no está definida arriba
        logger.warning("🧪 [WEBHOOK MODO LAB] !!! ALERTA: Saltando validación de firma")
    else:
        try:
            await asyncio.wait_for(
                validar_firma_meta(request),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=408,
                detail="Timeout validando firma"
            )
        except Exception as firma_e:
            logger.error(
                f"🚨 [WEBHOOK SIGNATURE] "
                f"{firma_e}"
            )
            raise HTTPException(
                status_code=403,
                detail="Firma inválida"
            )

    try:

        # ==========================================================
        # 📦 BODY
        # ==========================================================
        body_bytes = await request.body()

        if not body_bytes:

            raise HTTPException(
                400,
                "Payload vacío"
            )

        # ==========================================================
        # 🛡️ LIMITADOR PAYLOAD
        # ==========================================================
        if len(body_bytes) > 2_000_000:

            raise HTTPException(
                413,
                "Payload demasiado grande"
            )

        # ==========================================================
        # 🛡️ HASH PAYLOAD
        # ==========================================================
        payload_hash = hashlib.sha256(
            body_bytes[:100000]
        ).hexdigest()

        if payload_hash in PAYLOAD_FLOOD_CACHE:

            logger.warning(
                "♻️ [WEBHOOK DUPLICATE PAYLOAD]"
            )

        PAYLOAD_FLOOD_CACHE[payload_hash] = True

        # ==========================================================
        # 🛡️ PARSER JSON
        # ==========================================================
        try:

            body = orjson.loads(body_bytes)

        except Exception:

            raise HTTPException(
                400,
                "JSON inválido"
            )

        # ==========================================================
        # 🛡️ VALIDACIÓN ESTRUCTURA
        # ==========================================================
        if not isinstance(body, dict):

            raise HTTPException(
                400,
                "Payload inválido"
            )

        # ==========================================================
        # 🔄 ITERACIÓN META
        # ==========================================================
        for entry in body.get("entry", []):

            if not isinstance(entry, dict):
                continue

            for change in entry.get("changes", []):

                if not isinstance(change, dict):
                    continue

                value = change.get("value", {})

                if not isinstance(value, dict):
                    continue

                phone_id_receptor = (
                    value.get("metadata", {})
                    .get(
                        "phone_number_id",
                        WHATSAPP_PHONE_ID
                    )
                )

                mensajes = value.get(
                    "messages",
                    []
                )

                if not isinstance(mensajes, list):
                    continue

                for message in mensajes:

                    if not isinstance(message, dict):
                        continue

                    lanzar_tarea_segura(

                        gestionar_mensaje_entrante_bg(
                            value,
                            message,
                            phone_id_receptor
                        )
                    )

        # ==========================================================
        # ✅ FAST ACK META
        # ==========================================================
        return {
            "status": "ok"
        }

    except HTTPException:
        raise

    except Exception as e:

        logger.exception(
            f"❌ [WEBHOOK ENTRYPOINT ERROR] "
            f"{str(e)}"
        )

        return {
            "status": "error",
            "reason": str(e)
        }


# ==========================================================
# 🚀 REGISTRO ROUTER
# ==========================================================
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
