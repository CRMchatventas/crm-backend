# ==============================================================================
# 🚀 MÓDULO: config_and_schemas.py (AAA ENTERPRISE - DEFINITIVO v2.8)
# ==============================================================================
# Ecosistema Global Veltrix Engine • Única Fuente de Verdad Concurrente
# refactorizado ok
# ==============================================================================

import os
import time
import logging
import asyncio
import httpx
import re
from typing import Dict, Any, Optional, Set, TypedDict, Final
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator, EmailStr, HttpUrl, ConfigDict
from cachetools import TTLCache
from collections import deque
from passlib.context import CryptContext
from supabase import create_client, Client

# ==============================================================================
# 🛠️ DIAGNÓSTICO Y CARGA DE ENTORNO
# ==============================================================================
load_dotenv()

print("\n" + "="*50)
print("🚀 [VELTRIX ENGINE] INICIALIZANDO CORE CONFIG")
print("="*50)
print(f"[*] ¿Archivo .env detectado?: {'Sí' if os.path.exists('.env') else 'No (Usando variables de entorno del SO)'}")

# --- ESTRUCTURAS DE DATOS (ENTERPRISE) ---
class MetricasRadar(TypedDict):
    cache_hits: int
    cache_miss: int
    scraper_ok: int
    scraper_fail: int
    ultimo_reinicio: float

class CacheDivisa(TypedDict):
    valor: float
    expira: float

class CircuitBreakerPriceCharting(TypedDict):
    fallas: int
    bloqueado_hasta: float

# ==============================================================
# 🔌 INICIALIZACIÓN CRÍTICA DE BASE DE DATOS (FAIL-FAST)
# ==============================================================
SUPABASE_URL: Final = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY: Final = os.getenv("SUPABASE_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    print("🚨 [FATAL] Variables de Supabase ausentes o vacías.")
    raise RuntimeError("❌ FATAL: SUPABASE_URL o SUPABASE_KEY no configuradas en el entorno.")

# FIX FASE 2: Cliente Global único para inyección segura. 
# Se elimina cualquier otra instanciación de Supabase en el sistema.
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("[*] Cliente global de Supabase instanciado con éxito.")

# ==============================================================================
# 🔐 CONSTANTES Y SEGURIDAD UNIFICADA
# ==============================================================================
SCHEMA_VERSION: Final = "20.2.AAA"
MODO_LABORATORIO: Final = os.getenv("MODO_LABORATORIO", "false").lower() == "true"
print(f"[*] Modo Laboratorio: {'ACTIVADO (Cuidado en Prod)' if MODO_LABORATORIO else 'DESACTIVADO (Producción)'}")

JWT_SECRET: Final = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("❌ FATAL: JWT_SECRET no configurada en el entorno.")

# Unificación estricta de variables de Webhook para evitar duplicidad
WEBHOOK_SECRET: Final = os.getenv("META_WEBHOOK_SECRET", os.getenv("WEBHOOK_SECRET", "")).strip()
if not WEBHOOK_SECRET:
    raise RuntimeError("❌ FATAL: META_WEBHOOK_SECRET (o WEBHOOK_SECRET) no configurada.")

META_APP_SECRET: Final = os.getenv("META_APP_SECRET", "").strip()
if not META_APP_SECRET:
    raise RuntimeError("❌ FATAL: META_APP_SECRET no configurada.")
print("[*] Secretos core verificados y cargados con éxito.")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
DUMMY_HASH: Final = "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewK.Yj7bQoQfK10C"

MAX_MENSAJE_LEN: Final = 1200
CACHE_TTL_SECONDS: Final = 300 
MAX_REQUESTS_GLOBAL_MINUTO: Final = 250
HTTP_TIMEOUTS: Final = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

logger = logging.getLogger("VeltrixEngine.Config")
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==============================================================
# 🛠️ UTILERÍAS GLOBALES Y SANITIZACIÓN DE CAMPOS
# ==============================================================
def now_ts() -> float:
    return time.time()

def limpiar_texto(texto: str) -> str:
    if not texto: return ""
    texto = re.sub(r'<[^>]+>', '', str(texto))
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()

def sanitizar_nombre_columna(columna: str, permitir_reservadas: bool = False) -> str:
    if not columna: 
        return "Bandeja Nueva"
    
    limpio = re.sub(r"[^\w\s\-]", "", str(columna)).strip()
    
    if not permitir_reservadas:
        reservadas = {"requiere asistencia", "por entregar", "bandeja nueva", "envios masivos", "null", "undefined", "delete"}
        if limpio.lower() in reservadas:
            logger.warning(f"🛡️ [SANITIZER] Intento de asignación a columna reservada bloqueado: '{limpio}'")
            return "Bandeja Nueva"
            
    return limpio if limpio else "Bandeja Nueva"

class BaseSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

# ==============================================================================
# 🧠 ÚNICA FUENTE DE VERDAD: ESTADO GLOBAL (CACHES Y RATE LIMITS)
# ==============================================================================
print("[*] Asignando memoria para TTLCaches centralizados...")

cache_respuestas_ia = TTLCache(maxsize=5000, ttl=60) 
CHAT_MESSAGE_HASHES = TTLCache(maxsize=100000, ttl=86400)
cache_precios_ram: TTLCache = TTLCache(maxsize=50000, ttl=86400)
PAYLOAD_FLOOD_CACHE: TTLCache = TTLCache(maxsize=10000, ttl=CACHE_TTL_SECONDS)
IMAGE_HASHES_PROCESADOS: TTLCache = TTLCache(maxsize=10000, ttl=1800)
tokens_consumidos_tenant: TTLCache = TTLCache(maxsize=10000, ttl=CACHE_TTL_SECONDS)
procesados_recientemente: TTLCache = TTLCache(maxsize=50000, ttl=600)
mensajes_procesados_meta: TTLCache = TTLCache(maxsize=50000, ttl=3600)

rate_limit_tenant: TTLCache = TTLCache(maxsize=50000, ttl=120)
rate_limit_phone: TTLCache = TTLCache(maxsize=100000, ttl=120)
LOGIN_RATE_LIMIT: TTLCache = TTLCache(maxsize=10000, ttl=300)
RATE_LIMIT_MOBILE_OUTBOUND: TTLCache = TTLCache(maxsize=10000, ttl=60)
RATE_LIMIT_CLIENTES: TTLCache = TTLCache(maxsize=10000, ttl=10)
WEBHOOK_REPLAY_CACHE: TTLCache = TTLCache(maxsize=50000, ttl=900)

rate_limit_global: deque[float] = deque(maxlen=max(100, MAX_REQUESTS_GLOBAL_MINUTO))
print("[*] Estructuras en memoria RAM inicializadas y blindadas.")

# ==============================================================================
# 🚦 GESTIÓN DE CONCURRENCIA, LOCKS Y BACKGROUND TASKS
# ==============================================================================
# FIX FASE 2: Global Cache Lock para hacer las operaciones de TTLCache thread-safe
global_cache_lock = asyncio.Lock()

rate_limit_global_lock = asyncio.Lock() 
metricas_lock = asyncio.Lock()
locks_registry_lock = asyncio.Lock()
background_tasks_lock = asyncio.Lock()

metricas_radar: MetricasRadar = {"cache_hits": 0, "cache_miss": 0, "scraper_ok": 0, "scraper_fail": 0, "ultimo_reinicio": time.time()}
CACHE_DIVISA: CacheDivisa = {"valor": 18.0, "expira": 0.0}
CB_PRICECHARTING: CircuitBreakerPriceCharting = {"fallas": 0, "bloqueado_hasta": 0.0}

locks_por_conversacion: Dict[str, asyncio.Lock] = {}
tracking_locks_uso: Dict[str, float] = {}
background_tasks_activas: Set[asyncio.Task] = set()

async def resetear_metricas_radar_si_necesario():
    async with metricas_lock:
        if time.time() - metricas_radar["ultimo_reinicio"] > 86400:
            metricas_radar.update({"cache_hits": 0, "cache_miss": 0, "scraper_ok": 0, "scraper_fail": 0, "ultimo_reinicio": time.time()})
            logger.info("🔄 [METRICAS] Métricas diarias reseteadas.")

async def get_lock(key: str) -> asyncio.Lock:
    """Obtiene o crea un lock específico para una conversación asegurando concurrencia estricta."""
    async with locks_registry_lock:
        if key not in locks_por_conversacion:
            locks_por_conversacion[key] = asyncio.Lock()
        tracking_locks_uso[key] = time.time()
        return locks_por_conversacion[key]

async def registrar_uso_lock(key: str):
    async with locks_registry_lock:
        tracking_locks_uso[key] = time.time()

async def registrar_background_task(task: asyncio.Task):
    async with background_tasks_lock:
        background_tasks_activas.add(task)
        
    def _task_done_callback(t: asyncio.Task):
        background_tasks_activas.discard(t)
        try:
            exc = t.exception()
            if exc:
                logger.exception(f"❌ Background Task Error: {exc}")
        except asyncio.CancelledError:
            pass
            
    task.add_done_callback(_task_done_callback)

async def limpiar_locks_inactivos(threshold: float = 3600.0):
    async with locks_registry_lock:
        ahora = time.time()
        a_borrar = [k for k, v in tracking_locks_uso.items() if ahora - v > threshold]
        for k in a_borrar:
            lock = locks_por_conversacion.get(k)
            if lock and not lock.locked():
                locks_por_conversacion.pop(k, None)
                tracking_locks_uso.pop(k, None)

async def task_gc_locks():
    """Garbage Collector: Mantiene la RAM limpia liberando locks inactivos."""
    while True:
        try:
            await limpiar_locks_inactivos()
            await resetear_metricas_radar_si_necesario()
        except asyncio.CancelledError:
            logger.info("🛑 [GC] Garbage Collector detenido.")
            raise
        except Exception:
            logger.exception("❌ [GC ERROR] Fallo en limpieza de memoria.")
        # FIX FASE 2: GC optimizado a 5 minutos (300s) para evitar Memory Leaks bajo carga
        await asyncio.sleep(300) 

print("[*] Gestor de concurrencia y Garbage Collector listos.\n" + "="*50 + "\n")

# ==============================================================================
# 🛡️ VALIDACIONES LOCALES
# ==============================================================================
def _local_validar_tel(telefono: Any) -> str:
    if telefono in (None, ""): return ""
    limpio = re.sub(r"[^\d]", "", str(telefono))
    if not limpio: return ""
    if limpio.startswith("521"): limpio = "52" + limpio[3:]
    if len(limpio) < 10 or len(limpio) > 15: raise ValueError("Teléfono inválido.")
    if len(set(limpio)) == 1: raise ValueError("Teléfono inválido.")
    return limpio

# ==============================================================================
# 📦 SCHEMAS DE DATOS (PYDANTIC)
# ==============================================================================

class InventarioItem(BaseSchema):
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
    url_portada: Optional[str] = None
    estado_general: str = "Bueno"
    descripcion_detallada: str = ""
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("nombre", mode="before")
    @classmethod
    def v_n(cls, v: Any): 
        v_s = str(v).strip() if v else ""
        if not v_s: raise ValueError("Nombre vacío.")
        return v_s

class InventarioItemUpdate(BaseSchema):
    id: int = Field(gt=0)
    nombre: Optional[str] = None
    consola: Optional[str] = None
    categoria: Optional[str] = None
    precio: Optional[float] = Field(default=None, ge=0)
    stock: Optional[int] = Field(default=None, ge=0)

    @field_validator("nombre")
    @classmethod
    def v_n_u(cls, v):
        if v is not None and not v.strip(): raise ValueError("Nombre vacío.")
        return v

    @model_validator(mode="after")
    def validar_cambios(self):
        if all(getattr(self, c) is None for c in ["nombre", "consola", "categoria", "precio", "stock"]):
            raise ValueError("Update vacío.")
        return self

class VentaItem(BaseSchema): 
    model_config = ConfigDict(populate_by_name=True)
    nombre_producto: str = Field(alias="nombre", min_length=1) 
    id: Optional[int] = None 
    estado_general: str = ""
    nuevo_stock: Optional[int] = None      
    cantidad_vendida: Optional[int] = Field(default=None, ge=0) 
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("nombre_producto", mode="before")
    @classmethod
    def v_np(cls, v: Any): return str(v).strip() if v else v

class LoginUpdate(BaseSchema): 
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    @field_validator("password")
    @classmethod
    def v_p(cls, v: str):
        v = v.strip()
        if not v or len(v) < 8: raise ValueError("Password inválida.")
        return v

class MobileMessageRequest(BaseSchema): 
    to: str
    msg: str = Field(min_length=1)
    @field_validator("to", mode="before")
    @classmethod
    def v_t(cls, v: Any): return _local_validar_tel(v)

class ClienteIdentificador(BaseSchema): 
    nombre: str = ""
    telefono: str = ""
    @field_validator("telefono", mode="before")
    @classmethod
    def v_t(cls, v: Any): return _local_validar_tel(v)

class ColumnaUpdate(BaseSchema):
    nombre: str = ""
    telefono: str = ""
    columna: str = "" 
    fila: str = ""           
    nueva_fila: str = ""     

class ColumnaAction(BaseSchema):
    nombre: str = Field(min_length=1)
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")

class RenombrarColumnaAction(BaseSchema):
    viejo_nombre: str = Field(min_length=1)
    nuevo_nombre: str = Field(min_length=1)
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")

class NotasUpdate(BaseSchema):
    nombre: str = ""
    telefono: str = ""
    notas: str = ""
    etiquetas: str = ""
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")

class EstadoUpdate(BaseSchema): 
    nombre: str = Field(min_length=1)
    telefono: str = ""
    nueva_fila: str 
    @field_validator("telefono", mode="before")
    @classmethod
    def v_t(cls, v: Any): return _local_validar_tel(v)

class ReordenarColumnasAction(BaseSchema):
    columnas: list[str] = Field(min_length=1)
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")
    @field_validator("columnas")
    @classmethod
    def v_c(cls, v):
        if any(not x.strip() for x in v): raise ValueError("Nombre de columna vacío.")
        return v

class LeadAction(BaseSchema):
    lead_id: str = Field(..., min_length=1, max_length=100)
    accion: str = Field(..., pattern="^(mover_columna|mover_fila|actualizar_notas)$")
    valor: str = Field(..., min_length=1, max_length=100)

class BorrarRequest(BaseSchema):
    nombre: str = Field(min_length=1)
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")

class NuevoArticulo(BaseSchema): 
    nombre: str = Field(min_length=1)
    categoria: str = "General" 
    precio_compra: float = Field(default=0.0, ge=0)
    precio: float = Field(default=0.0, ge=0)
    stock: int = Field(default=1, ge=0)
    vendedor_id: str = Field(default="", pattern=r"^[A-Za-z0-9\-_]{0,50}$")
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)
    @field_validator("nombre", mode="before")
    @classmethod
    def v_n(cls, v): 
        if not str(v).strip(): raise ValueError("Vacio")
        return v

class PreciosDetalle(BaseSchema):
    loose: float = Field(default=0.0, ge=0)
    cib: float = Field(default=0.0, ge=0)
    new: float = Field(default=0.0, ge=0)
    valor_base: float = Field(default=0.0, ge=0) 

class PrecioResponse(BaseSchema):
    status: str
    api_version: str = "v3"  
    nombre_corregido: str
    mxn: PreciosDetalle      
    mxn_mercado: PreciosDetalle
    mxn_venta: PreciosDetalle
    usd: PreciosDetalle
    tipo_cambio: float = Field(ge=5.0, le=50.0)
    url_pc: Optional[str] = None
    confidence_score: float = Field(ge=0.0, le=100.0) 
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

class NuevaCita(BaseSchema):
    cliente_nombre: str = Field(min_length=1, max_length=150)
    cliente_telefono: str
    concepto: str = Field(min_length=1, max_length=500)
    fecha_inicio: datetime
    duracion_min: int = Field(default=30, ge=5, le=1440)
    atributos_extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("cliente_telefono", mode="before")
    @classmethod
    def v_tel(cls, v): return _local_validar_tel(v)

    @field_validator("fecha_inicio")
    @classmethod
    def val_f(cls, v: datetime):
        ahora = datetime.now(timezone.utc)
        v_aware = v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
        if v_aware < ahora - timedelta(minutes=5): raise ValueError("Pasada.")
        return v

class EstadoCita(BaseSchema):
    cita_id: int = Field(gt=0)
    nuevo_estado: str = Field(min_length=1)

class NuevaPublicacion(BaseSchema):
    id_inventario: int = Field(gt=0)
    titulo: str = Field(min_length=1)
    descripcion: str = Field(min_length=1)
    precio: float = Field(ge=0)

class CampanaMasiva(BaseSchema):
    mensaje: str = Field(min_length=1)
    columna_origen: str = Field(min_length=1)

class PeticionCopy(BaseSchema):
    juego: str = Field(min_length=1, max_length=150)
    prompt_interno: str = Field(min_length=1, max_length=5000)

__all__ = [
    "SCHEMA_VERSION", "MODO_LABORATORIO", "JWT_SECRET", "WEBHOOK_SECRET", "META_APP_SECRET", 
    "DUMMY_HASH", "pwd_context", "BaseSchema", "now_ts", "limpiar_texto", "sanitizar_nombre_columna",
    "cache_respuestas_ia", "CHAT_MESSAGE_HASHES", "cache_precios_ram", "supabase",
    "PAYLOAD_FLOOD_CACHE", "IMAGE_HASHES_PROCESADOS", "tokens_consumidos_tenant", 
    "procesados_recientemente", "mensajes_procesados_meta", "rate_limit_tenant", 
    "rate_limit_phone", "LOGIN_RATE_LIMIT", "RATE_LIMIT_MOBILE_OUTBOUND", 
    "RATE_LIMIT_CLIENTES", "rate_limit_global", "rate_limit_global_lock",
    "metricas_radar", "metricas_lock", "CACHE_DIVISA", "CB_PRICECHARTING", "HTTP_TIMEOUTS",
    "locks_por_conversacion", "tracking_locks_uso", "background_tasks_activas",
    "background_tasks_lock", "limpiar_locks_inactivos", "locks_registry_lock", "task_gc_locks",
    "registrar_uso_lock", "registrar_background_task", "get_lock",
    "WEBHOOK_REPLAY_CACHE", "global_cache_lock",
    "InventarioItem", "InventarioItemUpdate", "VentaItem", "LoginUpdate",
    "MobileMessageRequest", "ClienteIdentificador", "ColumnaUpdate",
    "ColumnaAction", "RenombrarColumnaAction", "NotasUpdate", "EstadoUpdate",
    "ReordenarColumnasAction", "LeadAction", "BorrarRequest", "NuevoArticulo",
    "PreciosDetalle", "PrecioResponse", "NuevaCita", "EstadoCita",
    "NuevaPublicacion", "CampanaMasiva", "PeticionCopy"
]