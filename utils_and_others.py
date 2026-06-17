# ==========================================================
# 🚀 MÓDULO: utils_and_others.py
# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V20.2 (AAA ENTERPRISE)
# Utilidades y Estado Global
# ==========================================================

import asyncio
import logging
import httpx
import hashlib
import time
import re
import unicodedata
import json
from typing import Optional, Any

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
from config_and_schemas import (
    JWT_SECRET,
    HTTP_TIMEOUTS,
    CACHE_DIVISA,
    cache_lock,
    cache_precios_ram,
    metricas_radar,
    lock_divisa,
    background_tasks_activas,
)

# ==============================================================================
# 🛡️ VALIDACIONES GLOBALES DE SEGURIDAD
# ==============================================================================
if not JWT_SECRET: 
    raise RuntimeError("❌ FATAL: JWT_SECRET no configurada en el entorno.")

# 🛡️ Logging Estructurado
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VeltrixEngine.Utils")

# ==============================================================================
# 🌐 RECURSOS COMPARTIDOS Y ESTADOS
# ==============================================================================
http_client: Optional[httpx.AsyncClient] = None

# ==========================================================
# 🛠️ UTILIDADES CORE AAA
# ==========================================================

def generar_hash_cache(*args: Any) -> str:
    """
    Genera un hash SHA-256 consistente para llaves de caché.
    Usa serialización compacta y 'default=str' para manejar objetos complejos.
    """
    # separators=(",", ":") elimina espacios innecesarios para optimizar peso/bytes
    payload_serializado = json.dumps(
        args, 
        sort_keys=True, 
        default=str, 
        separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload_serializado).hexdigest()

def lanzar_tarea_segura(coro) -> asyncio.Task:
    """
    🚀 Lanza tareas en background controlando excepciones (Anti-Zombies).
    """
    task = asyncio.create_task(coro)
    background_tasks_activas.add(task)
    
    def log_task_exception(t: asyncio.Task):
        try:
            if not t.done():
                return
            exc = t.exception()
            if exc: 
                # Uso oficial de exc_info=True para adjuntar stacktrace sin romper el flujo
                logger.error(f"❌ [TASK BG ERROR] Falla en segundo plano: {exc}", exc_info=True)
        except asyncio.CancelledError:
            logger.warning("⚠️ [TASK BG CANCELLED] Tarea cancelada abruptamente.")
        except Exception:
            logger.error("❌ [TASK BG CRITICAL] Error procesando excepción de tarea", exc_info=True)
        finally:
            background_tasks_activas.discard(t)
            
    task.add_done_callback(log_task_exception)
    return task

def now_ts() -> float: 
    """Retorna el timestamp Unix en SEGUNDOS (Compatible con TTL, Supabase y JWT)."""
    return time.time()

def now_ms() -> int:
    """Retorna el timestamp Unix en milisegundos para logs de alta precisión."""
    return time.time_ns() // 1_000_000

def validar_tel(telefono: str) -> str:
    """
    📞 Unificación AAA: Limpia y valida números de teléfono.
    """
    try:
        limpio = re.sub(r"[^\d]", "", str(telefono))
        if limpio.startswith("521"):
            limpio = "52" + limpio[3:]
            
        if len(limpio) < 10 or len(limpio) > 15:
            return ""
        
        # Filtro de seguridad O(N) para números vacíos o basura (999999...)
        if len(set(limpio)) == 1:
            return ""
            
        return limpio
    except Exception:
        logger.error("⚠️ [VALIDAR TEL ERROR] Error limpiando teléfono", exc_info=True)
        return ""

# ==========================================================
# 🧼 NORMALIZACIÓN Y LLAVES ÚNICAS
# ==========================================================

def normalizar_nombre_busqueda(nombre: str) -> str:
    """
    Normaliza títulos (Unicode, Stopwords, Basura) para matching de alta precisión.
    """
    texto_original = str(nombre).lower().strip()
    texto = unicodedata.normalize("NFKD", texto_original).encode("ASCII", "ignore").decode("utf-8")
    
    texto = f" {texto} "
    stopwords = [" the ", " a ", " an ", " of ", " for ", " and ", " el ", " la ", " los ", " las ", " de ", " para ", " y "]
    basura = [" edition ", " greatest hits ", " platinum ", " remastered ", " bundle ", " loose ", " cib ", " new ", " goty "]
    
    for word in stopwords + basura:
        texto = texto.replace(word, " ")
        
    resultado = " ".join(texto.split())
    return resultado if resultado else texto_original

def generar_cache_key(nombre: str, consola: str) -> str:
    """Genera una llave de caché estandarizada e inmutable."""
    return generar_hash_cache(
        normalizar_nombre_busqueda(nombre),
        str(consola).lower().strip()
    )

async def lanzar_gc_si_toca() -> None:
    """🛡️ Obsoleto por TTLCache, mantenido por compatibilidad estructural."""
    pass 

# ==========================================================
# 💰 MANEJO DE DIVISAS Y PRECIOS
# ==========================================================

async def obtener_precio_cache(llave: str) -> Optional[dict[str, Any]]:
    """Recupera un precio cacheadado usando copia defensiva."""
    datos = cache_precios_ram.get(llave)
    
    metricas_radar.setdefault("cache_hits", 0)
    metricas_radar.setdefault("cache_miss", 0)

    if datos:
        logger.info("⚡ [CACHE HIT] Precio recuperado.")
        metricas_radar["cache_hits"] += 1
        
        # Copia defensiva inmutable
        valores = dict(datos.get("valores", {}))
        if "mxn" not in valores and "mxn_mercado" in valores:
            valores["mxn"] = valores["mxn_mercado"]
        return valores
        
    metricas_radar["cache_miss"] += 1
    return None

async def guardar_precio_cache(llave: str, valores: dict[str, Any]) -> None:
    """Guarda un precio en RAM protegido por Lock y copia defensiva."""
    if not isinstance(valores, dict):
        raise TypeError(f"Los valores en caché deben ser un diccionario, se recibió: {type(valores)}")
        
    async with cache_lock:
        cache_precios_ram[llave] = {
            "valores": dict(valores) 
        }

async def obtener_dolar_hoy_async() -> float:
    """Obtiene divisa USD -> MXN con caché, Fallback y barrera de valores corruptos."""
    ahora = time.time()
    
    async with lock_divisa:
        if ahora < CACHE_DIVISA.get("expira", 0):
            return CACHE_DIVISA.get("valor", 18.00)
            
        try:
            if not http_client: 
                logger.warning("⚠️ [DIVISAS] http_client no inicializado, usando default $18.00")
                return 18.00

            timeout_cfg = HTTP_TIMEOUTS if isinstance(HTTP_TIMEOUTS, httpx.Timeout) else httpx.Timeout(15.0)

            # --- INTENTO 1: API PRIMARIA ---
            try:
                res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=timeout_cfg)
                if res.status_code == 200:
                    val = float(res.json().get("rates", {}).get("MXN", 18.00))
                    if 5.0 <= val <= 50.0:
                        CACHE_DIVISA["valor"] = val
                        CACHE_DIVISA["expira"] = ahora + 43200
                        logger.info(f"💵 [DIVISAS UPDATE] Nuevo tipo de cambio: ${val} MXN")
                        return val
            except Exception as e:
                logger.warning(f"⚠️ [DIVISAS FALLO PRIMARIA] {e}")

            # --- INTENTO 2: API SECUNDARIA (FALLBACK) ---
            res_sec = await http_client.get("https://open.er-api.com/v6/latest/USD", timeout=timeout_cfg)
            if res_sec.status_code == 200:
                val = float(res_sec.json().get("rates", {}).get("MXN", 18.00))
                if 5.0 <= val <= 50.0:
                    CACHE_DIVISA["valor"] = val
                    CACHE_DIVISA["expira"] = ahora + 43200  
                    return val

        except Exception:
            logger.error("⚠️ [DIVISAS ERROR FATAL] Agotados intentos de APIs", exc_info=True)
            
        return CACHE_DIVISA.get("valor", 18.00)

# ==========================================================
# 🛡️ ENMASCARADOR PII Y OBSERVABILIDAD
# ==========================================================

def enmascarar_telefono(tel: str) -> str:
    """Enmascara fuertemente un número de teléfono (Compliance)."""
    try:
        tel = re.sub(r"[^\d]", "", str(tel))
        if len(tel) >= 10:
            return tel[:2] + ("*" * (len(tel) - 5)) + tel[-3:]
        return "***"
    except Exception:
        return "***"

def validar_estado_utils() -> dict[str, Any]:
    """🩺 Health Check interno (Versión Enterprise)."""
    return {
        "status": "ok" if http_client is not None else "degraded",
        "timestamp": now_ts(),
        "timestamp_ms": now_ms(),
        "http_client_activo": http_client is not None,
        "tareas_background_vivas": len(background_tasks_activas),
        "items_en_cache_ram": len(cache_precios_ram),
        "divisa_actual": CACHE_DIVISA.get("valor", 18.0),
        "metricas_radar": dict(metricas_radar)
    }

# ==========================================================
# 📦 CONTROL DE EXPORTACIONES PÚBLICAS
# ==========================================================
__all__ = [
    "generar_hash_cache",
    "lanzar_tarea_segura",
    "now_ts",
    "now_ms",
    "validar_tel",
    "normalizar_nombre_busqueda",
    "generar_cache_key",
    "lanzar_gc_si_toca",
    "obtener_precio_cache",
    "guardar_precio_cache",
    "obtener_dolar_hoy_async",
    "enmascarar_telefono",
    "validar_estado_utils"
]