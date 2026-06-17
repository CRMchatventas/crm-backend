# ==============================================================================
# 🚀 MÓDULO: db_core_wrapper.py (GOLD PRODUCTION STANDARD - v2.8)
# refactorizado ok
# ==============================================================================
import asyncio
import time
import re
from typing import Any, Optional
from fastapi import HTTPException, status
from postgrest.exceptions import APIError  # <-- FIX: Importado para manejo real de errores

# FIX FASE 2: Importamos supabase desde config_and_schemas en lugar de crearlo aquí
from config_and_schemas import logger, now_ts, supabase 

# ==========================================================
# 🛑 CIRCUIT BREAKER STATE (Thread-safe, lazy-initialized)
# ==========================================================
CB_STATE = {"failures": 0, "blocked_until": 0.0}
_CB_LOCK: Optional[asyncio.Lock] = None

def get_cb_lock() -> asyncio.Lock:
    global _CB_LOCK
    if _CB_LOCK is None:
        _CB_LOCK = asyncio.Lock()
    return _CB_LOCK

async def _register_infra_failure():
    """Registra fallos atómicamente para evitar carreras estadísticas en el Breaker."""
    async with get_cb_lock():
        CB_STATE["failures"] += 1
        if CB_STATE["failures"] >= 5:
            CB_STATE["blocked_until"] = time.time() + 30.0
            logger.critical(f"🚨 [DB CIRCUIT OPEN] Fallos concurrentes: {CB_STATE['failures']}. Bloqueo 30s.")

# ==============================================================================
# 🛡️ WRAPPER ASÍNCRONO SUPABASE AAA HARDENED EDITION
# ==============================================================================
async def async_db_execute(query_builder: Any, timeout_seg: float = 15.0, allow_retry: bool = True):
    """
    Wrapper asíncrono optimizado y tolerante a fallos.
    
    Args:
        query_builder: El objeto .execute() de Supabase.
        timeout_seg: Tiempo máximo de espera en segundos.
        allow_retry: MANTENER EN FALSE para operaciones no idempotentes (INSERT, UPDATE, DELETE, RPC).
    """
    
    # 🛑 1. Verificación y Reset Automático del Circuit Breaker
    async with get_cb_lock():
        if CB_STATE["blocked_until"] > 0:
            if time.time() >= CB_STATE["blocked_until"]:
                CB_STATE["blocked_until"] = 0.0
                CB_STATE["failures"] = 0
            else:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                    detail="DB Circuit Breaker activo. Reintente en breve."
                )

    # 🛡️ 2. Validación estructural estricta
    if not hasattr(query_builder, "execute") or not callable(query_builder.execute):
        logger.error("🚨 [DB ERROR] QueryBuilder inválido o no ejecutable.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Consulta inválida.")

    inicio_query = now_ts()
    timeout_seg = max(3.0, min(float(timeout_seg), 60.0))
    
    # FIX FASE 1: Si no es seguro reintentar (ej. INSERT/RPC), cortamos los reintentos a 0.
    MAX_REINTENTOS = 2 if allow_retry else 0

    for intento in range(MAX_REINTENTOS + 1):
        try:
            if MAX_REINTENTOS > 0:
                logger.info(f"🛢️ [DB EXECUTE] Intento={intento+1} | Timeout={timeout_seg}s")

            resultado = await asyncio.wait_for(
                asyncio.to_thread(query_builder.execute),
                timeout=timeout_seg
            )

            if resultado is None:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Respuesta vacía.")

            # Telemetría Exitosa
            tiempo_total = now_ts() - inicio_query
            if tiempo_total >= 5.0:
                logger.warning(f"⚠️ [DB SLOW QUERY] {tiempo_total:.3f}s")
            
            async with get_cb_lock():
                CB_STATE["failures"] = 0  # Reset on success
            
            return resultado

        except HTTPException:
            raise  # Errores ya clasificados, propagar directamente
            
        except APIError as e:
            # --- FIX FASE 1: CLASIFICACIÓN DE ERRORES POSTGREST (Fail-Fast puro) ---
            error_code = getattr(e, 'code', 'UNKNOWN')
            error_details = getattr(e, 'message', str(e))
            
            # 23505 = unique_violation, 23503 = foreign_key_violation
            if error_code in ["23505", "23503"]:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Conflicto de integridad o duplicidad.")
            
            # 42XXX o 22XXX = syntax_error, undefined_table, data_exception
            if error_code.startswith(("42", "22")):
                raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Error de estructura en BD.")
            
            # 42501 = insufficient_privilege (RLS)
            if error_code == "42501" or "jwt" in error_details.lower():
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Acceso denegado por RLS o JWT.")

            # Errores no catalogados suman al breaker
            safe_error = re.sub(r'apikey=[^\s]+', 'apikey=[REDACTED]', error_details)
            logger.error(f"❌ [DB API ERROR] Code={error_code} | {safe_error[:200]}")
            await _register_infra_failure()

        except (asyncio.TimeoutError, Exception) as e:
            # --- FALLOS DE INFRAESTRUCTURA (Breaker Territory) ---
            safe_error = re.sub(r'apikey=[^\s]+', 'apikey=[REDACTED]', str(e))
            logger.error(f"❌ [DB INFRA ERROR] Intento={intento+1} | {safe_error[:200]}")
            
            await _register_infra_failure()
            
            if isinstance(e, asyncio.TimeoutError):
                logger.warning(f"[DB TIMEOUT] Timeout de {timeout_seg}s excedido.")

        # Aplicar Backoff exponencial solo si quedan reintentos
        if intento < MAX_REINTENTOS:
            await asyncio.sleep(min(4.0, 2 ** intento))

    # Si sale del loop, significa que agotó intentos o era operación de intento único (allow_retry=False)
    logger.critical(f"🚨 [DB FAILSAFE] Fallo total de operación {'idempotente' if allow_retry else 'crítica'} tras {MAX_REINTENTOS} reintentos.")
    raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail="La base de datos no responde de forma segura.")