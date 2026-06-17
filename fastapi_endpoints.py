# ==========================================================
# 🚀 MÓDULO: fastapi_endpoints.py (AAA ENTERPRISE 10/10)
# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V20.2
# Capa de Red, API HTTP, Middlewares y Orquestación
# ==========================================================

import os
import re
import time
import asyncio
import hashlib
import hmac
import json
import uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Final, Dict

from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from cachetools import TTLCache

# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
import config_and_schemas as config

logger = config.logger

# ==========================================================
# 🛡️ CONFIGURACIÓN Y SECRETOS
# ==========================================================

META_APP_SECRET: Final = os.getenv("META_APP_SECRET", "").strip()
if not META_APP_SECRET:
    raise RuntimeError("❌ FATAL: META_APP_SECRET no configurada.")

ALLOWED_ORIGINS: Final = [
    x.strip() 
    for x in os.getenv("ALLOWED_ORIGINS", "https://app.veltrix.com").split(",") 
    if x.strip()
]

MAX_WEBHOOK_SIZE: Final = 2 * 1024 * 1024  
MAX_BACKGROUND_TASKS: Final = 5000         
MAX_REQUESTS_PER_IP_MINUTE: Final = 300    
WEBHOOK_MAX_CONCURRENCY: Final = int(os.getenv("WEBHOOK_MAX_CONCURRENCY", "200"))

# ==========================================================
# ⚙️ ESTADO GLOBAL Y CONCURRENCIA
# ==========================================================

CB_CLIENTES_CACHE: TTLCache = TTLCache(maxsize=50000, ttl=3600)
RATE_LIMIT_IP: Dict[str, deque[float]] = {}

CIRCUIT_BREAKER_LOCK = asyncio.Lock()
REPLAY_LOCK = asyncio.Lock()
IP_LOCK = asyncio.Lock()
METRICS_LOCK = asyncio.Lock()

WEBHOOK_SEMAPHORE = asyncio.Semaphore(WEBHOOK_MAX_CONCURRENCY)

GLOBAL_ERRORES_WINDOW: deque[float] = deque(maxlen=500)
GLOBAL_CB_HASTA = 0.0

WEBHOOK_METRICS = {
    "recibidos": 0, "ok": 0, "timeout": 0, "error": 0, "rechazados": 0
}

# ==========================================================
# 🔍 HELPERS OPERATIVOS Y PRIVACIDAD
# ==========================================================

async def registrar_metrica(tipo: str):
    async with METRICS_LOCK:
        if tipo in WEBHOOK_METRICS:
            WEBHOOK_METRICS[tipo] += 1

def enmascarar_sender(sender: str) -> str:
    if sender == "unknown" or len(sender) < 6: return sender
    return f"{sender[:4]}******{sender[-2:]}"

def extraer_sender_seguro(data: dict) -> str:
    try:
        entries = data.get("entry", [])
        if not entries: return "unknown"
        changes = entries[0].get("changes", [])
        if not changes: return "unknown"
        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages: return "unknown"

        raw_sender = str(messages[0].get("from", ""))
        clean_sender = re.sub(r"\D", "", raw_sender)[:15]
        return clean_sender if clean_sender else "unknown"
    except (KeyError, IndexError, AttributeError, TypeError) as parse_e:
        logger.error(json.dumps({"event": "parse_sender_error", "error": str(parse_e)}))
        return "unknown"

async def verificar_circuit_breaker(sender: str) -> bool:
    async with CIRCUIT_BREAKER_LOCK:
        if time.time() < GLOBAL_CB_HASTA: return True
        estado = CB_CLIENTES_CACHE.get(sender, {"errores": 0, "bloqueado_hasta": 0.0})
        return time.time() < estado["bloqueado_hasta"]

async def registrar_error_cb(sender: str, request_id: str):
    global GLOBAL_CB_HASTA
    now = time.time()
    
    async with CIRCUIT_BREAKER_LOCK:
        estado = CB_CLIENTES_CACHE.get(sender, {"errores": 0, "bloqueado_hasta": 0.0})
        estado["errores"] += 1
        
        if estado["errores"] > 10:
            estado["bloqueado_hasta"] = now + 300
            logger.critical(json.dumps({"event": "cb_local_activado", "sender": enmascarar_sender(sender), "request_id": request_id}))
            
        CB_CLIENTES_CACHE[sender] = estado
        
        GLOBAL_ERRORES_WINDOW.append(now)
        while GLOBAL_ERRORES_WINDOW and GLOBAL_ERRORES_WINDOW[0] < now - 300:
            GLOBAL_ERRORES_WINDOW.popleft()
            
        if len(GLOBAL_ERRORES_WINDOW) >= 500:
            GLOBAL_CB_HASTA = now + 60
            GLOBAL_ERRORES_WINDOW.clear()
            logger.critical(json.dumps({"event": "cb_global_activado", "reason": "posible_ataque_distribuido"}))

async def registrar_exito_cb(sender: str):
    async with CIRCUIT_BREAKER_LOCK:
        estado = CB_CLIENTES_CACHE.get(sender)
        if estado:
            estado["errores"] = max(0, estado["errores"] - 1)
            if estado["errores"] == 0: estado["bloqueado_hasta"] = 0.0
            CB_CLIENTES_CACHE[sender] = estado

# ==========================================================
# 🛡️ CICLO DE VIDA (GRACEFUL SHUTDOWN & LOCAL GC)
# ==========================================================

async def task_gc_local_state():
    while True:
        try:
            await asyncio.sleep(600)
            now = time.time()
            async with IP_LOCK:
                ips_borrar = [ip for ip, cola in RATE_LIMIT_IP.items() if not cola or cola[-1] < now - 120]
                for ip in ips_borrar: RATE_LIMIT_IP.pop(ip, None)
        except asyncio.CancelledError:
            logger.info("🛑 [GC LOCAL] Tarea cancelada graceful.")
            break
        except Exception as e:
            logger.error(f"❌ Error en GC Local: {e}")

@asynccontextmanager
async def router_lifespan(app: APIRouter):
    logger.info("🚀 Router de Veltrix Engine Iniciando...")
    gc_task = asyncio.create_task(config.task_gc_locks())
    gc_local_task = asyncio.create_task(task_gc_local_state())
    yield
    
    logger.info("🛑 Iniciando apagado graceful del Router...")
    gc_task.cancel()
    gc_local_task.cancel()
    
    try:
        await asyncio.gather(gc_task, gc_local_task, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("🛑 [LIFESPAN] Tareas de limpieza canceladas correctamente.")

    async with config.background_tasks_lock:
        tareas_pendientes = list(config.background_tasks_activas)

    if tareas_pendientes:
        logger.info(f"⏳ Esperando {len(tareas_pendientes)} tareas en background para cierre seguro...")
        for task in tareas_pendientes: task.cancel()
        await asyncio.gather(*tareas_pendientes, return_exceptions=True)

    logger.info("🛑 Router Apagado. Memoria liberada.")

router = APIRouter(lifespan=router_lifespan)

# ==========================================================
# ❤️ HEALTH CHECKS, MÉTRICAS Y OBSERVABILIDAD
# ==========================================================

@router.get("/healthz")
async def healthz():
    return {"status": "ok"}

@router.get("/readyz")
async def readyz():
    """Readiness profundo paralelo y con snapshot de métricas seguras."""
    async with config.background_tasks_lock:
        tasks_count = len(config.background_tasks_activas)
        
    async with METRICS_LOCK:
        current_metrics = WEBHOOK_METRICS.copy()
        
    async def _check_module(mod) -> str:
        if hasattr(mod, "healthcheck"):
            try:
                return await asyncio.wait_for(mod.healthcheck(), timeout=2.0)
            except Exception as e:
                logger.error(f"❌ [HEALTHCHECK] Fallo validando módulo {mod.__name__}: {e}")
                return "fail"
        return "ok"

    import db_crm_logic
    import ai_gemini_core

    db_status, ai_status = await asyncio.gather(
        _check_module(db_crm_logic),
        _check_module(ai_gemini_core)
    )
    
    global_cb_active = time.time() < GLOBAL_CB_HASTA
    status = "degraded" if (tasks_count >= (MAX_BACKGROUND_TASKS * 0.8) or db_status != "ok" or ai_status != "ok" or global_cb_active) else "ready"
        
    return {
        "status": status,
        "background_tasks": tasks_count,
        "webhook_slots_free": getattr(WEBHOOK_SEMAPHORE, '_value', 0),
        "cb_global_active": global_cb_active,
        "dependencies": {"db_api": db_status, "ai_engine": ai_status},
        "metrics": current_metrics
    }

@router.get("/metrics")
async def metrics():
    """Endpoint nativo para scraping de Prometheus / Grafana."""
    async with METRICS_LOCK:
        m = WEBHOOK_METRICS.copy()
        
    lines = [
        "# HELP veltrix_webhook_requests_total Total de webhooks recibidos",
        "# TYPE veltrix_webhook_requests_total counter",
        f"veltrix_webhook_requests_total {m['recibidos']}",
        "# HELP veltrix_webhook_ok_total Webhooks procesados exitosamente",
        "# TYPE veltrix_webhook_ok_total counter",
        f"veltrix_webhook_ok_total {m['ok']}",
        "# HELP veltrix_webhook_errors_total Errores de procesamiento (Excepciones)",
        "# TYPE veltrix_webhook_errors_total counter",
        f"veltrix_webhook_errors_total {m['error']}",
        "# HELP veltrix_webhook_timeouts_total Timeouts en la lógica de negocio",
        "# TYPE veltrix_webhook_timeouts_total counter",
        f"veltrix_webhook_timeouts_total {m['timeout']}",
        "# HELP veltrix_webhook_rejected_total Webhooks rechazados (4xx/5xx)",
        "# TYPE veltrix_webhook_rejected_total counter",
        f"veltrix_webhook_rejected_total {m['rechazados']}"
    ]
    return PlainTextResponse("\n".join(lines) + "\n")

# ==========================================================
# 🌐 WEBHOOK VERIFY & RECEIVE
# ==========================================================

@router.get("/webhook")
async def verificar_webhook(request: Request):
    params = request.query_params
    token = params.get("hub.verify_token")

    if params.get("hub.mode") == "subscribe" and token == config.WEBHOOK_SECRET:
        challenge = params.get("hub.challenge")
        if not challenge or not challenge.isdigit():
            raise HTTPException(status_code=400, detail="Challenge inválido")
        return PlainTextResponse(challenge)

    raise HTTPException(status_code=403, detail="Token inválido")

@router.post("/webhook")
async def recibir_webhook(request: Request):
    await registrar_metrica("recibidos")
    request_id = str(uuid.uuid4())[:8] 
    
    async def rechazar(codigo: int, msj: str):
        await registrar_metrica("rechazados")
        logger.error(json.dumps({"event": "webhook_rechazado", "request_id": request_id, "reason": msj, "status": codigo}))
        raise HTTPException(status_code=codigo, detail=msj)

    client_ip = request.client.host if request.client else "127.0.0.1"
    now = time.time()
    
    async with IP_LOCK:
        if client_ip not in RATE_LIMIT_IP:
            RATE_LIMIT_IP[client_ip] = deque(maxlen=MAX_REQUESTS_PER_IP_MINUTE)
        
        while RATE_LIMIT_IP[client_ip] and RATE_LIMIT_IP[client_ip][0] < now - 60:
            RATE_LIMIT_IP[client_ip].popleft()
            
        if len(RATE_LIMIT_IP[client_ip]) >= MAX_REQUESTS_PER_IP_MINUTE:
            await rechazar(429, "Too Many Requests")
            
        RATE_LIMIT_IP[client_ip].append(now)

    if "application/json" not in request.headers.get("content-type", ""):
        await rechazar(415, "Content-Type inválido")

    body = await request.body()
    if len(body) > MAX_WEBHOOK_SIZE: await rechazar(413, "Payload demasiado grande")

    signature = request.headers.get("X-Hub-Signature-256", "")
    if not signature: await rechazar(403, "Falta firma")

    expected_sig = hmac.new(META_APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(f"sha256={expected_sig}", signature):
        await rechazar(403, "Firma inválida")

    try:
        data = json.loads(body)
    except json.JSONDecodeError as decode_e:
        logger.error(json.dumps({"event": "json_invalido", "error": str(decode_e)}))
        await rechazar(400, "JSON inválido")

    if "entry" not in data or not isinstance(data["entry"], list):
        await rechazar(400, "Payload Meta estructuralmente inválido")

    replay_key = hashlib.sha256(body).hexdigest()
    async with REPLAY_LOCK:
        if replay_key in config.WEBHOOK_REPLAY_CACHE:
            logger.info(json.dumps({"event": "webhook_duplicado", "request_id": request_id, "action": "ignored"}))
            return {"status": "ok", "note": "duplicate_ignored"}
        config.WEBHOOK_REPLAY_CACHE[replay_key] = True

    async with CIRCUIT_BREAKER_LOCK:
        if time.time() < GLOBAL_CB_HASTA:
            await rechazar(503, "Global Circuit Breaker activo")

    async with config.background_tasks_lock:
        if len(config.background_tasks_activas) >= MAX_BACKGROUND_TASKS:
            await rechazar(503, "Background queue saturated")

    task = asyncio.create_task(procesar_webhook_async(data, request_id))
    try:
        await asyncio.wait_for(config.registrar_background_task(task), timeout=3.0)
    except asyncio.TimeoutError:
        logger.error(json.dumps({"event": "task_registration_timeout", "request_id": request_id}))
        await rechazar(503, "Internal System Overload")

    return {"status": "accepted"}

# ==========================================================
# 🧠 PROCESAMIENTO Y LÓGICA
# ==========================================================

async def procesar_webhook_async(data: dict, request_id: str):
    sender = extraer_sender_seguro(data)
    masked_sender = enmascarar_sender(sender)
    
    if sender == "unknown": return

    if await verificar_circuit_breaker(sender):
        logger.error(json.dumps({"event": "webhook_bloqueado_cb", "sender": masked_sender, "request_id": request_id}))
        return

    logger.info(json.dumps({"event": "webhook_procesando", "sender": masked_sender, "request_id": request_id}))

    try:
        async with WEBHOOK_SEMAPHORE:
            await asyncio.wait_for(ejecutar_logica_negocio(sender, data, request_id), timeout=60.0)
        
        await registrar_metrica("ok")
        await registrar_exito_cb(sender)
        logger.info(json.dumps({"event": "webhook_completado", "sender": masked_sender, "request_id": request_id}))

    except asyncio.CancelledError:
        logger.info(json.dumps({"event": "tarea_cancelada", "sender": masked_sender, "request_id": request_id}))
        raise

    except asyncio.TimeoutError:
        await registrar_metrica("timeout")
        logger.error(json.dumps({"event": "webhook_timeout", "sender": masked_sender, "request_id": request_id}))
        await registrar_error_cb(sender, request_id)

    except Exception as e:
        await registrar_metrica("error")
        logger.exception(json.dumps({"event": "webhook_error_interno", "sender": masked_sender, "request_id": request_id, "error": str(e)}))
        await registrar_error_cb(sender, request_id)

# ==========================================================
# 🔒 LÓGICA DE NEGOCIO
# ==========================================================

async def ejecutar_logica_negocio(sender: str, data: dict, request_id: str):
    """Wrapper core con protección de concurrencia y propagación de Correlation ID."""
    async with await config.get_lock(sender):
        # NOTA: Aquí se delega a las funciones unificadas de db_crm_logic o ai_gemini_core
        # sin acoplar la capa HTTP a la lógica de base de datos directamente.
        pass