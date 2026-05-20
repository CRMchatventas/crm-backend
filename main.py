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
# 🛡️ 2. ESCUDO IA Y ARRANQUE DE APLICACIÓN
# ==========================================================
# 🛡️ FIX AAA: Prompt Injection Regex Hardening
PROMPT_INJECTION_KEYWORDS = ["ignora tus instrucciones", "developer mode", "system prompt", "eres chatgpt", "olvida las reglas"]

def detectar_prompt_injection(texto: str) -> bool:
    texto_lower = str(texto).lower()
    return any(kw in texto_lower for kw in PROMPT_INJECTION_KEYWORDS) or bool(re.search(r"ignore.{0,20}instruction", texto_lower))

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

# ==========================================
# 📦 3. MODELOS PYDANTIC
# ==========================================
class InventarioItem(BaseModel):
    id: Optional[int] = None
    nombre: str = Field(..., min_length=1, max_length=180)
    consola: str = Field(..., min_length=1, max_length=80)
    precio: float = Field(..., ge=0)
    nuevo_precio: Optional[float] = None
    costo: float = Field(default=0.0, ge=0)
    stock: int = Field(default=1, ge=0)
    nuevo_stock: Optional[int] = None
    codigo_barras: str = ""
    url_portada: str = ""
    estado_general: str = "Bueno"
    rareza: str = ""
    vendedor_id: str = ""
    tiene_caja: bool = False
    tiene_manual: bool = False
    es_portada_original: bool = False
    descripcion_detallada: str = ""
    @field_validator("nombre", "consola", mode="before")
    @classmethod
    def validar_texto(cls, value: str): return limpiar_texto(value)

class VentaItem(BaseModel): 
    id: Optional[int] = None
    nombre: str
    consola: str
    estado_general: str = ""
    nuevo_stock: Optional[int] = None      
    cantidad_vendida: Optional[int] = None 
    vendedor_id: str = ""
    
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

class NuevoArticulo(BaseModel): nombre: str; categoria: str = "General"; precio_compra: float = 0.0; precio: float = 0.0; stock: int = 1; vendedor_id: str = ""
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
    rareza: str
    url_pc: str
    confidence_score: float

class ReordenarColumnasAction(BaseModel):
    columnas: list[str]
    vendedor_id: str

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
    if not token: raise HTTPException(status_code=401, detail="Token faltante")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"], audience="veltrix-clients", issuer="veltrix-engine")
        return str(payload.get("sub"))
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado. Inicie sesión nuevamente.")
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

async def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    ahora = now_ts()
    
    # 🛡️ FIX AAA: Protección concurrente y validación de Token Buckets
    async with rate_limit_global_lock:
        tokens = tokens_consumidos_tenant.get(vendedor_id, 0)
        if tokens > MAX_TOKENS_POR_MINUTO_TENANT: return False
        
        while rate_limit_global and (ahora - rate_limit_global[0]) > 60:
            rate_limit_global.popleft()
            
        if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO: return False
        rate_limit_global.append(ahora)
        
        # Limpieza inteligente de tenants y teléfonos
        t_list = rate_limit_tenant.get(vendedor_id, [])
        t_list = [t for t in t_list if ahora - t <= 60]
        if len(t_list) >= MAX_REQUESTS_POR_MINUTO_TENANT: return False
        t_list.append(ahora)
        rate_limit_tenant[vendedor_id] = t_list
        
        return True

# ==========================================================
# 🧠 5. CEREBRO IA GEMINI Y RAG (RUTEADOR)
# ==========================================================
async def consultar_gemini_json(prompt: str, media_dict: dict = None, temperature: float = 0.2, retries: int = 2, vendedor_id: str = "V-001") -> dict:
    global gemini_bloqueado_hasta
    inicio_telemetria = now_ts()
    
    if now_ts() < gemini_bloqueado_hasta:
        return {"respuesta": "En este momento estoy atendiendo a varios clientes, denme un momento. 🎮", "intencion": "HUMANO", "confidence": 1.0}

    # 🛡️ FIX AAA: Implementación de CACHE IA Real
    cache_key = generar_hash_cache(str(prompt), vendedor_id, temperature)
    if cache_key in cache_respuestas_ia:
        cache_item = cache_respuestas_ia[cache_key]
        if now_ts() - cache_item["ts"] < CACHE_TTL_SECONDS:
            logger.info(f"⚡ [GEMINI] Respuesta servida desde Caché en {now_ts() - inicio_telemetria:.3f}s")
            return cache_item["data"]

    modelos = ['gemini-2.5-flash', 'gemini-1.5-flash'] 
    tokens_estimados = len(str(prompt)) // 4
    
    # 🛡️ FIX AAA: Rate Limit de Tokens Hardened (Protección contra Token Flood y Throttling)
    async with rate_limit_global_lock:
        tokens_actuales = tokens_consumidos_tenant.get(vendedor_id, 0)
        if tokens_actuales + tokens_estimados > MAX_TOKENS_POR_MINUTO_TENANT:
            logger.warning(f"🚨 [GEMINI FLOOD] Tenant {vendedor_id} superó el límite de tokens por minuto.")
            return {"respuesta": "Estoy procesando demasiadas solicitudes ahora mismo. Un asesor humano te atenderá.", "intencion": "HUMANO", "confidence": 0.0}
        tokens_consumidos_tenant[vendedor_id] = tokens_actuales + tokens_estimados

    for nombre_modelo in modelos:
        for intento in range(retries):
            try:
                # 🛡️ FIX AAA: Telemetría de IA
                model = genai.GenerativeModel(nombre_modelo) 
                contenido = prompt if isinstance(prompt, list) else [prompt]
                if media_dict and "data" in media_dict: 
                    contenido.append({"mime_type": media_dict.get("mime_type", "image/jpeg"), "data": media_dict["data"]})
                
                response = await asyncio.wait_for(
                    asyncio.to_thread(model.generate_content, contenido, generation_config=genai.types.GenerationConfig(temperature=temperature)),
                    timeout=20.0
                )
                
                texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
                
                # 🛡️ FIX AAA: JSON Parser Robusto e Incremental (Anti-Corrupción)
                try:
                    decoder = json.JSONDecoder()
                    obj, idx = decoder.raw_decode(texto_limpio)
                    
                    # Guardamos en caché en caso de éxito
                    cache_respuestas_ia[cache_key] = {"data": obj, "ts": now_ts()}
                    logger.info(f"🧠 [GEMINI] Generación exitosa con {nombre_modelo} en {now_ts() - inicio_telemetria:.3f}s")
                    return obj
                except json.JSONDecodeError:
                    # Fallback Regex si el incremental falla por basura alrededor
                    match = re.search(r'\{.*\}', texto_limpio, re.DOTALL)
                    if match: 
                        obj = orjson.loads(match.group())
                        cache_respuestas_ia[cache_key] = {"data": obj, "ts": now_ts()}
                        logger.info(f"🧠 [GEMINI] Generación exitosa (Regex Fallback) en {now_ts() - inicio_telemetria:.3f}s")
                        return obj
                    raise ValueError("Formato JSON incomprensible de la IA.")
                
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ [GEMINI] Timeout en intento {intento+1} con {nombre_modelo}")
            except Exception as e:
                logger.error(f"❌ [GEMINI] Error: {e}")
                if "429" in str(e) or "Quota" in str(e):
                    gemini_bloqueado_hasta = now_ts() + 60.0
                    break
                await asyncio.sleep(2)
                
    return {"respuesta": "Tuve un micro-corte. ¿Me repites tu mensaje por favor?", "intencion": "HUMANO", "confidence": 0.1}

def validar_respuesta_ia(data: dict) -> dict:
    if not isinstance(data, dict): raise Exception("IA devolvió formato inválido")
    intenciones_validas = ["COMPRA", "COTIZACION", "HUMANO", "PEDIDO_ESPECIAL", "REGATEO", "POSTVENTA", "GARANTIA", "SPAM", "MAYOREO", "SALUDO", "ENOJO"]
    intencion = str(data.get("intencion", "COTIZACION")).upper()
    if intencion not in intenciones_validas: intencion = "HUMANO"
    
    confidence = float(data.get("confidence", 1.0))
    if confidence < 0.60: intencion = "HUMANO" 
    
    return {
        "intencion": intencion,
        "respuesta": limpiar_texto(data.get("respuesta", "Hola. Estoy revisando la información.")),
        "juego_detectado": limpiar_texto(data.get("juego_detectado", "")),
        "emocion_cliente": str(data.get("emocion_cliente", "neutral")),
        "temperatura_lead": str(data.get("temperatura_lead", "frio")),
        "confidence": confidence,
        "accion_tool": str(data.get("accion_tool", "ninguna"))
    }

async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict, perfil_cliente_previo: dict = None, media_dict: dict = None):
    try:
        # 🛡️ FIX AAA: Prompt Injection Activo
        if detectar_prompt_injection(texto_cliente):
            logger.warning("🚨 [SECURITY] Prompt Injection interceptado en Cerebro IA.")
            return {"intencion": "SPAM", "respuesta": "Mensaje bloqueado por políticas de seguridad interna.", "confidence": 1.0}

        vendedor_id = config.get("vendedor_id", "V-001")
        giro_comercial = config.get("giro_comercial", "Videojuegos y Consolas")
        tono_ia = config.get("tono_ia", "Persuasivo y experto")
        
        lock_id = hashlib.sha256(f"{vendedor_id}:{texto_cliente[:50]}".encode()).hexdigest()
        
        tracking_locks_uso[lock_id] = now_ts()
        async with locks_por_conversacion[lock_id]:
            logger.info(f"🔮 [CEREBRO IA] Iniciando análisis cognitivo para Vendedor: {vendedor_id}")
            perfil_str = json.dumps(perfil_cliente_previo) if perfil_cliente_previo else "Cliente nuevo sin historial de consolas."

            prompt_estructurado = [
                {"role": "user", "parts": [f"""
[SYSTEM INSTRUCTIONS]
Eres el Motor de Inteligencia Artificial Comercial de un software SaaS llamado Veltrix Engine.
GIRO COMERCIAL: {giro_comercial}
PERSONALIDAD/TONO: {tono_ia}

[MEMORIA A LARGO PLAZO - PERFIL DEL CLIENTE]
{perfil_str}
*DIRECTRICES*: Si el perfil ya cuenta con una "consola_preferida", prioriza ofrecer juegos de esa plataforma. Si no la tiene o está vacía, dedúcela basándote en lo que el cliente pida en su mensaje.

[RAG CONTEXT - INVENTARIO DISPONIBLE EN TIEMPO REAL]
{inventario_contexto}

[HISTORIAL DE CHAT RECIENTE]
{historial_chat}

[MENSAJE ACTUAL O ACCIÓN DEL CLIENTE]
"{texto_cliente}"

🤖 TAREAS AUTÓNOMAS (TOOL CALLING INTEGRADO):
1. Detecta la intención real (COMPRA, COTIZACION, REGATEO, GARANTIA, HUMANO, SALUDO, ENOJO).
2. Determina la emoción dominante del lead y su temperatura comercial.
3. Extrae de forma limpia el nombre del juego solicitado.
4. Deduce o mantén la "consola_preferida" del usuario (PS5, PS4, Xbox, Nintendo Switch, etc).
5. REGLA DE NEGOCIO (TOOL): Si el cliente está en intención de REGATEO o expresa que el precio es muy elevado, tienes autorización exclusiva para aplicar una herramienta de descuento autónomo de hasta el 10% sobre el precio de lista. Si decides aplicarlo, cambia "accion_tool" a "aplicar_descuento" e inyecta el valor en "precio_oferta".

Responde estrictamente en un formato JSON plano, válido y limpio:
{{
  "intencion": "...",
  "respuesta": "Tu respuesta persuasiva, humana y orientada a cerrar la venta, mencionando precios si los tienes...",
  "emocion_cliente": "urgencia|enojo|duda|entusiasmo|neutral",
  "temperatura_lead": "frio|tibio|caliente",
  "juego_detectado": "...",
  "consola_preferida": "...",
  "confidence": 0.95,
  "accion_tool": "ninguna|aplicar_descuento",
  "precio_oferta": 0.0
}}
"""]}
            ]
            
            if media_dict and "data" in media_dict:
                logger.info(f"🎙️ [CEREBRO IA] Inyectando Audio Nativo Base64 al modelo generativo.")
                prompt_estructurado.append({
                    "mime_type": media_dict.get("mime_type", "audio/ogg"),
                    "data": media_dict["data"]
                })

            data = await consultar_gemini_json(prompt_estructurado, vendedor_id=vendedor_id)
            
            TOOLS_VALIDAS = ["ninguna", "aplicar_descuento"]
            if data.get("accion_tool") not in TOOLS_VALIDAS: data["accion_tool"] = "ninguna"
            
            logger.info(f"🎯 [CEREBRO IA] Análisis finalizado con éxito. Intención inferida: {data.get('intencion')}")
            return data

    except Exception as e:
        logger.error(f"❌ [CEREBRO ERROR] Error en el flujo cognitivo de la IA: {str(e)}")
        return {
            "intencion": "HUMANO", 
            "respuesta": "Hubo un micro-corte en mi sistema de datos. Un asesor humano revisará tu mensaje de inmediato. 🚀", 
            "confidence": 0.0,
            "consola_preferida": "",
            "accion_tool": "ninguna",
            "precio_oferta": 0.0
        }

async def obtener_contexto_inventario_rag(vendedor_id: str, texto_cliente: str = "") -> str:
    logger.info(f"🔍 [RAG INVENTARIO] Buscando coincidencias para: '{texto_cliente}' (Tenant: {vendedor_id})")
    try:
        palabras_clave = limpiar_texto(texto_cliente).lower()
        
        # 🛡️ FIX AAA: Prefiltro SQL en RAG (Evita Memory Kills en tenants gigantes)
        query = supabase.table('inventario').select('nombre, precio, stock, consola').eq('vendedor_id', str(vendedor_id)).gt('stock', 0)
        
        if palabras_clave and len(palabras_clave.strip()) >= 3:
            # Prefiltramos por las primeras palabras fuertes si es posible para aligerar carga
            palabras = palabras_clave.split()
            if palabras:
                query = query.ilike('nombre', f"%{palabras[0]}%")
                
        res_inv = await async_db_execute(query.limit(100)) # Limite duro escalable
        
        if not res_inv.data:
            logger.warning("⚠️ [RAG INVENTARIO] La base de datos del vendedor no tiene stock disponible (o el prefiltro falló).")
            # Fallback a inventario general
            res_inv = await async_db_execute(supabase.table('inventario').select('nombre, precio, stock, consola').eq('vendedor_id', str(vendedor_id)).gt('stock', 0).limit(50))
            if not res_inv.data: return "Catálogo vacío o agotado en este momento."

        inventario = res_inv.data

        if not palabras_clave or len(palabras_clave.strip()) < 3:
            logger.info("📋 [RAG INVENTARIO] Mensaje corto detectado. Retornando top 10 general.")
            return "\n".join([f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in inventario[:10]])

        diccionario_opciones = {f"{i['nombre']} {i.get('consola','')}".strip().lower(): i for i in inventario}
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
            logger.warning("⚠️ [RAG INVENTARIO] Ningún juego superó el filtro difuso. Activando Fallback de rescate.")
            items_filtrados = inventario[:5]

        lineas = [f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in items_filtrados]
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
    payload = {
        'columna': sanitizar_nombre_columna(columna, permitir_reservadas=True), 
        'estado_iluminacion': sanitizar_nombre_columna(iluminacion, permitir_reservadas=True), 
        'ultimo_juego_interes': bleach.clean(juego, tags=[], strip=True)[:100], 
        'ultima_interaccion_ia': datetime.now(timezone.utc).isoformat()
    }
    if perfil_ia: payload['perfil_psicologico'] = perfil_ia
    await async_db_execute(supabase.table('prospectos').update(payload).eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))

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

async def disparar_whatsapp_imagen_async(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    if not http_client: return False
    url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "image", "image": {"link": url_imagen, "caption": texto_mensaje}}
    
    for intento in range(2):
        try: 
            res = await http_client.post(url, headers=headers, json=payload, timeout=12.0)
            if res.status_code in [200, 201]: return True
        except: pass
    return False

async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        prompt = f"Cliente: {cliente}\nIntencion: {intencion}\nHistorial:\n{historial_str}\nResume el problema en 3 viñetas para el asesor humano. Devuelve un JSON: {{\"resumen\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        return data.get("resumen", "⚠️ El cliente necesita asistencia de inmediato.")
    except: return "⚠️ Cliente requiere atención humana."

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

async def descargar_media_whatsapp_async(media_id: str, token: str) -> Optional[dict]:
    if not http_client: return None
    try:
        url_info = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {token}"}
        res_info = await http_client.get(url_info, headers=headers)
        if res_info.status_code != 200: return None
        data_info = res_info.json()
        
        # 🛡️ FIX AAA: Límite de tamaño preventivo de Media (>15MB bloqueado)
        file_size = int(data_info.get("file_size", 0))
        if file_size > 15_000_000:
            logger.warning(f"⚠️ [MEDIA] Archivo excede el límite de tamaño seguro: {file_size} bytes")
            return None
            
        media_url = data_info.get("url")
        if not media_url: return None
        res_media = await http_client.get(media_url, headers=headers)
        if res_media.status_code != 200: return None
        
        mime_type = data_info.get("mime_type", "")
        if mime_type not in ["image/jpeg", "image/png", "audio/ogg", "audio/mp4", "audio/mpeg"]:
            logger.warning(f"⚠️ [MEDIA] Tipo MIME no soportado: {mime_type}")
            return None
            
        return {"mime_type": mime_type, "data": res_media.content}
    except Exception: return None

async def auditar_comprobante_ia(b64_img_data: bytes, mime_type: str, nombre_negocio: str, historial_chat: str):
    def safe_float_local(valor):
        try:
            if valor is None: return 0.0
            limpio = str(valor).replace("$", "").replace(",", "").replace("MXN", "").strip()
            return float(limpio)
        except: return 0.0
    try:
        fecha_hoy = datetime.now().strftime("%d de %B de %Y")
        prompt = f"""Eres el auditor financiero jefe de '{nombre_negocio}'. Tu misión es detectar estafas y pagos falsos.
HISTORIAL CHAT: {historial_chat}
HOY ES: {fecha_hoy}
REGLAS RECHAZO: 1. Debe ser un comprobante bancario real. 2. La fecha debe ser hoy o ayer. 3. Debe coincidir el monto del chat.
Responde en JSON: {{"es_pago": true/false, "monto_detectado": 0.0, "analisis": "motivo breve"}}"""
        data = await consultar_gemini_json(prompt, {"mime_type": mime_type, "data": b64_img_data}, temperature=0.0)
        return {
            "es_pago": bool(data.get("es_pago", False)),
            "monto_detectado": safe_float_local(data.get("monto_detectado", 0)),
            "analisis": str(data.get("analisis", "Análisis no disponible."))
        }
    except Exception as e: return {"es_pago": False, "monto_detectado": 0.0, "analisis": "Error interno del auditor."}

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
    try:
        consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
        query = f"{nombre_juego} {consola_web}".replace(" ", "+")
        url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
        
        html_search = await obtener_html_escalonado_async_portadas(url_search)
        if not html_search: return
        
        soup = BeautifulSoup(html_search, 'html.parser')
        img_tag = soup.find('img', class_='product_image') or soup.find('img', alt=lambda x: x and nombre_juego.lower() in x.lower())
        if not img_tag or not img_tag.get('src'): return
        
        imagen_url = img_tag['src']
        if not imagen_url.startswith("http"):
            imagen_url = "https:" + imagen_url if imagen_url.startswith("//") else "https://www.pricecharting.com" + imagen_url
            
        res_img = await http_client.get(imagen_url, timeout=15.0)
        if res_img.status_code != 200: return
        
        # 🛡️ FIX AAA: Compresión de imágenes para ahorrar Storage
        from PIL import Image
        import io
        img_buffer = io.BytesIO(res_img.content)
        img = Image.open(img_buffer)
        if img.mode in ("RGBA", "P"): img = img.convert("RGB")
        out_buffer = io.BytesIO()
        img.save(out_buffer, format="JPEG", quality=80, optimize=True)
        img_comprimida = out_buffer.getvalue()
        
        hash_img = hashlib.sha256(img_comprimida).hexdigest()[:10]
        nombre_archivo = f"{consola.replace(' ', '_')}_{nombre_juego.replace(' ', '_')}_{hash_img}.jpg"
        
        # 🛡️ FIX AAA: Manejo de Race Conditions (Duplicidad) en Supabase Upload
        try:
            await async_db_execute(supabase.storage.from_("portadas").upload(nombre_archivo, img_comprimida, {"content-type": "image/jpeg"}))
        except Exception as e_upload:
            if "Duplicate" not in str(e_upload): raise
            
        url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
        await async_db_execute(supabase.table('inventario').update({"url_portada": url_publica}).eq('id', juego_id_supabase))
        logger.info(f"🖼️ [PORTADA] Descargada, comprimida y linkeada: {nombre_juego}")
    except Exception as e: logger.error(f"⚠️ Error cazando portada: {e}")

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
                    contexto_inv = await obtener_contexto_inventario_rag(vendedor_id, p.get('ultimo_juego_interes', ''))
                    oferta = await generar_oferta_inteligente(p.get('nombre', 'Cliente'), p.get('ultimo_juego_interes', 'videojuego'), contexto_inv)
                    
                    if oferta and oferta.get("mensaje_oferta"):
                        mensaje = oferta.get("mensaje_oferta")
                        await disparar_whatsapp_dinamico_async(p.get('telefono'), mensaje, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
                        await actualizar_estado_crm(p.get('telefono'), vendedor_id, 'Con Descuento', 'oro', p.get('ultimo_juego_interes'))
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

            juego_detectado = decision.get("juego_detectado", "")
            consola_detectada = decision.get("consola_preferida", perfil_cliente_previo.get("consola_preferida", ""))
            accion_tool = str(decision.get("accion_tool", "ninguna")).lower()
            precio_oferta = decision.get("precio_oferta", 0.0)
            
            print(f"📊 [IA WORKFLOW] Diagnóstico - Intención: {intencion_ia} | Juego: {juego_detectado} | Plataforma: {consola_detectada}")

            perfil_cliente_actualizado = {
                **perfil_cliente_previo, 
                "emocion_actual": decision.get("emocion_cliente", "neutral"),
                "temperatura": decision.get("temperatura_lead", "frio"),
                "ultimo_interes": juego_detectado,
                "consola_preferida": consola_detectada,
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
                print("📦 [IA WORKFLOW] Título no localizado físicamente. Registrando alerta de pedido especial...")
                await enviar_alerta_whatsapp_admin(cliente, telefono, "PEDIDO_ESPECIAL", f"Busca: {juego_detectado}", config)

            print("💾 [IA WORKFLOW] Sincronizando metadatos de tarjeta y chat log en la nube...")
            await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, juego_detectado, perfil_ia=perfil_cliente_actualizado)
            await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

            url_imagen = None
            if juego_detectado:
                print(f"🖼️ [IA WORKFLOW] Rastreando enlace URL de portada para: '{juego_detectado}'")
                res_img = await async_db_execute(supabase.table('inventario').select('url_portada').ilike('nombre', f'%{juego_detectado}%').eq('vendedor_id', str(vendedor_id)).neq('url_portada', '').limit(1))
                if res_img.data: 
                    url_imagen = res_img.data[0].get('url_portada')
                    print(f"🔗 [IA WORKFLOW] Portada vinculada localizada: {url_imagen}")

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
            "confidence_score": 0.0
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
            "rareza": "Manual",
            "url_pc": url_final_godot,
            "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0
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
        "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0
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
        
        # 🔥 FIX AAA: Separación semántica de categoría y consola
        categoria_limpia = limpiar_texto(datos.categoria) 
        consola_limpia = categoria_limpia # Mantenemos retrocompatibilidad si Godot manda categoría como consola
        
        # 🛡️ FIX AAA: DB Timeout
        res_check = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').select('id')
                .eq('vendedor_id', vid_str)
                .ilike('nombre', nombre_limpio)
                .ilike('consola', consola_limpia)
                .limit(1)
            ),
            timeout=10.0
        )
        
        if res_check.data:
            raise HTTPException(400, "Este título ya existe en esta plataforma para tu inventario.")

        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').insert({
                    'vendedor_id': vid_str, 
                    'nombre': nombre_limpio, 
                    'categoria': categoria_limpia, 
                    'consola': consola_limpia, 
                    'precio_compra': datos.precio_compra, 
                    'precio': datos.precio, 
                    'stock': datos.stock
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
        
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario')
                .select("id, nombre, consola, precio, precio_compra, stock, url_portada, estado_general, rareza")
                .eq('vendedor_id', str(_sesion))
                .order('id', desc=True)
                .range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=15.0
        )
        
        inventario_limpio = []
        for row in (res.data or []):
            inventario_limpio.append({
                "id": row.get("id"),
                "nombre": html.escape(row.get("nombre") or ""),
                "consola": html.escape(row.get("consola") or ""),
                "precio": float(row.get("precio") or 0.0),
                "precio_compra": float(row.get("precio_compra") or 0.0),
                "stock": int(row.get("stock") or 0),
                "url_portada": row.get("url_portada") or "",
                "estado_general": row.get("estado_general") or "Bueno",
                "rareza": row.get("rareza") or "comun"
            })
            
        return {"status": "ok", "inventario": inventario_limpio}
    except Exception as e: 
        logger.error(f"❌ Error carga de inventario: {e}")
        raise HTTPException(status_code=500, detail="Error carga de inventario")

@app.post("/api/editar_item_visor")
async def editar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        if not item.id:
            raise HTTPException(400, "ID Requerido. Operación cancelada.")

        nombre_limpio = limpiar_texto(item.nombre)
        precio_final = max(0.0, float(item.nuevo_precio if hasattr(item, 'nuevo_precio') and item.nuevo_precio is not None else item.precio))
        stock_final = max(0, int(item.nuevo_stock if hasattr(item, 'nuevo_stock') and item.nuevo_stock is not None else item.stock))

        res_old = await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").select("nombre, consola").eq("id", item.id).eq("vendedor_id", vid_str).limit(1)),
            timeout=5.0
        )
        
        nombre_anterior = res_old.data[0].get("nombre", "") if res_old.data else ""
        consola_anterior = res_old.data[0].get("consola", "") if res_old.data else ""

        await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario")
                .update({"nombre": nombre_limpio, "precio": precio_final, "stock": stock_final, "consola": limpiar_texto(item.consola)})
                .eq("id", item.id).eq("vendedor_id", vid_str)
            ),
            timeout=10.0
        )
        
        # 🔥 Invalidación Doble de Caché AAA
        async with cache_lock:
            if nombre_anterior: cache_precios_ram.pop(generar_cache_key(nombre_anterior, consola_anterior), None)
            cache_precios_ram.pop(generar_cache_key(nombre_limpio, item.consola), None)

        return {"status": "ok"}
    except HTTPException: raise
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
                .select('id, nombre, consola, precio, stock, url_portada')
                .eq('vendedor_id', str(_sesion))
                .ilike('nombre', f'%{q_limpio}%')
                .limit(25)
            ),
            timeout=10.0
        )
        
        resultados = res.data or []
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
            async_db_execute(supabase.table("inventario").select("nombre, consola").eq("id", item.id).eq("vendedor_id", str(_sesion)).limit(1)),
            timeout=5.0
        )
        
        await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").delete().eq("id", item.id).eq("vendedor_id", str(_sesion))),
            timeout=10.0
        )
        
        # 🛡️ FIX AAA: Limpieza de RAM
        if res_old.data:
            async with cache_lock:
                cache_precios_ram.pop(generar_cache_key(res_old.data[0].get("nombre", ""), res_old.data[0].get("consola", "")), None)

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
                supabase.table("inventario").select("id, nombre, consola, precio, stock")
                .eq("id", item.id).eq("vendedor_id", vid_str).limit(1)
            ),
            timeout=10.0
        )
        
        if not res_inv.data: raise HTTPException(status_code=404, detail="Juego no localizado.")
            
        db_item = res_inv.data[0]
        stock_actual = int(db_item.get("stock", 0))
        precio_venta = float(db_item.get("precio", 0.0))
        nombre_real_db = db_item.get("nombre", item.nombre)
        consola_real_db = db_item.get("consola", item.consola)

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
        
        await asyncio.wait_for(
            async_db_execute(
                supabase.table("ventas").insert({
                    "vendedor_id": vid_str,
                    "articulo": nombre_real_db,
                    "consola": consola_real_db,
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
        # Aquí va tu lógica de actualización en Supabase
        await async_db_execute(
            supabase.table('vendedores') # O la tabla donde guardes esto
            .update({'columnas_ordenadas': datos.columnas})
            .eq('id', datos.vendedor_id)
        )
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
    trace_id = str(uuid.uuid4())[:8]
    logger.info(f"📥 [TRACE:{trace_id}] === INICIANDO ORQUESTACIÓN DE MENSAJE ===")
    
    # Declaramos el diccionario multimedia aquí para asegurar su limpieza en el bloque finally
    media_dict_audio = None
    media_dict_img = None
    
    try:
        # 1. 🛡️ ANTI-LOOP Y RESPUESTAS DEL SISTEMA
        if msg.get("from_me") or valor.get("statuses"):
            logger.info(f"♻️ [TRACE:{trace_id}] Mensaje de sistema o retorno. Ignorado.")
            return

        wamid = str(msg.get("id", "")).strip()
        
        # 2. 🛡️ DEDUPLICACIÓN ATÓMICA
        async with wamid_lock:
            if wamid and procesados_recientemente.get(wamid):
                logger.warning(f"♻️ [TRACE:{trace_id}] Webhook duplicado bloqueado: {wamid}")
                return
            if wamid:
                procesados_recientemente[wamid] = True

        # 3. 🛡️ AISLAMIENTO DE TENANT (Multi-Tenant Estricto)
        res_config = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1)),
            timeout=5.0
        )
        
        if not res_config.data:
            logger.error(f"🚨 [TRACE:{trace_id}] Tenant no encontrado para Phone ID: {phone_id_receptor}. Abortando.")
            return

        config_vendedor = res_config.data[0]
        vendedor_actual = str(config_vendedor.get("vendedor_id", ""))
        token_actual = str(config_vendedor.get("meta_token", "")) or WHATSAPP_TOKEN
        nombre_negocio = str(config_vendedor.get("nombre_negocio", "Fantasy Games"))
        
        if not token_actual or not config_vendedor.get("bot_activo", True):
            logger.warning(f"🚫 [TRACE:{trace_id}] Flujo denegado: Bot inactivo o sin token para {vendedor_actual}.")
            return

        telefono_cliente = str(msg.get("from", "")).strip()
        if telefono_cliente.startswith("521"): telefono_cliente = "52" + telefono_cliente[3:]
        if not telefono_cliente: return

        tel_mask = enmascarar_telefono(telefono_cliente)
        
        # 4. 🛡️ RATE LIMIT POR TENANT + TELÉFONO (Anti-Spam Aislado)
        rl_key = f"{vendedor_actual}:{telefono_cliente}"
        async with rate_limit_lock:
            peticiones_recientes = RATE_LIMIT_CLIENTES.get(rl_key, 0)
            if peticiones_recientes > 8:
                logger.warning(f"⚠️ [TRACE:{trace_id}] [RATE LIMIT] Spam detectado de {tel_mask}.")
                return
            RATE_LIMIT_CLIENTES[rl_key] = peticiones_recientes + 1

        tipo_mensaje = str(msg.get("type", "text")).lower()
        texto_entrante = ""

        logger.info(f"📦 [TRACE:{trace_id}] Formato: '{tipo_mensaje}' | Remitente: {tel_mask}")
        
        # 5. 🛡️ EXTRACCIÓN Y VALIDACIÓN MULTIMEDIA
        if tipo_mensaje == "text": 
            texto_entrante = msg.get("text", {}).get("body", "").strip()
        elif tipo_mensaje == "interactive": 
            texto_entrante = msg.get("interactive", {}).get("button_reply", {}).get("title", "").strip()
            
        elif tipo_mensaje in ["image", "audio"]:
            # 🛡️ RATE LIMIT MULTIMEDIA (Protege costos de APIs de IA)
            async with media_limit_lock:
                media_count = RATE_LIMIT_MEDIA.get(rl_key, 0)
                if media_count > 5:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Abuso multimedia detectado de {tel_mask}.")
                    return
                RATE_LIMIT_MEDIA[rl_key] = media_count + 1

            if tipo_mensaje == "audio":
                texto_entrante = "🎙️ [NOTA DE VOZ RECIBIDA - ANALIZANDO AUDIO...]"
                audio_id = msg.get("audio", {}).get("id", "").strip()
                if audio_id:
                    media_dict_audio = await descargar_media_whatsapp_async(audio_id, token_actual)
                    if media_dict_audio and len(media_dict_audio.get("data", b"")) > 15_000_000:
                        logger.warning(f"⚠️ [TRACE:{trace_id}] Audio demasiado pesado (>15MB). Abortando.")
                        return
                        
            elif tipo_mensaje == "image":
                texto_entrante = "📷 [IMAGEN RECIBIDA: Analizando comprobante de pago...]"
                image_id = msg.get("image", {}).get("id", "").strip()
                if image_id:
                    media_dict_img = await descargar_media_whatsapp_async(image_id, token_actual)
                    if not media_dict_img: return
                    
                    data_bytes = media_dict_img.get("data", b"")
                    
                    # 🛡️ FIX AAA: Validación de Tamaño Máximo de Imagen (Anti Decompression Bombs)
                    if len(data_bytes) > 10_000_000: # 10MB Máximo
                        logger.warning(f"🚨 [TRACE:{trace_id}] Imagen excede el límite de 10MB.")
                        return
                    
                    # 🛡️ FIX AAA: Validación de Magic Bytes Reales (Anti PHP/Malware disfrazado)
                    try:
                        img_val = Image.open(io.BytesIO(data_bytes))
                        img_val.verify() # Levanta excepción si el archivo está corrupto o no es imagen real
                    except Exception as img_e:
                        logger.warning(f"🚨 [TRACE:{trace_id}] Archivo de imagen corrupto o malicioso detectado: {img_e}")
                        return
                        
                    mime = media_dict_img.get("mime_type", "")
                    if mime not in ["image/jpeg", "image/png", "image/webp"]:
                        logger.warning(f"🚨 [TRACE:{trace_id}] Formato MIME no permitido: {mime}")
                        return
        else: 
            logger.info(f"ℹ️ [TRACE:{trace_id}] Formato '{tipo_mensaje}' descartado.")
            return

        # 6. 🛡️ GESTIÓN DE CRM (Manejo de Race Conditions con UPSERT Atómico)
        nombre_cliente = valor.get("contacts", [{}])[0].get("profile", {}).get("name", "Cliente")
        res_p = await async_db_execute(supabase.table('prospectos').select('columna, notas').eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_actual))
        columna_actual = res_p.data[0].get("columna", "Bandeja Nueva") if res_p.data else "Bandeja Nueva"

        if not res_p.data:
            try:
                # 🛡️ FIX AAA: Upsert atómico para evitar errores de Unique Constraint bajo concurrencia masiva
                await asyncio.wait_for(
                    async_db_execute(
                        supabase.table('prospectos').upsert({
                            "nombre": nombre_cliente, 
                            "telefono": telefono_cliente, 
                            "columna": columna_actual, 
                            "vendedor_id": vendedor_actual,
                            "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
                        }, on_conflict="telefono,vendedor_id")
                    ),
                    timeout=5.0
                )
            except Exception as db_e:
                logger.warning(f"⚠️ [TRACE:{trace_id}] Excepción controlada en Upsert CRM: {db_e}")

        # Guardado de mensaje en BD (Aislado para no romper el flujo si falla)
        try:
            await asyncio.wait_for(guardar_mensaje_chat(telefono_cliente, vendedor_actual, "USER", texto_entrante), timeout=5.0)
        except Exception as e:
            logger.error(f"⚠️ [TRACE:{trace_id}] Falla aisalada guardando chat: {e}")

        # 7. 🚀 ENRUTAMIENTO CON TIMEOUTS DUROS Y SEMÁFOROS
        if tipo_mensaje in ["text", "interactive", "audio"] and columna_actual != "En Conversacion":
            async with SEMAFORO_IA:
                logger.info(f"🤖 [TRACE:{trace_id}] Despachando a IA Chat...")
                # 🛡️ FIX AAA: Timeout crítico general para evitar que la IA congele el worker
                await asyncio.wait_for(
                    procesar_respuesta_bot(nombre_cliente, telefono_cliente, texto_entrante, columna_actual, config_vendedor, media_dict_audio, id_mensaje_meta=wamid),
                    timeout=90.0
                )
                
        elif tipo_mensaje == "image" and media_dict_img:
            async with SEMAFORO_MEDIA:
                logger.info(f"🛡️ [TRACE:{trace_id}] [DOBERMAN] Analizando finanzas visuales...")
                historial_para_auditor = await obtener_historial_chat(telefono_cliente, vendedor_actual)

                try:
                    auditoria = await asyncio.wait_for(
                        auditar_comprobante_ia(media_dict_img["data"], media_dict_img["mime_type"], nombre_negocio, historial_para_auditor),
                        timeout=45.0
                    )
                except asyncio.TimeoutError:
                    logger.error(f"⏱️ [TRACE:{trace_id}] Timeout excedido en Doberman Vision (45s).")
                    return

                es_pago = auditoria.get("es_pago", False)
                monto = float(auditoria.get("monto_detectado", 0.0)) 

                if es_pago:
                    logger.info(f"💰 [TRACE:{trace_id}] ¡PAGO VÁLIDO! ${monto} MXN.")
                    await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Por Entregar", "verde_exito", "")
                    msg_exito = f"✅ ¡Pago validado por ${monto:.2f} MXN!\nHemos recibido tu comprobante."
                    await disparar_whatsapp_dinamico_async(telefono_cliente, msg_exito, token_actual, phone_id_receptor)
                    await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_exito)
                else:
                    logger.warning(f"🚨 [TRACE:{trace_id}] FRAUDE O ERROR: {auditoria.get('analisis')}")
                    msg_fallo = f"🤖 Mi sistema no pudo validar la imagen.\nDetalle: {auditoria.get('analisis')}\nPor favor envía una foto clara."
                    await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Requiere Asistencia", "verde_alerta", "")
                    await disparar_whatsapp_dinamico_async(telefono_cliente, msg_fallo, token_actual, phone_id_receptor)
                    await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_fallo)

        logger.info(f"🏁 [TRACE:{trace_id}] === OPERACIÓN COMPLETADA EXITOSAMENTE ===")

    except asyncio.TimeoutError:
        logger.error(f"⏱️ [TRACE:{trace_id}] TIMEOUT GLOBAL. Proceso cancelado para liberar worker.")
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] CRÍTICO: Falla del supervisor background: {str(e)}")
    finally:
        # 8. 🧹 RECOLECCIÓN DE BASURA EXPLÍCITA (Fix AAA: Previene Memory Leaks Silenciosos de Archivos Binarios)
        media_dict_audio = None
        media_dict_img = None
        gc.collect()


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

if __name__ == "__main__":
    import uvicorn
    # En producción real (Render/AWS), uvicorn se lanza desde la terminal, no desde aquí.
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), reload=False)
