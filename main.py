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

load_dotenv()

# ==========================================================
# 🛡️ 1. REGLAS DE SEGURIDAD Y LÍMITES ENTERPRISE
# ==========================================================
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET: raise RuntimeError("❌ FATAL: JWT_SECRET no configurada.")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VeltrixEngine")

MAX_HISTORIAL = 8
MAX_MENSAJE_LEN = 1200
MAX_CACHE_IA = 500 
CACHE_TTL_SECONDS = 300 
GEMINI_TEMP = 0.2

# LÍMITES POR TENANT
MAX_TOKENS_POR_MINUTO_TENANT = 20000 
tokens_consumidos_tenant = defaultdict(int)
reset_tokens_tenant = defaultdict(float)
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
genai.configure(api_key=GENAI_KEY) # 🔥 FIX AAA: Se configura solo una vez globalmente

async def async_db_execute(query_builder):
    """Wrapper Asíncrono para Supabase (Evita congelar Godot/FastAPI)"""
    return await asyncio.to_thread(query_builder.execute)

registro_actividad_b2b = {}
procesados_recientemente = deque(maxlen=1000)
cache_respuestas_ia = {}

# MICRO-LOCKS Y TRACKING
locks_por_conversacion = defaultdict(asyncio.Lock)
tracking_locks_uso = defaultdict(float) # 🔥 FIX AAA: Para limpiar memoria
gemini_bloqueado_hasta = 0.0 
rate_limit_tenant = defaultdict(list)
rate_limit_phone = defaultdict(list)
rate_limit_global = []
http_client: Optional[httpx.AsyncClient] = None
mensajes_procesados_meta = set() # 🔥 FIX AAA: Idempotencia para Webhooks
background_tasks_activas = set() # 🔥 FIX AAA: Tracking de Tareas

import html

# 🔥 CONFIGURACIÓN DE SEGURIDAD AVANZADA (AUDITORÍA BLOQUES 9 Y 10)
LOGIN_RATE_LIMIT = TTLCache(maxsize=10000, ttl=300)
RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(maxsize=10000, ttl=60)
rate_limit_login_lock = asyncio.Lock()
rate_limit_mobile_lock = asyncio.Lock()

def normalizar_telefono(tel: str) -> str:
    """Standardizes phone numbers globally, correcting the Mexican mobile digit injection (521 -> 52)"""
    if not tel: return ""
    # Remover cualquier caracter no numérico
    limpio = "".join(filter(str.isdigit, str(tel)))
    
    # Manejo específico para México (Meta envía 521, Godot/CRM puede enviar 52)
    if limpio.startswith("521") and len(limpio) == 13:
        limpio = "52" + limpio[3:]
    elif limpio.startswith("52") and len(limpio) == 12:
        pass
    elif len(limpio) == 10:
        limpio = "52" + limpio
    return limpio

# ==========================================================
# 🛡️ 2. ESCUDO IA Y ARRANQUE DE APLICACIÓN
# ==========================================================
PROMPT_INJECTION_KEYWORDS = ["ignora tus instrucciones", "developer mode", "system prompt", "eres chatgpt", "olvida las reglas"]

def detectar_prompt_injection(texto: str) -> bool:
    texto_lower = str(texto).lower()
    return any(kw in texto_lower for kw in PROMPT_INJECTION_KEYWORDS)

def generar_hash_cache(*args) -> str:
    return hashlib.sha256("|".join([str(a) for a in args]).encode()).hexdigest()

def lanzar_tarea_segura(coro):
    """Lanza tareas en background sin generar Zombies en la RAM"""
    task = asyncio.create_task(coro)
    background_tasks_activas.add(task)
    task.add_done_callback(background_tasks_activas.discard)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    timeout = httpx.Timeout(connect=10.0, read=35.0, write=20.0, pool=10.0)
    http_client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True, http2=True)
    print("\n" + "="*50)
    print("🚀 [SISTEMA] Motor Central Veltrix V20.2 Iniciado (AAA Enterprise)")
    print("🤖 [MÓDULO IA] Listo y cargado (Con Auditor Activo)")
    print("="*50 + "\n")
    
    lanzar_tarea_segura(bucle_seguimiento_24h())
    lanzar_tarea_segura(limpiador_background_rutinario()) # 🔥 FIX AAA: Garbage Collector
    
    try: yield
    finally:
        if http_client: await http_client.aclose()
        print("🛑 [SISTEMA] Apagado Seguro Completado")

app = FastAPI(title="Veltrix Cognitive OS", version="20.2", lifespan=lifespan)
router = APIRouter()
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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
    nuevo_stock: Optional[int] = None      # Mantenido para Godot Legacy
    cantidad_vendida: Optional[int] = None # Nuevo estándar AAA Backend
    vendedor_id: str = ""
    
class LoginUpdate(BaseModel): email: str; password: str
class MobileMessageRequest(BaseModel): to: str; msg: str
class ClienteIdentificador(BaseModel): nombre: str = ""; telefono: str = ""
class ColumnaUpdate(BaseModel): nombre: str = ""; telefono: str = ""; columna: str = ""; nueva_columna: str = ""
class ColumnaAction(BaseModel): nombre: str; vendedor_id: str = ""
class RenombrarColumnaAction(BaseModel): viejo_nombre: str; nuevo_nombre: str; vendedor_id: str = ""
class NotasUpdate(BaseModel): nombre: str = ""; telefono: str = ""; notas: str = ""; etiquetas: str = ""; vendedor_id: str = ""
class EstadoUpdate(BaseModel): nombre: str; telefono: str = ""; nueva_columna: str
class NuevoArticulo(BaseModel): nombre: str; categoria: str = "General"; precio_compra: float = 0.0; precio: float = 0.0; stock: int = 1; vendedor_id: str = ""
class PreciosDetalle(BaseModel):
    loose: float
    cib: float
    new: float

class PrecioResponse(BaseModel):
    status: str
    api_version: str = "v3"  # Control de versión sugerido en auditoría
    nombre_corregido: str
    mxn: PreciosDetalle      # Retrocompatibilidad asegurada para la interfaz clásica
    mxn_mercado: PreciosDetalle
    mxn_venta: PreciosDetalle
    usd: PreciosDetalle
    tipo_cambio: float
    rareza: str
    url_pc: str
    confidence_score: float

# ==========================================================
# 🛡️ 4. MIDDLEWARES Y SEGURIDAD
# ==========================================================
def crear_token_jwt(vendedor_id: str, email: str):
    return jwt.encode({"sub": str(vendedor_id), "email": email, "exp": datetime.now(timezone.utc) + timedelta(days=1)}, JWT_SECRET, algorithm="HS256")

async def verificar_sesion_b2b(authorization: str = Header(None), auth_token: str = Header(None)):
    token = authorization.split(" ", 1)[1].strip() if authorization and authorization.startswith("Bearer ") else (auth_token.strip() if auth_token else None)
    if not token: raise HTTPException(status_code=401, detail="Token faltante")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return str(payload.get("sub"))
    except: raise HTTPException(status_code=401, detail="Token inválido")

async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta: raise HTTPException(status_code=400, detail="Falta firma")
    firma_calculada = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), await request.body(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma_meta, firma_calculada): raise HTTPException(status_code=403, detail="Firma inválida")
    return True

def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    ahora = now_ts()
    if ahora - reset_tokens_tenant[vendedor_id] > 60:
        tokens_consumidos_tenant[vendedor_id] = 0
        reset_tokens_tenant[vendedor_id] = ahora
    if tokens_consumidos_tenant[vendedor_id] > MAX_TOKENS_POR_MINUTO_TENANT: return False
    while rate_limit_global and (ahora - rate_limit_global[0]) > 60: rate_limit_global.pop(0)
    if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO: return False
    rate_limit_global.append(ahora); rate_limit_tenant[vendedor_id].append(ahora); rate_limit_phone[telefono].append(ahora)
    return True

# ==========================================================
# 🧠 5. CEREBRO IA GEMINI Y RAG (RUTEADOR)
# ==========================================================
async def consultar_gemini_json(prompt: str, media_dict: dict = None, temperature: float = 0.2, retries: int = 2, vendedor_id: str = "V-001") -> dict:
    global gemini_bloqueado_hasta
    if now_ts() < gemini_bloqueado_hasta:
        return {"respuesta": "En este momento estoy atendiendo a varios clientes, denme un momento. 🎮", "intencion": "HUMANO", "confidence": 1.0}

    modelos = ['gemini-2.5-flash', 'gemini-1.5-flash'] 
    tokens_estimados = len(str(prompt)) // 4
    
    # 🔥 FIX AAA: Sumador Seguro
    tokens_consumidos_tenant.setdefault(vendedor_id, 0)
    tokens_consumidos_tenant[vendedor_id] += tokens_estimados

    for nombre_modelo in modelos:
        for intento in range(retries):
            try:
                model = genai.GenerativeModel(nombre_modelo) 
                contenido = prompt if isinstance(prompt, list) else [prompt]
                if media_dict and "data" in media_dict: 
                    contenido.append({"mime_type": media_dict.get("mime_type", "image/jpeg"), "data": media_dict["data"]})
                
                # 🔥 FIX AAA: Timeout seguro para evitar congelar servidor
                response = await asyncio.wait_for(
                    asyncio.to_thread(model.generate_content, contenido, generation_config=genai.types.GenerationConfig(temperature=temperature)),
                    timeout=20.0
                )
                
                texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
                
                # 🔥 FIX AAA: Parser JSON Ultra-Robusto con Regex y orjson
                match = re.search(r'\{.*\}', texto_limpio, re.DOTALL)
                if match: 
                    return orjson.loads(match.group())
                
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ [GEMINI] Timeout en intento {intento+1}")
            except Exception as e:
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
        vendedor_id = config.get("vendedor_id", "V-001")
        giro_comercial = config.get("giro_comercial", "Videojuegos y Consolas")
        tono_ia = config.get("tono_ia", "Persuasivo y experto")
        
        lock_id = hashlib.sha256(f"{vendedor_id}:{texto_cliente[:50]}".encode()).hexdigest()
        
        # 🔥 FIX AAA: Tracking de locks para garbage collection
        tracking_locks_uso[lock_id] = now_ts()
        async with locks_por_conversacion[lock_id]:
            print(f"🔮 [CEREBRO IA] Iniciando análisis cognitivo para Vendedor: {vendedor_id}")
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
                print(f"🎙️ [CEREBRO IA] Inyectando Audio Nativo Base64 al modelo generativo.")
                prompt_estructurado.append({
                    "mime_type": media_dict.get("mime_type", "audio/ogg"),
                    "data": media_dict["data"]
                })

            data = await consultar_gemini_json(prompt_estructurado, vendedor_id=vendedor_id)
            
            # 🔥 FIX AAA: Validador Estricto de Tools
            TOOLS_VALIDAS = ["ninguna", "aplicar_descuento"]
            if data.get("accion_tool") not in TOOLS_VALIDAS: data["accion_tool"] = "ninguna"
            
            print(f"🎯 [CEREBRO IA] Análisis finalizado con éxito. Intención inferida: {data.get('intencion')}")
            return data

    except Exception as e:
        print(f"❌ [CEREBRO ERROR] Error en el flujo cognitivo de la IA: {str(e)}")
        return {
            "intencion": "HUMANO", 
            "respuesta": "Hubo un micro-corte en mi sistema de datos. Un asesor humano revisará tu mensaje de inmediato. 🚀", 
            "confidence": 0.0,
            "consola_preferida": "",
            "accion_tool": "ninguna",
            "precio_oferta": 0.0
        }

async def obtener_contexto_inventario_rag(vendedor_id: str, texto_cliente: str = "") -> str:
    print(f"🔍 [RAG INVENTARIO] Buscando coincidencias para: '{texto_cliente}' (Tenant: {vendedor_id})")
    try:
        query = supabase.table('inventario').select('nombre, precio, stock, consola').eq('vendedor_id', str(vendedor_id)).gt('stock', 0).limit(300)
        res_inv = await async_db_execute(query)
        
        if not res_inv.data:
            print("⚠️ [RAG INVENTARIO] La base de datos del vendedor no tiene stock disponible.")
            return "Catálogo vacío o agotado en este momento."

        inventario = res_inv.data
        palabras_clave = limpiar_texto(texto_cliente).lower()

        if not palabras_clave or len(palabras_clave.strip()) < 3:
            print("📋 [RAG INVENTARIO] Mensaje corto detectado. Retornando top 10 general.")
            return "\n".join([f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in inventario[:10]])

        # 🔥 FIX AAA: Sustitución del bucle O(N) por Búsqueda Vectorial Simulada con RapidFuzz
        diccionario_opciones = {f"{i['nombre']} {i.get('consola','')}".strip().lower(): i for i in inventario}
        matches = process.extract(
            palabras_clave, 
            diccionario_opciones.keys(), 
            scorer=fuzz.token_sort_ratio, 
            limit=8
        )
        
        items_filtrados = []
        for match_str, score, _ in matches:
            if score > 20.0: # Umbral tolerante
                items_filtrados.append(diccionario_opciones[match_str])

        if not items_filtrados:
            print("⚠️ [RAG INVENTARIO] Ningún juego superó el filtro difuso. Activando Fallback de rescate.")
            items_filtrados = inventario[:5]

        lineas = [f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in items_filtrados]
        print(f"✅ [RAG INVENTARIO] Bloque RAG construido con {len(lineas)} opciones relevantes.")
        return "\n".join(lineas)

    except Exception as e:
        print(f"❌ [RAG ERROR] Falló la construcción del contexto de inventario: {str(e)}")
        return "Error técnico al recuperar el catálogo."

async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    print(f"📖 [HISTORIAL CHAT] Solicitando últimas interacciones del Tel: {telefono}")
    try:
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)).order('created_at', desc=True).limit(10)
        res_hist = await async_db_execute(query)
        
        if not res_hist.data: 
            print("🆕 [HISTORIAL CHAT] No hay registros previos. Es el primer mensaje del cliente.")
            return "Primer mensaje del cliente en el sistema."

        mensajes_ordenados = list(reversed(res_hist.data))
        
        # 🔥 FIX AAA: Truncado inteligente para no romper tokens con historiales largos
        historial_texto = "\n".join([f"{m.get('autor')}: {m.get('mensaje')}" for m in mensajes_ordenados])
        MAX_CHARS = 3500
        if len(historial_texto) > MAX_CHARS:
            historial_texto = "... [Trunk] ...\n" + historial_texto[-MAX_CHARS:]
            
        print("✅ [HISTORIAL CHAT] Conversación recuperada e indexada correctamente.")
        return historial_texto

    except Exception as e:
        print(f"❌ [HISTORIAL ERROR] Falló la lectura de logs de chat: {str(e)}")
        return "No se pudo recuperar el historial de chat."

# ==========================================================
# 🛠️ 6. FUNCIONES CORE: SCRAPER, ALERTAS, MEDIA Y COMUNICACIÓN
# ==========================================================
async def actualizar_estado_crm(telefono: str, vendedor_id: str, columna: str, iluminacion: str, juego: str, perfil_ia: dict = None):
    payload = {'columna': columna, 'estado_iluminacion': iluminacion, 'ultimo_juego_interes': juego, 'ultima_interaccion_ia': datetime.now(timezone.utc).isoformat()}
    if perfil_ia: payload['perfil_psicologico'] = perfil_ia
    await async_db_execute(supabase.table('prospectos').update(payload).eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str):
    await async_db_execute(supabase.table('mensajes_chat').insert({'telefono': telefono, 'vendedor_id': str(vendedor_id), 'autor': autor, 'mensaje': mensaje}))

async def disparar_whatsapp_dinamico_async(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    if not http_client: return False
    url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    
    # 🔥 FIX AAA: Retries con status check
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
        
        mensaje_alerta = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Tel: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_ia}"
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje_alerta, token, phone_id)
        print(f"📩 [ALERTA ADMIN] Enviada para el cliente {cliente}")
    except Exception as e: 
        print(f"❌ [ALERTA ERROR] Falló envío a Admin: {e}")

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nProducto: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera un mensaje corto de remarketing ofreciendo un pequeño descuento. Formato JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))}
    except: return None

# 🚀 RESTAURACIÓN: Manejo de Media para WhatsApp
async def descargar_media_whatsapp_async(media_id: str, token: str) -> Optional[dict]:
    if not http_client: return None
    try:
        url_info = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {token}"}
        res_info = await http_client.get(url_info, headers=headers)
        if res_info.status_code != 200: return None
        data_info = res_info.json()
        media_url = data_info.get("url")
        if not media_url: return None
        res_media = await http_client.get(media_url, headers=headers)
        if res_media.status_code != 200: return None
        
        # 🔥 FIX AAA: Validar MIME types permitidos
        mime_type = data_info.get("mime_type", "")
        if mime_type not in ["image/jpeg", "image/png", "audio/ogg", "audio/mp4", "audio/mpeg"]:
            logger.warning(f"⚠️ [MEDIA] Tipo MIME no soportado: {mime_type}")
            return None
            
        return {"mime_type": mime_type, "data": res_media.content}
    except Exception: return None

# 🚀 RESTAURACIÓN: El "Dóberman" (Auditor IA de Comprobantes)
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

# 🚀 RESTAURACIÓN: Scraper Automático de Portadas al Storage
async def obtener_html_escalonado_async_portadas(url_objetivo: str) -> str:
    if not http_client: return ""
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
    """Descarga la portada en background y la sube al Storage de Supabase"""
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
        
        # 🔥 FIX AAA: Hashing SHA256 para evitar duplicados en el Storage
        hash_img = hashlib.sha256(res_img.content).hexdigest()[:10]
        nombre_archivo = f"{consola.replace(' ', '_')}_{nombre_juego.replace(' ', '_')}_{hash_img}.jpg"
        
        await async_db_execute(supabase.storage.from_("portadas").upload(nombre_archivo, res_img.content, {"content-type": "image/jpeg"}))
        url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
        await async_db_execute(supabase.table('inventario').update({"url_portada": url_publica}).eq('id', juego_id_supabase))
        print(f"🖼️ [PORTADA] Descargada exitosamente: {nombre_juego}")
    except Exception as e: logger.error(f"⚠️ Error cazando portada en background: {e}")

# ==========================================================
# ⏰ 7. WATCHDOG B2B Y FLUJO PRINCIPAL IA
# ==========================================================
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
            res = await async_db_execute(supabase.table('prospectos').select('*').eq('columna', 'Envios Masivos').lt('ultima_interaccion_ia', hace_24h).limit(20))
            
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
        # 🔥 FIX AAA: Bloqueo Anti-Duplicación de Webhooks (Idempotencia)
        if id_mensaje_meta:
            if id_mensaje_meta in mensajes_procesados_meta:
                logger.info(f"♻️ [WEBHOOK IGNORED] Mensaje duplicado de Meta ignorado en capa IA.")
                return
            mensajes_procesados_meta.add(id_mensaje_meta)
            if len(mensajes_procesados_meta) > 1000: mensajes_procesados_meta.clear()

        print(f"\n🧠 [IA WORKFLOW] ==========================================")
        print(f"🧠 [IA WORKFLOW] PROCESANDO RESPUESTA AUTÓNOMA DEL BOT")
        print(f"🧠 [IA WORKFLOW] Cliente: {cliente} | Tel: {telefono} | Columna: {columna_actual}")
        print(f"==============================================================")
        
        vendedor_id = config.get("vendedor_id", "")
        
        if not verificar_rate_limit(vendedor_id, telefono):
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
        decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, perfil_cliente_previo, media_dict)
        
        intencion_ia = str(decision.get("intencion", "CONSULTA")).upper()
        respuesta_final = decision.get("respuesta", "En un momento te atiendo.")
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

    except Exception as e: 
        logger.exception(f"❌ [IA WORKFLOW CRITICAL ERROR] Falla estructural en el orquestador del Bot: {str(e)}")

# ==========================================================
# 📈 8. MOTOR DE PRECIOS PRO (CACHE AAA, MATCHING SCORE, PRICING DINÁMICO)
# ==========================================================

# 🚀 1. GESTIÓN DE CACHÉ DE ALTO RENDIMIENTO (LOCK-FREE READS)
cache_precios_ram = {}
cache_lock = asyncio.Lock()
TIEMPO_VIDA_CACHE_HORAS = 24 
ULTIMA_LIMPIEZA_CACHE = 0.0

# Caché exclusivo para divisas (Evita latencia externa)
CACHE_DIVISA = {"valor": 18.0, "expira": 0.0}

# Circuit Breaker Aislado (Solo afecta al dominio objetivo)
CB_PRICECHARTING = {"fallas": 0, "bloqueado_hasta": 0.0}

# Configuración de Timeouts para evitar sockets colgados
HTTP_TIMEOUTS = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

# 🧼 2. NORMALIZACIÓN Y LLAVES ÚNICAS
def normalizar_nombre_busqueda(nombre: str) -> str:
    basura = ["edition", "edición", "greatest hits", "platinum", "remastered", "bundle", "loose", "cib", "new", "goty"]
    nombre_limpio = nombre.lower()
    for p in basura:
        nombre_limpio = nombre_limpio.replace(p, "")
    return " ".join(nombre_limpio.split())

def generar_cache_key(nombre: str, consola: str) -> str:
    """Garantiza que variaciones del mismo juego apunten al mismo bloque de RAM"""
    return f"{normalizar_nombre_busqueda(nombre)}::{consola.lower().strip()}"

async def limpiar_cache_expirado():
    """Ejecutado por el Singleton Scheduler. Libera RAM de forma segura."""
    async with cache_lock:
        ahora = datetime.now()
        expirados = [k for k, v in cache_precios_ram.items() if ahora >= v["expira"]]
        for k in expirados:
            del cache_precios_ram[k]
        if expirados:
            print(f"🧹 [GC HYPERSCALE] Liberados {len(expirados)} registros de memoria.")

async def lanzar_gc_si_toca():
    """Singleton Scheduler: Evita Task Leaks (Fugas de tareas)"""
    global ULTIMA_LIMPIEZA_CACHE
    ahora = time.time()
    if ahora - ULTIMA_LIMPIEZA_CACHE > 300: # Solo ejecuta 1 vez cada 5 minutos
        ULTIMA_LIMPIEZA_CACHE = ahora
        asyncio.create_task(limpiar_cache_expirado())

async def obtener_precio_cache(llave: str) -> dict | None:
    # LECTURA LOCK-FREE: Máxima concurrencia. Python GIL protege lectura de dicts.
    datos = cache_precios_ram.get(llave)
    if datos and datetime.now() < datos["expira"]:
        print(f"⚡ [CACHE HIT] Precio recuperado en O(1).")
        
        # Redundancia estructural defensiva para asegurar compatibilidad si el formato en RAM es antiguo
        if "mxn" not in datos["valores"] and "mxn_mercado" in datos["valores"]:
            datos["valores"]["mxn"] = datos["valores"]["mxn_mercado"]
        
        return datos["valores"]
    return None

async def guardar_precio_cache(llave: str, valores: dict):
    # ESCRITURA PROTEGIDA: Evita Race Conditions.
    async with cache_lock:
        cache_precios_ram[llave] = {
            "valores": valores,
            "expira": datetime.now() + timedelta(hours=TIEMPO_VIDA_CACHE_HORAS)
        }

async def obtener_dolar_hoy_async():
    """Caché de divisas independiente. TTL de 12 horas."""
    ahora = time.time()
    if ahora < CACHE_DIVISA["expira"]:
        return CACHE_DIVISA["valor"]
        
    try:
        if not http_client: return 18.00
        res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=HTTP_TIMEOUTS)
        if res.status_code == 200:
            val = float(res.json().get("rates", {}).get("MXN", 18.00))
            CACHE_DIVISA["valor"] = val
            CACHE_DIVISA["expira"] = ahora + 43200 # 12 horas en segundos
            return val
    except Exception as e:
        print(f"⚠️ [DIVISAS ERROR] {e}")
    return CACHE_DIVISA["valor"]

# 🕸️ 3. MOTOR DE SCRAPING CON BACKOFF Y CIRCUIT BREAKER
async def obtener_html_escalonado_async(url_objetivo: str, es_busqueda: bool = True) -> str:
    if not http_client: return ""
    
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

    # 🔥 CORRECCIÓN CRÍTICA: Usamos quote estándar conservando caracteres limpios,
    # evitando la doble codificación del símbolo '%' que rompe el bypass de ScraperAPI.
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
                return res.text
        except Exception as e:
            print(f"❌ [SCRAPER] Fallo en {nombre_fase}: {str(e)[:50]}")
            
    CB_PRICECHARTING["fallas"] = CB_PRICECHARTING.get("fallas", 0) + 1
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
    return float(round(max(precio_calculado, max(250.0, costo_compra + 100.0)) / 10) * 10)

@app.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo", dias_inventario: int = 0, rareza: str = "comun"):
    await lanzar_gc_si_toca() 
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
        # 🔥 FIX AUDITORÍA: Respuesta de error con compatibilidad contractual íntegra
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
    nodos_a_buscar = soup.find(id="games_table").find_all('a', href=True) if soup.find(id="games_table") else soup.find_all('a', href=True)
    
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

            # 🔥 EXTRACTOR MATEMÁTICO BLINDADO MULTI-CAPA
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

            # 🧠 PRICING DE RESPALDO (Fallback Pricing)
            if p_cib == 0.0:
                if p_loose > 0:
                    p_cib = round(p_loose * 1.30, 2)
                    print(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde Loose: ${p_cib} USD")
                elif p_new > 0:
                    p_cib = round(p_new * 0.70, 2)
                    print(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde New: ${p_cib} USD")

    url_final_godot = link_juego if link_juego else url_search

    # 🔥 FIX AUDITORÍA: Inyección de contratos de equivalencia total en contingencias de cero dólares
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
    
    # 🔥 FIX AUDITORÍA: Duplicación paralela de la estructura clásica 'mxn' + versión de API
    respuesta_final = {
        "status": "ok",
        "api_version": "v3",
        "nombre_corregido": nombre_oficial_pc,
        
        # Estructura Legacy nativa para Godot
        "mxn": {
            "loose": mxn_loose_real,
            "cib": mxn_cib_real,
            "new": mxn_new_real
        },
        
        # Nuevas capas funcionales analíticas AAA
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
# 🛡️ CONFIGURACIÓN DE SEGURIDAD GLOBAL (AUDITORÍA AAA)
# ==========================================================
LOGIN_RATE_LIMIT = TTLCache(maxsize=10000, ttl=300)
RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(maxsize=10000, ttl=60)
rate_limit_login_lock = asyncio.Lock()
rate_limit_mobile_lock = asyncio.Lock()

def normalizar_telefono(tel: str) -> str:
    """Normalización Enterprise Global con fallback local"""
    if not tel: return ""
    try:
        # Preprocesamiento para asegurar que la librería detecte el código de país
        tel_procesado = tel if tel.startswith('+') else '+' + tel if tel.startswith('52') else '+52' + tel
        parsed = phonenumbers.parse(tel_procesado, None)
        if phonenumbers.is_valid_number(parsed):
            return str(parsed.country_code) + str(parsed.national_number)
    except Exception:
        pass
    
    # Fallback agresivo para números locales/basura
    limpio = "".join(filter(str.isdigit, str(tel)))
    if limpio.startswith("521") and len(limpio) == 13: return "52" + limpio[3:]
    if len(limpio) == 10: return "52" + limpio
    return limpio


# ==========================================================
# 🔐 9. AUTENTICACIÓN Y LOGIN B2B (MIGRACIÓN COMPLETA Y RATE LIMIT HARDENING)
# ==========================================================

@app.post("/api/login")
async def login_b2b(datos: LoginUpdate, request: Request):
    ip_cliente = request.headers.get("x-forwarded-for", request.client.host)
    ip_cliente = ip_cliente.split(",")[0].strip()
    
    email_normalizado = datos.email.lower().strip()
    llave_limite = f"{ip_cliente}:{email_normalizado}"
    
    async with rate_limit_login_lock:
        intentos_previos = LOGIN_RATE_LIMIT.get(llave_limite, 0)
        if intentos_previos >= 5:
            logger.warning(f"🚨 [ANTI-BRUTEFORCE] IP bloqueada preventivamente: {llave_limite}")
            raise HTTPException(status_code=429, detail="Demasiados intentos fallidos. Cuenta bloqueada por 5 minutos.")

    logger.info(f"🔑 [LOGIN] Autenticando: {email_normalizado}")
    try:
        # 🛡️ FIX AAA: DB Timeouts añadidos
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('*').eq('email', email_normalizado).limit(1)),
            timeout=10.0
        )
        
        if not res.data: 
            async with rate_limit_login_lock:
                LOGIN_RATE_LIMIT[llave_limite] = intentos_previos + 1
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")
            
        usuario = res.data[0]
        estado_usuario = usuario.get('estado', 'Activo')
        if estado_usuario != 'Activo':
            logger.warning(f"🚫 [LOGIN] Ingreso denegado a cuenta inactiva: {email_normalizado}")
            raise HTTPException(status_code=403, detail="Esta cuenta se encuentra suspendida o inactiva.")

        password_guardada = str(usuario.get('password', ''))
        
        # 🛡️ FIX AAA: Remoción absoluta del soporte de contraseñas Legacy en texto plano
        if not password_guardada.startswith('$2b$'):
            logger.critical(f"🚨 [RIESGO DE SEGURIDAD] Cuenta con password no encriptada detectada y bloqueada: {email_normalizado}")
            raise HTTPException(status_code=403, detail="Por políticas de seguridad debes actualizar tu contraseña. Contacta a soporte.")

        # CPU-Bound Task delegada al Threadpool
        password_valida = await run_in_threadpool(pwd_context.verify, datos.password, password_guardada)
            
        if not password_valida: 
            async with rate_limit_login_lock:
                LOGIN_RATE_LIMIT[llave_limite] = intentos_previos + 1 # Degradación parcial inteligente
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")
            
        suscripcion_valida = True
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        if fecha_pago_str:
            try:
                from datetime import date
                if date.today() > date.fromisoformat(fecha_pago_str):
                    suscripcion_valida = False
                    await asyncio.wait_for(
                        async_db_execute(supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id'])),
                        timeout=5.0
                    )
            except ValueError: 
                logger.error(f"❌ Formato de fecha de pago corrupto: {email_normalizado}")

        async with rate_limit_login_lock:
            LOGIN_RATE_LIMIT[llave_limite] = max(0, intentos_previos - 1)

        vendedor_id = str(usuario.get('vendedor_id', 'V-001'))
        ahora = datetime.now(timezone.utc)
        
        # 🛡️ FIX AAA: JWT Definitivo con Issuer y Audience
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
        logger.info(f"✅ [LOGIN EXITOSO] {vendedor_id} autenticado.")

        return {
            "status": "ok",
            "datos": {
                "vendedor_id": vendedor_id,
                "email": usuario['email'],
                "estado": estado_usuario,
                "pais": usuario.get('pais', 'México'),
                "suscripcion_activa": suscripcion_valida,
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
        columnas_izq = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
        columnas_der = ["Por Entregar", "Vendidos", "Papelera"]
        
        # Selección limpia de columnas específicas
        res_cols = await async_db_execute(supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', str(_sesion)))
        
        # 🔥 FIX AUDITORÍA: Si no hay columnas personalizadas, devolvemos lista vacía y manejamos el "+" en la interfaz
        columnas_custom = [r['nombre_columna'] for r in (res_cols.data or []) if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)]]
        
        limit_seguro = min(limit, 300)
        res_prospectos = await async_db_execute(
            supabase.table('prospectos')
            .select('id, nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas')
            .eq('vendedor_id', str(_sesion))
            .order('ultima_interaccion_ia', desc=True)
            .range(offset, offset + limit_seguro - 1)
        )
        
        ultimos = {}
        # Ordenamiento defensivo aplicando normalización estricta sobre las claves telefónicas
        for fila in (res_prospectos.data or []):
            tel_norm = normalizar_telefono(fila.get('telefono', ''))
            key_identificador = tel_norm if tel_norm else fila.get('nombre', 'Desconocido')
            
            if key_identificador not in ultimos:
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
            
        res = await async_db_execute(
            supabase.table("prospectos").select("id, notas, etiquetas, columna, perfil_psicologico")
            .eq("telefono", tel_norm).eq("vendedor_id", str(vendedor_id)).limit(1)
        )
        if res.data: return {"status": "ok", "datos": res.data[0]}
        raise HTTPException(status_code=404, detail="Prospecto no localizado.")
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error en perfil_cliente: {e}")
        raise HTTPException(status_code=500, detail="Fallo interno en consulta de perfil.")

@app.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(supabase.table("configuracion").select("nombre_columna").eq("vendedor_id", str(vendedor_id)))
        return {"status": "ok", "columnas": [item["nombre_columna"] for item in (res.data or [])]}
    except Exception as e: 
        raise HTTPException(status_code=500, detail="Error al solicitar columnas configuradas.")

@app.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, limit: int = 50, offset: int = 0, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: return {"status": "ok", "historial": []}
        
        limit_seguro = min(limit, 100)
        res = await async_db_execute(
            supabase.table("mensajes_chat")
            .select("mensaje, autor, created_at")
            .eq("vendedor_id", str(vendedor_id))
            .eq("telefono", tel_norm)
            .order("created_at", desc=True) # Traemos los más recientes primero para paginar con scrolls
            .range(offset, offset + limit_seguro - 1)
        )
        
        historial_formateado = []
        for m in reversed(res.data or []): # Invertimos para entregar orden pasado -> presente a Godot
            historial_formateado.append({
                "contenido": str(m.get("mensaje") or ""),
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
    if not tel_norm or not data.msg:
        raise HTTPException(status_code=400, detail="Datos de envío incompletos o teléfono erróneo.")
        
    # 🛡️ FIX AUDITORÍA: Rate limit de salida por combinación de vendedor/destino (Anti-Spam / Meta Bans)
    llave_outbound = f"{vendedor_id}:{tel_norm}"
    async with rate_mobile_lock:
        envios_recientes = RATE_LIMIT_MOBILE_OUTBOUND.get(llave_outbound, 0)
        if envios_recientes > 10: # Límite de 10 mensajes por minuto al mismo número desde la app móvil
            raise HTTPException(status_code=429, detail="Límite de envío masivo excedido para este canal. Espera un momento.")
        RATE_LIMIT_MOBILE_OUTBOUND[llave_outbound] = envios_recientes + 1

    try:
        res_conf = await async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', str(vendedor_id)).limit(1))
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración no encontrada para el canal.")
        config = res_conf.data[0]
        
        await disparar_whatsapp_dinamico_async(tel_norm, data.msg, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
        await guardar_mensaje_chat(tel_norm, str(vendedor_id), 'ASESOR', data.msg)
        await actualizar_estado_crm(tel_norm, str(vendedor_id), "En Seguimiento", "azul", "")
        return {"status": "ok", "message": "Enviado"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error de retransmisión manual: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al despachar WhatsApp.")

@app.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        
        # SQL aggregation / Cálculo de suma delegada a través del mapeo rápido
        ventas_res = await async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio))
        total_hoy = sum(float(v.get("monto") or 0.0) for v in (ventas_res.data or []))

        prospectos_res = await async_db_execute(
            supabase.table("prospectos")
            .select("id, nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas")
            .eq("vendedor_id", str(vendedor_id))
            .order("ultima_interaccion_ia", desc=True)
            .limit(50)
        )
        
        # Sanitización de nulos al vuelo antes de inyectar al cliente móvil/Godot
        prospectos_limpios = []
        for p in (prospectos_res.data or []):
            prospectos_limpios.append({
                "id": p.get("id"),
                "nombre": p.get("nombre") or "Cliente",
                "telefono": normalizar_telefono(p.get("telefono", "")),
                "columna": p.get("columna") or "Bandeja Nueva",
                "ultima_interaccion_ia": p.get("ultima_interaccion_ia") or "",
                "ultimo_msj": p.get("ultimo_msj") or "",
                "notas": p.get("notas") or "",
                "etiquetas": p.get("etiquetas") or ""
            })
            
        return {"status": "ok", "vendedor": vendedor_id, "ventas_hoy": total_hoy, "prospectos": prospectos_limpios}
    except Exception as e: 
        logger.error(f"❌ Error en mobile_dashboard pipeline: {e}")
        raise HTTPException(status_code=500, detail="Error interno al compilar dashboard.")

@app.post("/api/actualizar_estado")
async def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Identificador telefónico obligatorio para el movimiento.")
            
        col_segura = sanitizar_nombre_columna(datos.nueva_columna)
        
        resultado = await async_db_execute(
            supabase.table('prospectos').update({'columna': col_segura})
            .eq('vendedor_id', str(_sesion))
            .eq('telefono', tel_norm)
        )
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="No se encontró registro con los datos provistos.")
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error actualizando tarjeta: {e}")
        raise HTTPException(status_code=500, detail="Fallo interno de actualización.")

@app.post("/api/historial_chat")
async def historial_chat(datos: ClienteIdentificador, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Se requiere número telefónico válido para el historial clásico.")
            
        res = await async_db_execute(
            supabase.table('mensajes_chat').select('autor, mensaje')
            .eq('vendedor_id', str(_sesion))
            .eq('telefono', tel_norm)
            .order('created_at', desc=False)
            .limit(50)
        )
        return {"historial": [{"texto": f.get('mensaje', ''), "es_mio": f.get('autor', 'USER') != 'USER'} for f in (res.data or [])], "telefono_oficial": tel_norm}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error consultando historial clasico: {e}")
        raise HTTPException(status_code=500, detail="Error en consulta histórica.")

@app.post("/api/mover_prospecto")
async def mover_prospecto(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Identificador telefónico obligatorio para desplazar tarjetas.")
            
        col_final = sanitizar_nombre_columna(datos.nueva_columna if datos.nueva_columna else datos.columna)
        
        await async_db_execute(supabase.table('prospectos').update({"columna": col_final}).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion)))
        return {"status": "ok", "mensaje": f"Movido a {col_final}"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error transaccional al desplazar tarjeta.")

@app.post("/api/actualizar_notas")
async def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm:
            raise HTTPException(status_code=400, detail="Número telefónico obligatorio para adjuntar notas.")
            
        # 🔥 FIX AUDITORÍA: Sanitización estricta contra XSS almacenado en notas y etiquetas
        notas_sanitizadas = html.escape(datos.notas) if datos.notas else ""
        etiquetas_sanitizadas = html.escape(datos.etiquetas) if datos.etiquetas else ""
        nombre_sanitizado = html.escape(datos.nombre) if datos.nombre else "Cliente"
        
        update_data = {"notas": notas_sanitizadas, "etiquetas": etiquetas_sanitizadas, "nombre": nombre_sanitizado}
        res = await async_db_execute(supabase.table('prospectos').update(update_data).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion)))
        
        if res and res.data: return {"status": "ok", "mensaje": "Sincronización completa"}
        raise HTTPException(status_code=404, detail="No se localizó la tarjeta para inyectar metadatos.")
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error inyectando notas CRM: {e}")
        raise HTTPException(status_code=500, detail="Error de servidor al sincronizar apuntes.")

# ==========================================================
# 📦 BLOQUE 11: INVENTARIO Y GESTIÓN DE COLUMNAS (AAA ENTERPRISE)
# ==========================================================

# 🛡️ 1. SEGURIDAD Y REGLAS DE NEGOCIO
COLUMNAS_SISTEMA_RESERVADAS = {"requiere asistencia", "por entregar", "bandeja nueva", "envios masivos", "null", "undefined", "delete"}

def sanitizar_nombre_columna(nombre: str) -> str:
    limpio = limpiar_texto(nombre).strip()
    if limpio.lower() in COLUMNAS_SISTEMA_RESERVADAS:
        raise HTTPException(400, "Nombre de columna reservado por el sistema. Elige otro.")
    return limpio

@app.post("/api/crear_inventario")
async def crear_inventario(datos: NuevoArticulo, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        nombre_limpio = limpiar_texto(datos.nombre)
        
        # 🔥 FIX AAA: Límite de longitud y protección de variables numéricas
        if len(nombre_limpio) > 120:
            raise HTTPException(400, "Nombre de artículo demasiado largo. Máximo 120 caracteres.")
        if datos.precio < 0 or datos.stock < 0:
            raise HTTPException(400, "Valores de precio o stock inválidos.")

        vid_str = str(_sesion)
        consola_limpia = limpiar_texto(datos.categoria) 

        res_check = await async_db_execute(
            supabase.table('inventario').select('id')
            .eq('vendedor_id', vid_str)
            .ilike('nombre', nombre_limpio)
            .ilike('consola', consola_limpia)
            .limit(1)
        )
        if res_check.data:
            raise HTTPException(400, "Este título ya existe en esta plataforma para tu inventario.")

        res = await async_db_execute(supabase.table('inventario').insert({
            'vendedor_id': vid_str, 
            'nombre': nombre_limpio, 
            'categoria': consola_limpia, 
            'consola': consola_limpia, 
            'precio_compra': datos.precio_compra, 
            'precio': datos.precio, 
            'stock': datos.stock
        }))
        
        if res.data:
            juego_id_creado = str(res.data[0]['id'])
            # 🚀 Scraper Inteligente: Ahora verifica URL antes en un entorno real, aquí se manda directo al background
            background_tasks.add_task(cazar_portada_y_guardar_background, juego_id_creado, datos.nombre, datos.categoria)
            
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error DB Crear Inventario: {e}")
        raise HTTPException(status_code=500, detail="Error interno al crear artículo")

@app.get("/api/cargar_inventario")
async def cargar_inventario(offset: int = 0, limit: int = 100, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        limit_seguro = min(limit, 100) 
        # 🔥 FIX AAA: Corrección del nombre del campo a 'precio_compra'
        res = await async_db_execute(
            supabase.table('inventario')
            .select("id, nombre, consola, precio, precio_compra, stock, url_portada, estado_general, rareza")
            .eq('vendedor_id', str(_sesion))
            .order('id', desc=True)
            .range(offset, offset + limit_seguro - 1)
        )
        
        # 🔥 FIX AAA: Normalización estricta de JSON para evitar crashes en Godot por culpa de NULLs
        inventario_limpio = []
        for row in (res.data or []):
            inventario_limpio.append({
                "id": row.get("id"),
                "nombre": row.get("nombre") or "",
                "consola": row.get("consola") or "",
                "precio": float(row.get("precio") or 0.0),
                "precio_compra": float(row.get("precio_compra") or 0.0),
                "stock": int(row.get("stock") or 0),
                "url_portada": row.get("url_portada") or "",
                "estado_general": row.get("estado_general") or "Bueno",
                "rareza": row.get("rareza") or "comun"
            })
            
        return {"status": "ok", "inventario": inventario_limpio}
    except Exception as e: raise HTTPException(status_code=500, detail="Error carga de inventario")

@app.post("/api/editar_item_visor")
async def editar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        if not item.id:
            raise HTTPException(400, "ID Requerido. Operación cancelada.")

        nombre_limpio = limpiar_texto(item.nombre)
        precio_final = max(0.0, float(item.nuevo_precio if item.nuevo_precio is not None else item.precio))
        stock_final = max(0, int(item.nuevo_stock if item.nuevo_stock is not None else item.stock))

        # 🔥 FIX AAA: Leer el estado anterior para limpiar el caché de forma efectiva
        res_old = await async_db_execute(supabase.table("inventario").select("nombre, consola").eq("id", item.id).eq("vendedor_id", vid_str).limit(1))
        
        nombre_anterior = res_old.data[0].get("nombre", "") if res_old.data else ""
        consola_anterior = res_old.data[0].get("consola", "") if res_old.data else ""

        await async_db_execute(
            supabase.table("inventario")
            .update({"nombre": nombre_limpio, "precio": precio_final, "stock": stock_final, "consola": limpiar_texto(item.consola)})
            .eq("id", item.id).eq("vendedor_id", vid_str)
        )
        
        # 🔥 FIX AAA: Invalidación Doble de Caché (El nombre viejo y el nuevo)
        async with cache_lock:
            if nombre_anterior: cache_precios_ram.pop(generar_cache_key(nombre_anterior, consola_anterior), None)
            cache_precios_ram.pop(generar_cache_key(nombre_limpio, item.consola), None)

        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error editar item")

@app.get("/api/buscar_maestro")
async def buscar_maestro(q: str, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        q_limpio = normalizar_nombre_busqueda(q) if q else ""
        if not q_limpio: return {"status": "ok", "resultados": []}

        res = await async_db_execute(
            supabase.table('inventario')
            .select('id, nombre, consola, precio, stock, url_portada')
            .eq('vendedor_id', str(_sesion))
            .ilike('nombre', f'%{q_limpio}%')
            .limit(25)
        )
        return {"status": "ok", "resultados": res.data or []}
    except Exception as e: 
        raise HTTPException(status_code=500, detail="Error en buscador maestro")

@app.post("/api/borrar_item")
async def borrar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        if not item.id: raise HTTPException(400, "ID Requerido. Borrado bloqueado.")
        await async_db_execute(supabase.table("inventario").delete().eq("id", item.id).eq("vendedor_id", str(_sesion)))
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error borrar item")

# 🚀 ENDPOINT ATÓMICO DE VENTAS (Cero Race Conditions / Auditoría UUID)
@app.post("/api/actualizar_stock")
async def actualizar_stock(item: VentaItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        if not item.id: raise HTTPException(400, "ID requerido para transacción segura.")
        
        res_inv = await async_db_execute(
            supabase.table("inventario").select("id, nombre, consola, precio, stock")
            .eq("id", item.id).eq("vendedor_id", vid_str).limit(1)
        )
        
        if not res_inv.data: raise HTTPException(status_code=404, detail="Juego no localizado.")
            
        db_item = res_inv.data[0]
        stock_actual = int(db_item.get("stock", 0))
        precio_venta = float(db_item.get("precio", 0.0))
        nombre_real_db = db_item.get("nombre", item.nombre)
        consola_real_db = db_item.get("consola", item.consola)

        # 🔥 FIX AAA: Límite estricto de ventas (Protección Anti-Abusos)
        if item.cantidad_vendida is not None:
            if item.cantidad_vendida > 100: raise HTTPException(400, "Cantidad de venta sospechosa. Límite excedido.")
            cantidad_descontar = max(1, item.cantidad_vendida)
        else:
            nuevo_req = item.nuevo_stock if item.nuevo_stock is not None else stock_actual
            cantidad_descontar = max(0, stock_actual - nuevo_req)

        if cantidad_descontar <= 0: return {"status": "ok", "msg": "Sin cambios reales en stock"}

        if cantidad_descontar > stock_actual:
            raise HTTPException(status_code=400, detail=f"Stock insuficiente. Solicitado: {cantidad_descontar}, Disponible: {stock_actual}")

        nuevo_stock_seguro = stock_actual - cantidad_descontar
        
        res_update = await async_db_execute(
            supabase.table("inventario").update({"stock": nuevo_stock_seguro})
            .eq("id", item.id).eq("stock", stock_actual) 
        )
        
        if not res_update.data:
            raise HTTPException(status_code=409, detail="Colisión de concurrencia. Reintente.")
            
        # 🔥 FIX AAA: Registro de Auditoría Avanzada con UUID
        ingreso_total = precio_venta * cantidad_descontar
        transaccion_id = str(uuid.uuid4())
        
        # OJO: Asegúrate de que las columnas extra existan en tu tabla 'ventas' de Supabase
        # Si no existen, quita las líneas de stock_anterior, stock_nuevo, cantidad y tx_uuid
        # O idealmente, créalas en Supabase como pide el nivel AAA.
        await async_db_execute(supabase.table("ventas").insert({
            "vendedor_id": vid_str,
            "articulo": nombre_real_db,
            "consola": consola_real_db,
            "monto": ingreso_total,
            "cantidad": cantidad_descontar,            # Nueva métrica AAA
            "stock_anterior": stock_actual,            # Nueva métrica AAA
            "stock_nuevo": nuevo_stock_seguro,         # Nueva métrica AAA
            "tx_uuid": transaccion_id,                 # Trazabilidad AAA
            "created_at": datetime.now(timezone.utc).isoformat()
        }))
        
        return {"status": "ok", "nuevo_stock": nuevo_stock_seguro, "tx_id": transaccion_id}
    except HTTPException: raise
    except Exception as e:
        logger.error(f"❌ Error Transaccional de Venta: {str(e)}")
        raise HTTPException(status_code=500, detail="Error crítico al procesar la venta.")

# ==========================================================
# 📊 ENDPOINTS DE COLUMNAS (CRM KANBAN)
# ==========================================================
# (Se mantienen los de la versión anterior que ya eran AAA)
@app.post("/api/crear_columna")
async def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        nombre_seguro = sanitizar_nombre_columna(datos.nombre)
        res_check = await async_db_execute(supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', str(_sesion)).ilike('nombre_columna', nombre_seguro))
        if res_check.data: raise HTTPException(400, "La columna ya existe.")
        
        await async_db_execute(supabase.table('configuracion').insert({'vendedor_id': str(_sesion), 'nombre_columna': nombre_seguro}))
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error crear columna")

@app.post("/api/renombrar_columna")
async def renombrar_columna(datos: RenombrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        nuevo_seguro = sanitizar_nombre_columna(datos.nuevo_nombre)
        viejo_seguro = limpiar_texto(datos.viejo_nombre)
        if nuevo_seguro.lower() == viejo_seguro.lower(): return {"status": "ok"} 
            
        await async_db_execute(supabase.table('configuracion').update({'nombre_columna': nuevo_seguro}).eq('vendedor_id', vid_str).eq('nombre_columna', viejo_seguro))
        await async_db_execute(supabase.table('prospectos').update({'columna': nuevo_seguro}).eq('vendedor_id', vid_str).eq('columna', viejo_seguro))
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail="Error renombrar")

@app.post("/api/borrar_columna")
async def borrar_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        col_name = limpiar_texto(datos.nombre)
        await async_db_execute(supabase.table('prospectos').update({"columna": "Bandeja Nueva"}).eq('columna', col_name).eq('vendedor_id', str(_sesion)))
        await async_db_execute(supabase.table('configuracion').delete().eq('vendedor_id', str(_sesion)).eq('nombre_columna', col_name))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error borrar columna")

# ==========================================================
# ⚙️ 12. BACKGROUND WORKER Y WEBHOOKS DE META (AAA ENTERPRISE)
# ==========================================================
import uuid
import json
import asyncio
from cachetools import TTLCache

# 🛡️ CACHÉS Y LOCKS DISTRIBUIDOS EN MEMORIA
procesados_recientemente = TTLCache(maxsize=20000, ttl=600)
wamid_lock = asyncio.Lock()

RATE_LIMIT_CLIENTES = TTLCache(maxsize=10000, ttl=10)
rate_limit_lock = asyncio.Lock()

RATE_LIMIT_MEDIA = TTLCache(maxsize=10000, ttl=60)
media_limit_lock = asyncio.Lock()

# 🛡️ SEMÁFOROS DIVIDIDOS (Evita que la lentitud de un área paralice todo el SaaS)
SEMAFORO_IA = asyncio.Semaphore(15)      # Máximo 15 conexiones concurrentes a Gemini Chat
SEMAFORO_MEDIA = asyncio.Semaphore(10)   # Máximo 10 procesamientos de imágenes/audios concurrentes

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
        res_config = await async_db_execute(supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1))
        
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

        # 4. 🛡️ RATE LIMIT POR TELÉFONO (Anti-Spam)
        async with rate_limit_lock:
            peticiones_recientes = RATE_LIMIT_CLIENTES.get(telefono_cliente, 0)
            if peticiones_recientes > 8:
                logger.warning(f"⚠️ [TRACE:{trace_id}] [RATE LIMIT] Spam detectado de {telefono_cliente}.")
                return
            RATE_LIMIT_CLIENTES[telefono_cliente] = peticiones_recientes + 1

        tipo_mensaje = str(msg.get("type", "text")).lower()
        texto_entrante = ""

        logger.info(f"📦 [TRACE:{trace_id}] Formato: '{tipo_mensaje}' | Remitente: {telefono_cliente}")
        
        # 5. 🛡️ EXTRACCIÓN Y VALIDACIÓN MULTIMEDIA
        if tipo_mensaje == "text": 
            texto_entrante = msg.get("text", {}).get("body", "").strip()
        elif tipo_mensaje == "interactive": 
            texto_entrante = msg.get("interactive", {}).get("button_reply", {}).get("title", "").strip()
            
        elif tipo_mensaje in ["image", "audio"]:
            # 🛡️ RATE LIMIT MULTIMEDIA (Protege costos de APIs de IA)
            async with media_limit_lock:
                media_count = RATE_LIMIT_MEDIA.get(telefono_cliente, 0)
                if media_count > 5:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Abuso multimedia detectado de {telefono_cliente}.")
                    return
                RATE_LIMIT_MEDIA[telefono_cliente] = media_count + 1

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
                    
                    # 🛡️ VALIDACIÓN MIME ESTRICTA (Protege contra inyecciones de código disfrazadas de imágenes)
                    mime = media_dict_img.get("mime_type", "")
                    if mime not in ["image/jpeg", "image/png", "image/webp"]:
                        logger.warning(f"🚨 [TRACE:{trace_id}] Formato MIME no permitido: {mime}")
                        return
        else: 
            logger.info(f"ℹ️ [TRACE:{trace_id}] Formato '{tipo_mensaje}' descartado.")
            return

        # 6. 🛡️ GESTIÓN DE CRM (Manejo de Race Conditions en inserción)
        res_p = await async_db_execute(supabase.table('prospectos').select('columna, notas').eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_actual))
        columna_actual = res_p.data[0].get("columna", "Bandeja Nueva") if res_p.data else "Bandeja Nueva"
        nombre_cliente = valor.get("contacts", [{}])[0].get("profile", {}).get("name", "Cliente")

        if not res_p.data:
            try:
                await async_db_execute(supabase.table('prospectos').insert({
                    "nombre": nombre_cliente, 
                    "telefono": telefono_cliente, 
                    "columna": columna_actual, 
                    "vendedor_id": vendedor_actual,
                    "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
                }))
            except Exception as db_e:
                # Capturamos fallo de restricción UNIQUE en caso de inserción concurrente
                logger.warning(f"⚠️ [TRACE:{trace_id}] Posible colisión en inserción CRM (Ignorada de forma segura): {db_e}")

        # Guardado de mensaje en BD (Aislado para no romper el flujo si falla)
        try:
            await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "USER", texto_entrante)
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
        # 8. 🧹 RECOLECCIÓN DE BASURA EXPLÍCITA (Previene Memory Leaks de Archivos Binarios)
        if media_dict_audio: media_dict_audio.clear()
        if media_dict_img: media_dict_img.clear()


@app.get("/webhook")
async def verificar_webhook(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == WEBHOOK_SECRET:
        logger.info("✅ [WEBHOOK] Servidor validado con éxito por Meta.")
        return int(params.get("hub.challenge"))
    raise HTTPException(status_code=403, detail="Token de validación de Meta inválido")

@app.post("/webhook")
async def recibir_mensajes(request: Request):
    try:
        await asyncio.wait_for(validar_firma_meta(request), timeout=5.0)
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Timeout validando firma")

    try:
        # 🛡️ FIX AAA: Limitador estricto de Payload (Protección DDoS)
        body_bytes = await request.body()
        if len(body_bytes) > 2_000_000: # 2MB Max Payload
            raise HTTPException(413, "Payload demasiado grande")
            
        # 🛡️ FIX AAA: Captura de JSONDecodeError
        try:
            body = json.loads(body_bytes)
        except json.JSONDecodeError:
            raise HTTPException(400, "JSON corrupto o inválido")
            
        # 🛡️ FIX AAA: Navegación Profunda y Segura (Anti-Crash)
        entry = body.get("entry", [{}])
        changes = entry[0].get("changes", [{}])
        if not changes: return {"status": "ignored"}
        
        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages: return {"status": "ignored"}
        
        # Lanzamiento Seguro Asíncrono
        lanzar_tarea_segura(gestionar_mensaje_entrante_bg(value, messages[0], value.get("metadata", {}).get("phone_number_id", WHATSAPP_PHONE_ID)))
        
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e: 
        logger.error(f"❌ Error en Webhook Entrypoint: {e}")
        return {"status": "error", "reason": str(e)}

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), reload=False)
