# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V15.0 (HARDENED)
# Multi-Tenant • Anti-Abuso • Anti-429 • Escalable • Seguro
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
import mimetypes
import urllib.parse
import re
import unicodedata
import base64
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
import google.generativeai as genai

# 🔥 Inyección AAA: Manejo criptográfico de contraseñas (Preparación para Login seguro)
from passlib.context import CryptContext

load_dotenv()

# ==========================================================
# 🛡️ REGLAS DE SEGURIDAD Y CONFIGURACIÓN GLOBAL
# ==========================================================
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("❌ FATAL: JWT_SECRET no configurada en las variables de entorno de Render.")

# Configuración del encriptador de passwords
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ==========================================
# 📝 CONFIGURACIÓN DE LOGGING PROFESIONAL
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("VeltrixEngine")

# ==========================================================
# 🔧 LÍMITES OPERATIVOS Y CONSTANTES
# ==========================================================
BATCH_SIZE = 250
MAX_BG_TASKS = 30
PRECIO_FALLBACK = 50.0

MAX_HISTORIAL = 8
MAX_MENSAJE_LEN = 1200
MAX_CONTEXTO_INV = 150
MAX_CACHE_IA = 5000

GEMINI_TIMEOUT = 35.0
GEMINI_REINTENTOS = 3
GEMINI_TEMP = 0.2

# 🔥 Protección anti-abuso (Rate Limiting)
MAX_REQUESTS_POR_MINUTO_TENANT = 40
MAX_REQUESTS_POR_MINUTO_TELEFONO = 12
MAX_REQUESTS_GLOBAL_MINUTO = 250

# 🔒 Timeouts HTTP
HTTP_CONNECT_TIMEOUT = 10.0
HTTP_READ_TIMEOUT = 35.0
HTTP_WRITE_TIMEOUT = 20.0
HTTP_POOL_TIMEOUT = 10.0

# ==========================================================
# 🔑 CREDENCIALES BASE AAA (CARGADAS ESTRICTAMENTE DEL ENTORNO)
# ==========================================================
GENAI_KEY = os.getenv("GENAI_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("META_WEBHOOK_SECRET", "").strip()
VERIFY_TOKEN = WEBHOOK_SECRET
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "524491142598").strip()
ALGORITHM = "HS256"
PORT = int(os.getenv("PORT", 10000))
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0").strip()

# --- 📞 CREDENCIALES WHATSAPP (SALVAVIDAS GLOBAL) ---
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ ERROR CRÍTICO: Faltan credenciales de Supabase")
if not JWT_SECRET or len(JWT_SECRET) < 32:
    raise ValueError("❌ ERROR CRÍTICO: JWT_SECRET inseguro o demasiado corto")
if not WEBHOOK_SECRET:
    logger.warning("⚠️ META_WEBHOOK_SECRET vacío. Las validaciones Meta fallarán.")

# ==========================================
# ☁️ CONEXIÓN SUPABASE
# ==========================================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================================
# 🧠 MEMORIA RAM OPERATIVA (ESTADO GLOBAL)
# ==========================================================
registro_actividad_b2b = {}
historial_hashes_b2b = {}
procesados_recientemente = deque(maxlen=1000)
cache_respuestas_ia = {}
locks_por_tenant = {}

# 🛡️ CIRCUIT BREAKER GEMINI (Para evitar bloqueos 429 en cadena)
gemini_bloqueado_hasta = 0.0 

# Rate limiters
rate_limit_tenant = defaultdict(list)
rate_limit_phone = defaultdict(list)
rate_limit_global = []

# ⚡ HTTPX Singleton
http_client: Optional[httpx.AsyncClient] = None

# ==========================================
# 🧹 LIMPIEZA DE MEMORIA
# ==========================================
def limpiar_cache_ia_si_excede_limite():
    if len(cache_respuestas_ia) <= MAX_CACHE_IA:
        return
    logger.warning("🧹 Limpiando cache IA por límite RAM")
    items_ordenados = sorted(cache_respuestas_ia.items(), key=lambda x: x[1].get("ts", 0))
    elementos_a_borrar = len(items_ordenados) // 2
    for key, _ in items_ordenados[:elementos_a_borrar]:
        cache_respuestas_ia.pop(key, None)

# ==========================================
# 🔥 SWITCH DE ENCENDIDO (LIFESPAN ÚNICO)
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client

    limits = httpx.Limits(
        max_keepalive_connections=50,
        max_connections=100
    )

    timeout = httpx.Timeout(
        connect=HTTP_CONNECT_TIMEOUT,
        read=HTTP_READ_TIMEOUT,
        write=HTTP_WRITE_TIMEOUT,
        pool=HTTP_POOL_TIMEOUT
    )

    http_client = httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
        http2=True # Inyección AAA: Soporte HTTP/2 para mayor velocidad con Meta
    )

    logger.info("🚀 [SISTEMA] Motor Central Veltrix Iniciado")

    # 🧵 Worker persistente
    seguimiento_task = asyncio.create_task(bucle_seguimiento_24h())

    try:
        yield
    finally:
        seguimiento_task.cancel()
        try:
            await seguimiento_task
        except asyncio.CancelledError:
            logger.info("🛑 Worker seguimiento cancelado")

        if http_client:
            await http_client.aclose()

        logger.info("🛑 [SISTEMA] Motor Central Apagado")

# ==========================================
# ✨ FASTAPI INIT (Instancia Única Blindada)
# ==========================================
app = FastAPI(
    title="Motor Central CRM B2B - Veltrix Engine",
    version="15.0",
    lifespan=lifespan
)

router = APIRouter()

# ==========================================
# 🌍 CORS ENDURECIDO
# ==========================================
# Si defines credenciales=True, origins no puede ser ["*"] en producción estricta, 
# pero lo configuramos dinámicamente.
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["*"],
)

# ==========================================================
# 🧰 HELPERS AAA (Sanitización y Seguridad)
# ==========================================================
def now_ts() -> float:
    return time.time()

def safe_float(valor):
    try:
        if valor is None: return 0.0
        limpio = str(valor).replace("$", "").replace(",", "").replace("MXN", "").strip()
        return float(limpio)
    except (ValueError, TypeError):
        return 0.0

def limpiar_texto(texto: str) -> str:
    if texto is None: return ""
    texto = str(texto).replace("\x00", "")
    texto = unicodedata.normalize("NFKC", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    
    # Sanitización Anti-Injection básica sin romper JSONs
    texto = texto.replace("<script>", "").replace("</script>", "")
    
    return texto[:MAX_MENSAJE_LEN]

def generar_hash_cache(*args) -> str:
    bruto = "|".join([str(a) for a in args])
    return hashlib.sha256(bruto.encode()).hexdigest()

# ==========================================
# 📸 HELPER MULTIMEDIA (PREPARACIÓN WHATSAPP AUDIO/FOTOS)
# ==========================================
async def descargar_media_whatsapp_async(media_id: str, token: str) -> Optional[dict]:
    """Descarga un audio o imagen desde los servidores de Meta."""
    if not http_client: return None
    try:
        # 1. Obtener URL del archivo
        url_info = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {token}"}
        res_info = await http_client.get(url_info, headers=headers)
        
        if res_info.status_code != 200:
            logger.error(f"❌ Error al consultar media ID {media_id}")
            return None
            
        data_info = res_info.json()
        media_url = data_info.get("url")
        mime_type = data_info.get("mime_type")
        
        if not media_url: return None
        
        # 2. Descargar el binario del archivo
        res_media = await http_client.get(media_url, headers=headers)
        if res_media.status_code != 200:
            return None
            
        # Retornamos el payload listo para Gemini
        return {
            "mime_type": mime_type,
            "data": res_media.content # Binario puro, Gemini SDK lo acepta o lo pasamos a Base64 según necesidad
        }
    except Exception as e:
        logger.error(f"❌ Error descargando media: {e}")
        return None

# ==========================================
# 📦 MODELOS PYDANTIC BLINDADOS
# ==========================================
class Credenciales(BaseModel):
    email: str = Field(..., min_length=5, max_length=120)
    password: str = Field(..., min_length=6, max_length=120)

class ProspectoUpdate(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=120)
    nueva_columna: str = Field(..., min_length=1, max_length=80)
    vendedor_id: str = Field(..., min_length=1, max_length=80)

class NotaUpdate(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=120)
    notas: str = Field(default="", max_length=4000)
    etiquetas: str = Field(default="", max_length=500)
    vendedor_id: str = Field(..., min_length=1, max_length=80)

class MensajeSaliente(BaseModel):
    cliente: str = Field(..., min_length=1, max_length=120)
    texto: str = Field(..., min_length=1, max_length=MAX_MENSAJE_LEN)
    vendedor_id: str = Field(..., min_length=1, max_length=80)

class InventarioItem(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=180)
    consola: str = Field(..., min_length=1, max_length=80)
    precio: float = Field(..., ge=0)
    costo: float = Field(default=0.0, ge=0)
    stock: int = Field(default=1, ge=0)
    codigo_barras: str = Field(default="", max_length=120)
    url_portada: str = Field(default="", max_length=500)
    estado_general: str = Field(default="Bueno", max_length=80)
    rareza: str = Field(default="", max_length=80)
    vendedor_id: str = Field(..., min_length=1, max_length=80)
    tiene_caja: bool = False
    tiene_manual: bool = False
    es_portada_original: bool = False
    descripcion_detallada: str = Field(default="", max_length=4000)

    @field_validator("nombre", "consola", mode="before")
    @classmethod
    def validar_texto(cls, value: str):
        return limpiar_texto(value)

class VentaItem(BaseModel):
    nombre: str = Field(..., min_length=1, max_length=180)
    consola: str = Field(..., min_length=1, max_length=80)
    estado_general: str = Field(default="", max_length=80)
    nuevo_stock: int = Field(..., ge=0)
    vendedor_id: str = Field(..., min_length=1, max_length=80)

class BotConfig(BaseModel):
    vendedor_id: str = Field(..., min_length=1, max_length=80)
    link_pago: str = Field(default="", max_length=500)
    texto_entrega: str = Field(default="", max_length=2000)
    admin_phone: str = Field(default="", max_length=40)
    bot_activo: bool = True
    
class LoginUpdate(BaseModel):
    email: str
    password: str

# --- MOLDES EXCLUSIVOS PARA MOBILE HUB ---
class MobileMessageRequest(BaseModel):
    to: str = Field(..., min_length=1, max_length=40)
    msg: str = Field(..., min_length=1, max_length=2000)

class MobileChatRequest(BaseModel):
    telefono: str = Field(..., min_length=1, max_length=40)

class ClienteIdentificador(BaseModel):
    nombre: str = ""
    telefono: str = ""

class ColumnaUpdate(BaseModel):
    nombre: str = ""
    telefono: str = ""
    columna: str = ""       # Lo que manda la PC
    nueva_columna: str = "" # Lo que manda el Móvil (Aceptamos ambos)

class NotasUpdate(BaseModel):
    nombre: str = ""
    telefono: str = ""
    notas: str = ""
    etiquetas: str = ""
    vendedor_id: str = ""

# ==========================================
# 🔐 AUTENTICACIÓN JWT Y WEBHOOKS
# ==========================================
def crear_token_jwt(vendedor_id: str, email: str):
    expiracion = datetime.now(timezone.utc) + timedelta(days=1)
    payload = {
        "sub": vendedor_id,
        "email": email,
        "exp": expiracion,
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)

async def verificar_sesion_b2b(
    authorization: str = Header(None),
    auth_token: str = Header(None)
):
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif auth_token:
        token = auth_token.strip()

    if not token:
        raise HTTPException(status_code=401, detail="Acceso denegado: Token faltante")

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        vendedor_id_real = payload.get("sub")
        if not vendedor_id_real:
            raise HTTPException(status_code=401, detail="Token corrupto")
        return vendedor_id_real
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Sesión expirada")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta:
        raise HTTPException(status_code=400, detail="Falta firma Meta")

    cuerpo_bytes = await request.body()
    firma_calculada = (
        "sha256=" +
        hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            cuerpo_bytes,
            hashlib.sha256
        ).hexdigest()
    )

    if not hmac.compare_digest(firma_meta, firma_calculada):
        logger.warning("🚨 Intento de webhook inválido")
        raise HTTPException(status_code=403, detail="Firma inválida")

    return True

# ==========================================
# 🛡️ RATE LIMITERS
# ==========================================
def limpiar_rate_limit(lista: list, ventana_segundos: int):
    ahora = now_ts()
    while lista and (ahora - lista[0]) > ventana_segundos:
        lista.pop(0)

def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    ahora = now_ts()
    limpiar_rate_limit(rate_limit_global, 60)
    if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO:
        logger.warning("🚨 RATE LIMIT GLOBAL")
        return False

    limpiar_rate_limit(rate_limit_tenant[vendedor_id], 60)
    if len(rate_limit_tenant[vendedor_id]) >= MAX_REQUESTS_POR_MINUTO_TENANT:
        logger.warning(f"🚨 RATE LIMIT TENANT: {vendedor_id}")
        return False

    limpiar_rate_limit(rate_limit_phone[telefono], 60)
    if len(rate_limit_phone[telefono]) >= MAX_REQUESTS_POR_MINUTO_TELEFONO:
        logger.warning(f"🚨 RATE LIMIT TELÉFONO: {telefono}")
        return False

    rate_limit_global.append(ahora)
    rate_limit_tenant[vendedor_id].append(ahora)
    rate_limit_phone[telefono].append(ahora)
    return True

# ==========================================
# 💵 MOTOR DE PRECIOS E IA CORE
# ==========================================
async def obtener_dolar_hoy_async():
    try:
        if not http_client: return 18.00
        res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD")
        if res.status_code != 200: return 18.00
        data = res.json()
        return float(data.get("rates", {}).get("MXN", 18.00))
    except Exception:
        return 18.00

async def obtener_html_escalonado_async(url_objetivo: str) -> str:
    if not http_client: return ""
    estrategias = [
        ("🟢 Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("🟡 Render", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true")
    ]
    headers_humanos = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for nombre_estrategia, url_scraper in estrategias:
        try:
            res = await http_client.get(url_scraper)
            if res.status_code == 200:
                texto = res.text.lower()
                if "pricecharting" in texto or "price" in texto:
                    return res.text
        except Exception:
            pass

    try:
        res = await http_client.get(url_objetivo, headers=headers_humanos)
        if res.status_code == 200: return res.text
    except Exception:
        pass
    return ""

def calcular_rareza_ia(nombre: str, consola: str, precio: float) -> str:
    nombre, consola = nombre.upper(), consola.upper()
    consolas_modernas = ["PS5", "PS4", "NINTENDO SWITCH", "XBOX ONE", "XBOX SERIES X"]
    
    if any(x in nombre for x in ["FIFA", "MADDEN", "NBA", "NCAA", "PES", "SINGSTAR", "EA FC"]): return "Común"
    if any(x in nombre for x in ["SILENT HILL", "KUON", "RULE OF ROSE", "OBSCURE", "HAUNTING GROUND", "PRAGMATA"]): return "Élite"
    if any(x in nombre for x in ["MARIO", "ZELDA", "METROID", "POKEMON", "HALO", "GTA"]): return "Demandado"
    
    if consola in consolas_modernas:
        if precio >= 3500: return "Élite"
        if precio >= 1000: return "Demandado"
        return "Común"
        
    if precio >= 1500: return "Élite"
    if precio >= 800: return "Joya"
    if precio >= 400: return "Demandado"
    return "Común"

def calcular_precio_venta_inteligente(precio_mercado_mxn: float, costo_compra: float = 0.0):
    piso_absoluto = 250.0
    precio_con_margen = (precio_mercado_mxn + 150.0 if precio_mercado_mxn > 0 else 0.0)
    precio_seguridad = (costo_compra + 100.0 if costo_compra > 0 else 0.0)
    precio_bruto = max(piso_absoluto, precio_con_margen, precio_seguridad)
    return float(round(precio_bruto / 10) * 10)

# ==========================================================
# 🗄️ CAPA DE REPOSITORIO Y SERVICIOS B2B
# ==========================================================
async def obtener_contexto_inventario(vendedor_id: str) -> str:
    """Extrae SOLO los campos críticos del inventario para ahorrar Tokens de Gemini."""
    res_inv = supabase.table('inventario').select('nombre, precio, stock, consola').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(MAX_CONTEXTO_INV).execute()
    if not res_inv.data: return "Inventario vacío."
    
    # Compactación Ninja para ahorrar miles de tokens
    lineas = [f"- {i['nombre']} ({i['consola']}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in res_inv.data]
    return "\n".join(lineas)

async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    res_hist = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', vendedor_id).order('created_at', desc=True).limit(MAX_HISTORIAL).execute()
    if not res_hist.data: return "Primer mensaje."
    mensajes = reversed(res_hist.data)
    return "\n".join([f"{m.get('autor', 'USER')}: {m.get('mensaje', '')}" for m in mensajes])

async def actualizar_estado_crm(telefono: str, vendedor_id: str, columna: str, iluminacion: str, juego: str):
    supabase.table('prospectos').update({
        'columna': columna,
        'estado_iluminacion': iluminacion,
        'ultimo_juego_interes': juego,
        'ultima_interaccion_ia': datetime.now(timezone.utc).isoformat()
    }).eq('telefono', telefono).eq('vendedor_id', vendedor_id).execute()

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str):
    supabase.table('mensajes_chat').insert({
        'telefono': telefono,
        'vendedor_id': vendedor_id,
        'autor': autor,
        'mensaje': mensaje
    }).execute()

# ==========================================
# 📥 MOTOR WHATSAPP ASYNC
# ==========================================
async def disparar_whatsapp_dinamico_async(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        await http_client.post(url, headers=headers, json=payload)
    except Exception: 
        logger.exception("⚠️ Error disparando WhatsApp Text")

async def disparar_whatsapp_imagen_async(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "image", "image": {"link": url_imagen, "caption": texto_mensaje}}
    try: 
        await http_client.post(url, headers=headers, json=payload)
    except Exception: 
        logger.exception("⚠️ Error disparando WhatsApp Imagen")

async def cazar_portada_y_guardar_background(juego_id_supabase: str, nombre_juego: str, consola: str):
    try:
        consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
        query = f"{nombre_juego} {consola_web}".replace(" ", "+")
        url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
        html_search = await obtener_html_escalonado_async(url_search)
        if not html_search: return
        soup = BeautifulSoup(html_search, 'html.parser')
        img_tag = soup.find('img', class_='product_image') or soup.find('img', alt=lambda x: x and nombre_juego.lower() in x.lower())
        if not img_tag or not img_tag.get('src'): return
        imagen_url = img_tag['src']
        if not imagen_url.startswith("http"):
            imagen_url = "https:" + imagen_url if imagen_url.startswith("//") else "https://www.pricecharting.com" + imagen_url
        res_img = await http_client.get(imagen_url)
        if res_img.status_code != 200: return
        image_bytes = res_img.content
        nombre_archivo = f"{consola.replace(' ', '_')}_{nombre_juego.replace(' ', '_')}_{int(now_ts())}.jpg"
        supabase.storage.from_("portadas").upload(nombre_archivo, image_bytes, {"content-type": "image/jpeg"})
        url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
        supabase.table('inventario').update({"url_portada": url_publica}).eq('id', juego_id_supabase).execute()
    except Exception: 
        pass

def validar_respuesta_ia(data: dict) -> dict:
    if not isinstance(data, dict):
        raise Exception("IA devolvió formato inválido")

    intencion = str(data.get("intencion", "COTIZACION")).upper()
    if intencion not in ["COMPRA", "COTIZACION", "HUMANO", "PEDIDO_ESPECIAL"]:
        intencion = "HUMANO"

    respuesta = limpiar_texto(data.get("respuesta", ""))
    juego = limpiar_texto(data.get("juego_detectado", ""))

    if not respuesta:
        respuesta = "Hola. Estoy revisando la información."

    return {
        "intencion": intencion,
        "respuesta": respuesta,
        "juego_detectado": juego,
        "pedido_especial_juego": data.get("pedido_especial_juego", ""),
        "pedido_especial_consola": data.get("pedido_especial_consola", "")
    }

# ==========================================================
# 🧠 CLIENTE GEMINI CENTRALIZADO (V14 MULTIMODAL + CIRCUIT BREAKER)
# ==========================================================
async def consultar_gemini_json(prompt: str, media_dict: dict = None, temperature: float = 0.2, retries: int = 3) -> dict:
    global gemini_bloqueado_hasta
    
    # 🛡️ CIRCUIT BREAKER: Si estamos bloqueados, no quemamos más intentos
    if now_ts() < gemini_bloqueado_hasta:
        logger.warning("🚫 [CIRCUIT BREAKER] Gemini está en cooldown temporal.")
        return {"respuesta": "En este momento estoy atendiendo a varios clientes, denme un par de minutos y les respondo. 🎮", "intencion": "HUMANO"}

    api_key = os.getenv("GENAI_KEY")
    if not api_key:
        raise Exception("Falta GENAI_KEY en las variables de entorno")
    
    genai.configure(api_key=api_key)
    
    for intento in range(retries):
        try:
            model = genai.GenerativeModel('gemini-2.5-flash') 
            
            # 📦 Soporte Multimodal (Texto + Audio/Imagen)
            contenido = [prompt]
            if media_dict and "data" in media_dict:
                # Si viene binario puro, lo pasamos crudo (Gemini acepta blob object)
                contenido.append({
                    "mime_type": media_dict.get("mime_type", "image/jpeg"),
                    "data": media_dict["data"]
                })

            config_ia = genai.types.GenerationConfig(temperature=temperature)
            
            response = await asyncio.to_thread(
                model.generate_content, 
                contenido,
                generation_config=config_ia
            )
            
            texto_crudo = response.text
            texto_limpio = texto_crudo.replace("```json", "").replace("```", "").strip()
            
            inicio = texto_limpio.find('{')
            fin = texto_limpio.rfind('}')
            
            if inicio != -1 and fin != -1:
                return json.loads(texto_limpio[inicio:fin+1])
            
        except Exception as e:
            error_str = str(e)
            print(f"⚠️ [GEMINI] Error en intento {intento + 1}: {error_str}")
            
            # 🚦 Detector de Cuota Excedida (Error 429)
            if "429" in error_str or "Quota exceeded" in error_str:
                logger.error("🚨 [QUOTA ALARM] Límite de Gemini alcanzado. Activando Circuit Breaker (60s).")
                gemini_bloqueado_hasta = now_ts() + 60.0
                break # Rompemos el ciclo de reintentos, ya no sirve de nada intentar
                
            await asyncio.sleep(2)
            
    print("❌ [FATAL] Gemini falló procesando el mensaje.")
    return {
        "respuesta": "Tuve un micro-corte en el sistema. ¿Me repites tu mensaje por favor?", 
        "intencion": "HUMANO"
    }

# ==========================================================
# 🤖 ANALIZAR INTENCIÓN IA (CERRADOR MAESTRO V-13.0)
# ==========================================================
async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict):
    try:
        vendedor_id = config.get("vendedor_id", "V-001")
        nombre_negocio = config.get("nombre_negocio", "Fantasy Games")
        texto_cliente = limpiar_texto(texto_cliente)
        
        # Hash para cache (Evita procesar el mismo mensaje doble vez)
        cache_key = generar_hash_cache(vendedor_id, texto_cliente, historial_chat[-200:])
        cache_item = cache_respuestas_ia.get(cache_key)

        if cache_item and (now_ts() - cache_item["ts"]) < 90:
            return cache_item["data"]

        if vendedor_id not in locks_por_tenant:
            locks_por_tenant[vendedor_id] = asyncio.Lock()

        async with locks_por_tenant[vendedor_id]:
            link_pago = config.get("link_pago", "Solicita el link de pago")
            texto_entrega = config.get("texto_entrega", "Coordinar entrega con asesor")

            prompt = f"""
[SYSTEM: Eres un Vendedor Senior Elite estricto, persuasivo y altamente adaptable].
Eres el mejor cerrador de ventas operando bajo la tecnología del CRM 'Veltrix Engine'.
Tu identidad oficial y la empresa que representas es: "{nombre_negocio}".
OBJETIVO PRINCIPAL: VENDER RÁPIDO Y ENVIAR EL LINK DE PAGO. 

ESTRATEGIA DE PRECIOS:
1. PRECIO: Es tu precio base de salida.
2. PRECIO_SUGERIDO: Es el valor real de mercado. Úsalo para dar valor.
3. PRECIO_MINIMO: LÍMITE SECRETO. NUNCA lo menciones, úsalo para regatear.

NUEVAS DIRECTRICES V13 (ESTRICTAS):
1. CERO ALUCINACIONES: Solo vende o confirma si el juego está EXACTAMENTE en el INVENTARIO ACTUAL.
2. FILTRO: Si piden recomendaciones, NO mandes todo. Sugiere 3 juegos y pregunta: "¿Para qué consola?".
3. CATÁLOGO COMPLETO: https://veltrixengine.pro/catalogo
4. LINK DE PAGO: SIEMPRE incluye: "💳 Paga seguro aquí: {link_pago}"
5. LOGÍSTICA: {texto_entrega}

REGLAS DE CLASIFICACIÓN ('intencion'):
- "COTIZACION": Pregunta precio, dudas, fotos.
- "COMPRA": SOLO si el cliente dice "ya pagué", "ya transferí", "te mandé el ticket".
- "PEDIDO_ESPECIAL": Pide un juego que NO TENEMOS en el inventario.
- "HUMANO": Dudas que no sepas responder, quejas, audios o fotos complejas.

INVENTARIO: 
{inventario_contexto}

HISTORIAL DEL CHAT:
{historial_chat}

MENSAJE CLIENTE: 
"{texto_cliente}"

Responde EXCLUSIVAMENTE en JSON válido:
{{
  "intencion": "COMPRA", "HUMANO", "COTIZACION" o "PEDIDO_ESPECIAL",
  "respuesta": "Tu respuesta persuasiva aquí",
  "juego_detectado": "Nombre del producto exacto (si lo tenemos)",
  "pedido_especial_juego": "Nombre del juego si NO lo tenemos",
  "pedido_especial_consola": "Consola del juego si NO lo tenemos"
}}
"""
            data = await consultar_gemini_json(prompt)
            data = validar_respuesta_ia(data)

            limpiar_cache_ia_si_excede_limite()
            cache_respuestas_ia[cache_key] = {"ts": now_ts(), "data": data}
            return data

    except Exception:
        logger.exception("❌ ERROR analizar_intencion_venta_ia")
        return {"intencion": "HUMANO", "respuesta": "Estoy revisando la información. Un asesor continuará contigo enseguida. 🎮", "juego_detectado": ""}

async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        if intencion == "COMPRA": motivo = "quiere cerrar compra"
        elif intencion == "PEDIDO_ESPECIAL": motivo = "busca un juego que NO tenemos en stock"
        else: motivo = "requiere ayuda humana"
            
        prompt = f"Cliente: {cliente}\nMotivo: {motivo}\nHistorial:\n{historial_str}\nGenera resumen ejecutivo en 3 viñetas. JSON: {{\"resumen\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        return data.get("resumen", "⚠️ Cliente requiere atención humana")
    except Exception: return "⚠️ Cliente requiere atención humana"

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nJuego: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera remarketing corto persuasivo para venta. JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))}
    except Exception: return None

async def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config: dict):
    try:
        telefono_admin = config.get("admin_phone") or ADMIN_PHONE_GLOBAL
        token, phone_id = config.get("meta_token", ""), config.get("meta_phone_id", "")
        
        if intencion == "COMPRA": encabezado = "💰 *NUEVA VENTA DETECTADA*"
        elif intencion == "PEDIDO_ESPECIAL": encabezado = "⚠️ *NUEVO PEDIDO ESPECIAL*"
        else: encabezado = "🚨 *ASISTENCIA REQUERIDA*"
            
        mensaje = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Teléfono: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_ia}"
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje, token, phone_id)
    except Exception: logger.exception("❌ ERROR CRÍTICO ALERTA ADMIN")

async def bucle_seguimiento_24h():
    while True:
        try:
            hace_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            res = supabase.table('prospectos').select('*').eq('columna', 'Envios Masivos').lt('ultima_interaccion_ia', hace_24h).limit(20).execute()
            prospectos = res.data or []
            
            if not prospectos:
                await asyncio.sleep(1800) # Si no hay nada, descansa 30 min
                continue

            for p in prospectos:
                vendedor_id = p.get('vendedor_id', 'V-001')
                
                res_conf = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).limit(1).execute()
                if not res_conf.data: continue
                config = res_conf.data[0]

                tk_final = config.get('meta_token') or WHATSAPP_TOKEN
                ph_final = config.get('meta_phone_id') or WHATSAPP_PHONE_ID

                try:
                    # Usamos la versión compactada para ahorrar tokens en remarketing
                    contexto_inv = await obtener_contexto_inventario(vendedor_id)
                    oferta = await generar_oferta_inteligente(p.get('nombre', 'Cliente'), p.get('ultimo_juego_interes', 'videojuego'), contexto_inv)
                    
                    if oferta and oferta.get("mensaje_oferta"):
                        mensaje = oferta.get("mensaje_oferta")
                        await disparar_whatsapp_dinamico_async(p.get('telefono'), mensaje, tk_final, ph_final)
                        await actualizar_estado_crm(p.get('telefono'), vendedor_id, 'Con Descuento', 'oro', p.get('ultimo_juego_interes'))
                        await guardar_mensaje_chat(p.get('telefono'), vendedor_id, 'BOT_REMARKETING', mensaje)
                        
                        await asyncio.sleep(5) 

                except Exception as e:
                    if "429" in str(e) or "Quota" in str(e):
                        logger.warning("⚠️ [GEMINI] Límite alcanzado en Bucle 24h. Durmiendo 60 seg...")
                        await asyncio.sleep(60)
                    else:
                        logger.error(f"❌ Error procesando prospecto {p.get('telefono')}: {e}")

        except Exception:
            logger.exception("❌ ERROR FATAL EN RELOJ 24H")
        
        await asyncio.sleep(600)

# ==========================================================
# 🤖 MOTOR PRINCIPAL DE NEGOCIO (IA & WORKFLOW V13)
# ==========================================================
async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict):
    try:
        vendedor_id = config.get("vendedor_id", "")
        if not verificar_rate_limit(vendedor_id, telefono): 
            logger.warning(f"⚠️ Rate limit excedido para {telefono}")
            return

        token = config.get("meta_token", "")
        phone_id = config.get("meta_phone_id", "")
        
        # 1. Recopilar munición compactada para la IA
        contexto = await obtener_contexto_inventario(vendedor_id)
        historial = await obtener_historial_chat(telefono, vendedor_id)

        # 2. Consultar al Cerebro IA
        decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial, config)
        
        nueva_columna = columna_actual
        iluminacion = "blanco"
        intencion_ia = str(decision.get("intencion", "CONSULTA")).upper()
        respuesta_final = decision.get("respuesta", "Tuve un contratiempo, en un momento te atiendo.")
        juego_detectado = decision.get("juego_detectado", "")

        # ==========================================================
        # 🚦 RUTEO DE INTENCIONES
        # ==========================================================
        if intencion_ia == "HUMANO":
            nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
            resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)

        elif intencion_ia == "COMPRA":
            nueva_columna, iluminacion = "Por Entregar", "verde_exito"
            resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)

        elif intencion_ia == "COTIZACION" and columna_actual == "Bandeja Nueva":
            nueva_columna = "Envios Masivos"
            
        elif intencion_ia == "PEDIDO_ESPECIAL":
            nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
            juego_buscado = decision.get("pedido_especial_juego", "Juego no especificado")
            consola_buscada = decision.get("pedido_especial_consola", "Consola no especificada")
            resumen_alerta = f"🚨 *NUEVO PEDIDO ESPECIAL*\n👤 Cliente: {cliente}\n🎮 Busca: {juego_buscado}\n🕹️ Consola: {consola_buscada}\n¡Revisa tu proveedor!"
            await enviar_alerta_whatsapp_admin(cliente, telefono, "PEDIDO_ESPECIAL", resumen_alerta, config)

        # ==========================================================
        # 💾 ACTUALIZAR CRM Y CHAT
        # ==========================================================
        await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, juego_detectado)
        await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

        # ==========================================================
        # 🖼️ BÚSQUEDA DE PORTADA DE JUEGO (MULTI-TENANT FIJADO)
        # ==========================================================
        url_imagen = None
        if juego_detectado:
            try:
                res_img = (
                    supabase.table('inventario')
                    .select('url_portada')
                    .ilike('nombre', f'%{juego_detectado}%')
                    .eq('vendedor_id', vendedor_id)
                    .neq('url_portada', '')
                    .limit(1)
                    .execute()
                )
                if res_img.data: 
                    url_imagen = res_img.data[0].get('url_portada')
            except Exception as e:
                logger.error(f"⚠️ Error buscando imagen: {e}")

        # ==========================================================
        # 🚀 DISPARO FINAL DE WHATSAPP
        # ==========================================================
        if url_imagen:
            await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, token, phone_id)
        else:
            await disparar_whatsapp_dinamico_async(telefono, respuesta_final, token, phone_id)

    except Exception as e:
        logger.exception(f"❌ ERROR FATAL en procesar_respuesta_bot: {e}")

# ==========================================================
# 📱 HUB DE INTERACCIÓN MÓVIL: LECTURA DE HISTORIAL REAL
# ==========================================================

@app.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table("mensajes_chat").select("*") \
            .eq("vendedor_id", vendedor_id) \
            .eq("telefono", telefono) \
            .order("created_at", desc=False).execute()
        
        historial_formateado = []
        for m in res.data:
            autor = str(m.get("autor", "")).upper()
            es_mio = autor in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"]
            
            # 🛡️ TRIPLE MAPEADO: Buscamos el texto en todas las posibles columnas
            contenido_real = m.get("mensaje") or m.get("contenido") or m.get("texto") or ""
            
            historial_formateado.append({
                "contenido": str(contenido_real),
                "es_mio": es_mio,
                "fecha": str(m.get("created_at", ""))
            })
        
        return {"status": "ok", "historial": historial_formateado}
        
    except Exception as e:
        print(f"❌ Error en chat_history: {e}")
        return {"status": "error", "historial": []}

@app.post("/api/mobile/send_message")
async def send_mobile_message(data: MobileMessageRequest, vendedor_id: str = Depends(verificar_sesion_b2b)):
    """El celular llama aquí cuando tú escribes y das clic en 'Enviar'"""
    try:
        res_conf = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).limit(1).execute()
        if not res_conf.data:
            raise HTTPException(status_code=404, detail="Configuración de WhatsApp no encontrada")
        
        config = res_conf.data[0]
        tk_final = config.get('meta_token') or WHATSAPP_TOKEN
        ph_final = config.get('meta_phone_id') or WHATSAPP_PHONE_ID

        await disparar_whatsapp_dinamico_async(data.to, data.msg, tk_final, ph_final)
        await guardar_mensaje_chat(data.to, vendedor_id, 'ASESOR', data.msg)
        await actualizar_estado_crm(data.to, vendedor_id, "En Seguimiento", "azul", "")

        return {"status": "ok", "message": "Mensaje enviado y registrado"}

    except Exception as e:
        logger.error(f"❌ Error enviando desde móvil: {e}")
        raise HTTPException(status_code=500, detail="Error al enviar mensaje de WhatsApp")

# ==========================================================
# 🔐 AUTENTICACIÓN Y LOGIN B2B (UNIFICADO AAA)
# ==========================================================
@app.post("/api/login")
def login_b2b(datos: LoginUpdate):
    """
    Sistema de Autenticación Central Veltrix.
    Soporta contraseñas planas (legacy) y Bcrypt (AAA).
    Retorna payloads compatibles para Godot PC y Mobile.
    """
    try:
        # Buscamos en usuarios_veltrix (o usuarios_b2b si migraste, ajusta el nombre de tu tabla)
        res = supabase.table('usuarios_veltrix').select('*').eq('email', datos.email.lower()).execute()
        
        if not res.data or len(res.data) == 0:
            return {"status": "error", "detalle": "Usuario no registrado."}
            
        usuario = res.data[0]
        password_guardada = str(usuario.get('password', ''))
        
        # 🔐 VALIDACIÓN HÍBRIDA (Bcrypt + Plaintext fallback)
        password_valida = False
        if password_guardada.startswith('$2b$'):
            # Usa el pwd_context importado en la Parte 1
            password_valida = pwd_context.verify(datos.password, password_guardada)
        else:
            password_valida = (datos.password == password_guardada)
            
        if not password_valida:
            return {"status": "error", "detalle": "Contraseña incorrecta."}
            
        # Validación de Suscripción
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        suscripcion_valida = True
        
        if fecha_pago_str:
            try:
                from datetime import date
                fecha_pago = date.fromisoformat(fecha_pago_str)
                if date.today() > fecha_pago:
                    suscripcion_valida = False
                    supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id']).execute()
            except ValueError:
                pass 

        vendedor_id = usuario.get('vendedor_id', 'V-001')
        rol = usuario.get('rol', 'vendedor')
        token_jwt = crear_token_jwt(vendedor_id, usuario['email'])

        paquete_seguro = {
            "vendedor_id": vendedor_id,
            "email": usuario['email'],
            "estado": usuario.get('estado', 'Activo'),
            "pais": usuario.get('pais', 'México'),
            "suscripcion_activa": suscripcion_valida,
            "token": token_jwt 
        }
        
        print(f"✅ [LOGIN SUCCESS] Vendedor {vendedor_id} ha iniciado sesión.")

        # 🚀 RETORNO SINCRONIZADO CON APP MÓVIL Y PC
        return {
            "status": "ok",
            "datos": paquete_seguro, # 💻 Para tu Godot PC
            "access_token": token_jwt, # 📱 Para tu Godot Móvil
            "token_type": "bearer",
            "vendedor_id": vendedor_id,
            "nombre": usuario.get('nombre', 'Vendedor'),
            "rol": rol
        }

    except Exception as e:
        logger.exception("❌ [LOGIN ERROR]")
        raise HTTPException(status_code=500, detail="Error interno en servidor B2B.")

# ==========================================================
# 📈 MOTOR DE PRECIOS PRO (INTACTO)
# ==========================================================
@app.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo"):
    if vendedor_id != "ADMIN_VELTRIX":
        tiempo_actual = now_ts()
        llave_spam = f"precio_{vendedor_id}"
        estado = registro_actividad_b2b.get(llave_spam, {"strikes": 0, "last": 0, "ban": 0})
        
        if tiempo_actual < estado["ban"]:
            restante = int((estado["ban"] - tiempo_actual) / 60)
            return {"status": "error", "detalle": f"🚫 BAN ACTIVO: Espera {restante} minutos.", "mxn": {"loose": 0, "cib": 0, "new": 0}}
            
        if tiempo_actual - estado["last"] < 10:
            estado["strikes"] += 1
            estado["last"] = tiempo_actual
            if estado["strikes"] >= 3:
                estado["ban"] = tiempo_actual + (30 * 60) 
                estado["strikes"] = 0
                registro_actividad_b2b[llave_spam] = estado
                return {"status": "error", "detalle": "🚫 BANEADO: Múltiples intentos rápidos.", "mxn": {"loose": 0, "cib": 0, "new": 0}}
            
            registro_actividad_b2b[llave_spam] = estado
            return {"status": "error", "detalle": f"⚠️ Espera 10 segundos. (Strike {estado['strikes']}/3)", "mxn": {"loose": 0, "cib": 0, "new": 0}}
            
        estado["strikes"] = 0
        estado["last"] = tiempo_actual
        registro_actividad_b2b[llave_spam] = estado

    tipo_cambio = await obtener_dolar_hoy_async()
    nombre_busqueda = nombre.lower().strip()
    
    try:
        res_cache = supabase.table('cache_precios').select('*').eq('juego', nombre_busqueda).eq('consola', consola).execute()
        if res_cache.data and len(res_cache.data) > 0:
            datos_cache = res_cache.data[0]
            fecha_str = datos_cache['created_at'].split('+')[0].split('.')[0] 
            fecha_cache = datetime.fromisoformat(fecha_str)
            
            if (datetime.now() - fecha_cache).days < 30:
                rareza_calc = calcular_rareza_ia(nombre, consola, round(datos_cache['cib'] * tipo_cambio, 2))
                return {
                    "status": "ok",
                    "mxn": {"loose": round(datos_cache['loose'] * tipo_cambio, 2), "cib": round(datos_cache['cib'] * tipo_cambio, 2), "new": round(datos_cache['new'] * tipo_cambio, 2)},
                    "usd": {"loose": datos_cache['loose'], "cib": datos_cache['cib'], "new": datos_cache['new']},
                    "tipo_cambio": tipo_cambio,
                    "url_pc": datos_cache['url_pc'],
                    "rareza": rareza_calc
                }
    except Exception: 
        pass

    slugs_pc = {
        "PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3", "PS2": "playstation-2", "PS1": "playstation",
        "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox",
        "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", "Nintendo DS": "nintendo-ds", "Nintendo 64": "nintendo-64",
        "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance", "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", 
        "SNES": "super-nintendo", "NES": "nes", "Genesis": "sega-genesis"
    }
    
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    query = f"{nombre} {consola_web}".replace(" ", "+")
    url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
    
    html_search = await obtener_html_escalonado_async(url_search)
    if not html_search:
        return {"status": "error", "detalle": "Error Radar de Precios", "url_pc": "https://www.pricecharting.com", "mxn": {"loose": 0, "cib": 0, "new": 0}}
        
    soup = BeautifulSoup(html_search, 'html.parser')
    link_juego = None
    slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
    etiqueta_busqueda = f"/game/{slug_esperado}/"
    palabras_prohibidas = ['strategy-guide', 'magazine', 'comic', 'lot', 'bundle', 'box-only', 'manual-only', 'empty-box']
    
    tabla_resultados = soup.find(id="games_table")
    nodos_a_buscar = tabla_resultados.find_all('a', href=True) if tabla_resultados else soup.find_all('a', href=True)
    
    for a in nodos_a_buscar:
        href = a['href'].lower()
        if '/game/' in href and not any(b in href for b in palabras_prohibidas):
            if etiqueta_busqueda in href:
                link_juego = a['href'] if a['href'].startswith("http") else "https://www.pricecharting.com" + a['href']
                break
    
    if not link_juego:
        for a in nodos_a_buscar:
            href = a['href'].lower()
            if '/game/' in href and not any(b in href for b in palabras_prohibidas):
                link_juego = a['href'] if a['href'].startswith("http") else "https://www.pricecharting.com" + a['href']
                break

    if link_juego:
        html_juego = await obtener_html_escalonado_async(link_juego)
        if html_juego: soup = BeautifulSoup(html_juego, 'html.parser')

    def extraer_numero_puro(id_css):
        nodo = soup.find(id=id_css)
        if nodo:
            texto_limpio = ''.join(c for c in nodo.text.replace(',', '.') if c.isdigit() or c == '.')
            try:
                if texto_limpio: return float(texto_limpio)
            except: pass
        return 0.0

    p_loose = extraer_numero_puro("used_price")
    p_cib = extraer_numero_puro("cib_price")
    p_new = extraer_numero_puro("new_price")

    url_final_pc = link_juego if link_juego else url_search

    if p_loose > 0 or p_cib > 0:
        try:
            datos_cache = {
                "juego": nombre_busqueda,
                "consola": consola,
                "loose": p_loose, "cib": p_cib, "new": p_new,
                "url_pc": url_final_pc,
                "created_at": datetime.now().isoformat()
            }
            res_ex = supabase.table('cache_precios').select('id').eq('juego', nombre_busqueda).eq('consola', consola).execute()
            if res_ex.data:
                supabase.table('cache_precios').update(datos_cache).eq('id', res_ex.data[0]['id']).execute()
            else:
                supabase.table('cache_precios').insert(datos_cache).execute()
        except Exception: pass

    rareza_calc = calcular_rareza_ia(nombre, consola, round(p_cib * tipo_cambio, 2))
    mxn_loose_real = round(p_loose * tipo_cambio, 2)
    mxn_cib_real = round(p_cib * tipo_cambio, 2)
    mxn_new_real = round(p_new * tipo_cambio, 2)

    return {
        "status": "ok",
        "mxn_mercado": {"loose": mxn_loose_real, "cib": mxn_cib_real, "new": mxn_new_real},
        "mxn_venta": {
            "loose": calcular_precio_venta_inteligente(mxn_loose_real), 
            "cib": calcular_precio_venta_inteligente(mxn_cib_real), 
            "new": calcular_precio_venta_inteligente(mxn_new_real)
        },
        "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
        "tipo_cambio": tipo_cambio,
        "url_pc": url_final_pc,
        "rareza": rareza_calc
    }

# ==========================================================
# 🌐 RUTAS DE GESTIÓN CRM & ENDPOINTS DE SINCRONIZACIÓN
# ==========================================================
@app.get("/api/cargar_todo")
def cargar_todo(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        columnas_izq = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
        columnas_der = ["Por Entregar", "Vendidos", "Papelera"]
        res_cols = supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', _sesion).execute()
        
        columnas_custom = [r['nombre_columna'] for r in res_cols.data if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)] and r['nombre_columna'].upper() != "EN ATENCION"]
        if not columnas_custom: columnas_custom = ["+"]
        
        columnas_finales = columnas_izq + columnas_custom + columnas_der
        
        res_prospectos = supabase.table('prospectos').select('*').eq('vendedor_id', _sesion).order('ultima_interaccion_ia', desc=True).limit(500).execute()
        
        ultimos = {}
        prospectos_ordenados = sorted(res_prospectos.data, key=lambda x: (x.get('telefono') is None or x.get('telefono') == ""))
        
        for fila in prospectos_ordenados:
            nombre = fila.get('nombre', 'Desconocido')
            tel = fila.get('telefono')
            if nombre not in ultimos:
                ultimos[nombre] = fila
            else:
                if tel and not ultimos[nombre].get('telefono'):
                    ultimos[nombre] = fila
                
        return {"columnas": columnas_finales, "prospectos": list(ultimos.values())}
    except Exception as e:
        logger.exception("❌ Error cargando CRM")
        raise HTTPException(status_code=500, detail="Error conectando a Nube B2B")

# --- 🔄 RUTAS FALTANTES PARA SINCRONIZACIÓN MÓVIL-PC ---
@app.get("/api/perfil_cliente")
async def obtener_perfil_cliente(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table("prospectos").select("notas, etiquetas, columna").eq("telefono", telefono).eq("vendedor_id", vendedor_id).execute()
        if res.data:
            return {"status": "ok", "datos": res.data[0]}
        return {"status": "error", "datos": {}}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table("configuracion").select("nombre_columna").eq("vendedor_id", vendedor_id).execute()
        columnas = [item["nombre_columna"] for item in res.data] if res.data else []
        return {"status": "ok", "columnas": columnas}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        total_hoy = 0.0
        try:
            ventas_res = supabase.table("ventas").select("monto").eq("vendedor_id", vendedor_id).gte("created_at", hoy_inicio).execute()
            if ventas_res.data:
                total_hoy = sum(float(v.get("monto", 0)) for v in ventas_res.data)
        except Exception as ve:
            logger.warning(f"⚠️ No se pudo calcular ventas_hoy: {ve}")

        prospectos_res = (
            supabase.table("prospectos")
            .select("nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas")
            .eq("vendedor_id", vendedor_id)
            .order("ultima_interaccion_ia", desc=True)
            .limit(50)
            .execute()
        )
        
        lista_prospectos = []
        for p in (prospectos_res.data if prospectos_res.data else []):
            p["ultimo_msj"] = p.get("ultimo_msj") or "" 
            p["notas"] = p.get("notas") or ""
            p["etiquetas"] = p.get("etiquetas") or "" 
            lista_prospectos.append(p)

        print(f"📊 [DASHBOARD] V-ID: {vendedor_id} | Ingresos: ${total_hoy} | Leads: {len(lista_prospectos)}")

        return {
            "status": "ok",
            "vendedor": vendedor_id,
            "ventas_hoy": total_hoy,
            "prospectos": lista_prospectos
        }
    except Exception as e:
        logger.error(f"❌ Error crítico en mobile_dashboard: {e}")
        return {"status": "error", "message": str(e), "prospectos": []}

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        query = supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('vendedor_id', _sesion)
        if datos.telefono and datos.telefono != "Sin registrar":
            query = query.eq('telefono', datos.telefono)
        else:
            query = query.eq('nombre', datos.nombre)
        
        query.execute()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error actualizando tarjeta")

@app.post("/api/historial_chat")
def historial_chat(datos: ClienteIdentificador, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res_prospecto = supabase.table('prospectos').select('telefono').eq('nombre', datos.nombre).eq('vendedor_id', _sesion).execute()
        
        tel_oficial = ""
        if res_prospecto.data and res_prospecto.data[0].get('telefono'):
            tel_oficial = res_prospecto.data[0]['telefono']
        
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('vendedor_id', _sesion)
        
        if tel_oficial:
            query = query.eq('telefono', tel_oficial)
        else:
            query = query.eq('nombre', datos.nombre) 

        res = query.order('created_at', desc=False).limit(50).execute()
        
        historial_formateado = []
        for fila in res.data:
            es_mio = (fila.get('autor', 'USER') != 'USER')
            historial_formateado.append({"texto": fila.get('mensaje', ''), "es_mio": es_mio})
            
        return {"historial": historial_formateado, "telefono_oficial": tel_oficial}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Error en historial")

@app.post("/api/mover_prospecto")
def mover_prospecto(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        if datos.telefono and datos.telefono.lower() not in ["", "sin registrar", "null", "none"]:
            supabase.table('prospectos').update({"columna": datos.columna}).eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        else:
            supabase.table('prospectos').update({"columna": datos.columna}).eq('nombre', datos.nombre).eq('vendedor_id', _sesion).execute()
            
        return {"status": "ok", "mensaje": f"Movido a {datos.columna}"}
    except Exception as e:
        print(f"❌ Error moviendo columna: {e}")
        raise HTTPException(status_code=500, detail="Error en base de datos")

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        print(f"📝 Recibida petición de notas para: {datos.nombre} | Tel: {datos.telefono}")
        update_data = {"notas": datos.notas, "etiquetas": datos.etiquetas, "nombre": datos.nombre}
        tel = str(datos.telefono).strip()
        res = None
        
        if tel and tel.lower() not in ["", "null", "sin registrar"]:
            res = supabase.table('prospectos').update(update_data).eq('telefono', tel).eq('vendedor_id', _sesion).execute()

        if not res or len(res.data) == 0:
            print(f"⚠️ No se encontró por teléfono. Intentando por nombre: {datos.nombre}")
            res = supabase.table('prospectos').update(update_data).eq('nombre', datos.nombre).eq('vendedor_id', _sesion).execute()

        if res.data:
            print(f"✅ Notas guardadas con éxito. Filas afectadas: {len(res.data)}")
            return {"status": "ok", "mensaje": "Sincronización completa"}
        else:
            return {"status": "error", "mensaje": "No se encontró el registro"}
    except Exception as e:
        print(f"💥 Error crítico en actualizar_notas: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@app.get("/api/cargar_inventario")
def cargar_inventario(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('inventario').select("*").eq('vendedor_id', _sesion).execute()
        print(f"📦 [INVENTARIO] Sincronizando {len(res.data)} artículos para el ID: {_sesion}")
        return {"status": "ok", "inventario": res.data}
    except Exception as e:
        print(f"❌ [ERROR INVENTARIO] Falló la carga: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno al acceder a la tabla de inventario")

@app.post("/api/crear_columna")
def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').insert({'vendedor_id': _sesion, 'nombre_columna': datos.nombre_columna}).execute()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al crear columna")

@app.post("/api/borrar_columna")
def borrar_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').delete().eq('vendedor_id', _sesion).eq('nombre_columna', datos.nombre_columna).execute()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al borrar columna")

@app.post("/api/renombrar_columna")
def renombrar_columna(datos: RenombrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').update({'nombre_columna': datos.nuevo_nombre}).eq('vendedor_id', _sesion).eq('nombre_columna', datos.viejo_nombre).execute()
        supabase.table('prospectos').update({'columna': datos.nuevo_nombre}).eq('vendedor_id', _sesion).eq('columna', datos.viejo_nombre).execute()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al renombrar columna")

# ==========================================================
# ⚙️ BACKGROUND WORKER DE ENTRADA (CON IDEMPOTENCIA AAA)
# ==========================================================
async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    try:
        if not isinstance(valor, dict) or not isinstance(msg, dict):
            return

        # 🛡️ IDEMPOTENCIA: Evitar procesar el mismo mensaje de Meta 2 veces
        wamid = str(msg.get("id", "")).strip()
        if wamid and wamid in procesados_recientemente:
            logger.info(f"♻️ [WEBHOOK] Mensaje duplicado de Meta ignorado ({wamid}).")
            return
        if wamid: procesados_recientemente.append(wamid)

        try:
            res_config = supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1).execute()
            data_config = res_config.data
        except Exception as db_err:
            logger.error(f"🚨 Error conectando a Supabase (Config): {db_err}")
            data_config = [] 

        if data_config:
            config_vendedor = data_config[0]
            vendedor_actual = str(config_vendedor.get("vendedor_id", "V-001")).strip()
            token_actual = str(config_vendedor.get("meta_token", "")).strip() or WHATSAPP_TOKEN
            nombre_negocio = str(config_vendedor.get("nombre_negocio", "Fantasy Games")).strip()
        else:
            vendedor_actual = "V-001"
            token_actual = WHATSAPP_TOKEN
            nombre_negocio = "Fantasy Games"
            config_vendedor = {
                "vendedor_id": vendedor_actual, "meta_token": token_actual,
                "meta_phone_id": WHATSAPP_PHONE_ID, "nombre_negocio": nombre_negocio,
                "bot_activo": True
            }

        if not token_actual: return
        if not config_vendedor.get("bot_activo", True): return

        contact = valor.get("contacts", [{}])[0]
        nombre_cliente = contact.get("profile", {}).get("name", "Cliente").strip()
        telefono_cliente = str(msg.get("from", "")).strip()

        if telefono_cliente.startswith("521"): telefono_cliente = "52" + telefono_cliente[3:]
        if not telefono_cliente: return

        tipo_mensaje = str(msg.get("type", "text")).lower()
        texto_entrante = ""

        if tipo_mensaje == "text": texto_entrante = msg.get("text", {}).get("body", "").strip()
        elif tipo_mensaje == "image": texto_entrante = "📷 [IMAGEN RECIBIDA: Analizando comprobante de pago con Gemini...]"
        elif tipo_mensaje == "audio": texto_entrante = "🎙️ [NOTA DE VOZ RECIBIDA: Pendiente de transcripción/análisis]"
        elif tipo_mensaje == "interactive": texto_entrante = msg.get("interactive", {}).get("button_reply", {}).get("title", "").strip()
        else: texto_entrante = f"[{tipo_mensaje.upper()}] no soportado por el momento."

        if not texto_entrante: return

        try:
            res_p = supabase.table('prospectos').select('columna, notas, etiquetas').eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_actual).execute()
            if res_p.data:
                columna_actual = res_p.data[0].get("columna", "Bandeja Nueva")
                supabase.table('prospectos').update({"ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()}).eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_actual).execute()
            else:
                columna_actual = "Bandeja Nueva"
                nuevo_p = {
                    "nombre": nombre_cliente, "telefono": telefono_cliente, "origen": "WHATSAPP",
                    "columna": columna_actual, "vendedor_id": vendedor_actual, "estado_iluminacion": "blanco",
                    "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
                }
                supabase.table('prospectos').insert(nuevo_p).execute()
        except Exception as db_crm_err:
            columna_actual = "Bandeja Nueva"

        await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "USER", texto_entrante)

        if tipo_mensaje in ["text", "interactive", "audio"] and columna_actual != "En Conversacion":
            await procesar_respuesta_bot(nombre_cliente, telefono_cliente, texto_entrante, columna_actual, config_vendedor)

        elif tipo_mensaje == "image":
            image_id = msg.get("image", {}).get("id", "").strip()
            if not image_id: return

            historial_para_auditor = await obtener_historial_chat(telefono_cliente, vendedor_actual)
            media_dict = await descargar_media_whatsapp_async(image_id, token_actual)

            if not media_dict:
                logger.warning("⚠️ Falla al descargar la imagen de los servidores de Meta.")
                return

            # Pasamos la data de media al auditor directamente
            auditoria = await auditar_comprobante_ia(
                media_dict["data"], 
                media_dict["mime_type"], 
                nombre_negocio, 
                historial_para_auditor
            )

            es_pago = auditoria.get("es_pago", False)
            monto = float(auditoria.get("monto_detectado", 0.0)) 

            if es_pago:
                await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Por Entregar", "verde_exito", "")
                msg_exito = f"✅ ¡Pago validado por ${monto:.2f} MXN!\nHemos recibido correctamente tu comprobante. Procesando tu entrega..."
                await disparar_whatsapp_dinamico_async(telefono_cliente, msg_exito, token_actual, phone_id_receptor)
                await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_exito)
                logger.info(f"💰 PAGO EXITOSO | {telefono_cliente} | ${monto}")
            else:
                razon = auditoria.get("analisis", "No se reconoce como comprobante válido.")
                msg_fallo = f"🤖 Mi sistema no pudo validar la imagen.\n\nDetalle: {razon}\n\nPor favor envía una foto clara del ticket o comprobante de transferencia."
                await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Requiere Asistencia", "verde_alerta", "")
                await disparar_whatsapp_dinamico_async(telefono_cliente, msg_fallo, token_actual, phone_id_receptor)
                await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_fallo)

    except Exception as e:
        logger.exception(f"❌ [FATAL BACKGROUND TASK ERROR] Colapso en el Worker: {str(e)}")

# ==========================================================
# 🔍 AUDITOR DE COMPROBANTES V14 (EL DÓBERMAN - ANTI-FRAUDE)
# ==========================================================
async def auditar_comprobante_ia(b64_img_data: bytes, mime_type: str, nombre_negocio: str, historial_chat: str):
    def safe_float_local(valor):
        try:
            if valor is None: return 0.0
            limpio = str(valor).replace("$", "").replace(",", "").replace("MXN", "").strip()
            return float(limpio)
        except (ValueError, TypeError): return 0.0

    try:
        from datetime import datetime
        fecha_hoy = datetime.now().strftime("%d de %B de %Y")

        prompt = f"""
Eres el auditor financiero jefe de '{nombre_negocio}'. Tu misión es detectar estafas y pagos que no nos pertenecen.
HISTORIAL DEL CHAT:
{historial_chat}
HOY ES: {fecha_hoy}

REGLAS DE RECHAZO RADICAL (Si ocurre UNA, "es_pago": false):
1. NATURALEZA: Debe ser un recibo bancario real. No fotos de objetos o personas.
2. DESTINATARIO: Si el beneficiario es un "Colegio", "A.C.", "CFE", etc., RECHÁZALO. 
3. VIGENCIA: La transferencia debe ser de hoy ({fecha_hoy}) o ayer. Si es vieja, es fraude.
4. COHERENCIA: Si el monto es muy alto pero en el chat se habló de un juego barato, rechaza por sospecha.

RESPONDE EXCLUSIVAMENTE EN JSON:
{{
    "es_pago": true,
    "monto_detectado": 0.0,
    "destinatario": "Nombre detectado",
    "analisis": "Breve explicación."
}}
"""
        # Re-usamos la super función centralizada de Gemini!
        data = await consultar_gemini_json(prompt, {"mime_type": mime_type, "data": b64_img_data}, temperature=0.0)
        
        return {
            "es_pago": bool(data.get("es_pago", False)),
            "monto_detectado": safe_float_local(data.get("monto_detectado", 0)),
            "analisis": str(data.get("analisis", "Análisis no disponible."))
        }
    except Exception as e:
        logger.exception(f"❌ ERROR auditar_comprobante_ia: {str(e)}")
        return {"es_pago": False, "monto_detectado": 0.0, "analisis": "Error interno al auditar."}


# ==========================================================
# 🤖 MÓDULO DE WEBHOOK: CONEXIÓN AL MOTOR IA VELTRIX
# ==========================================================
@app.get("/webhook")
def verificar_webhook(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == WEBHOOK_SECRET:
        print("✅ [WEBHOOK] Servidor validado con éxito por Meta.")
        try: return int(params.get("hub.challenge"))
        except: return params.get("hub.challenge")
    raise HTTPException(status_code=403, detail="Token de verificación inválido")

@app.post("/webhook")
async def recibir_mensajes(request: Request, background_tasks: BackgroundTasks):
    # 🛡️ Validación Criptográfica de Meta antes de leer el mensaje
    await validar_firma_meta(request)
    
    try:
        body = await request.json()
        if not body.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("messages"):
            return {"status": "ignored_update"}

        print("\n--- 📥 [NUEVO MENSAJE RECIBIDO] ---")
        value = body["entry"][0]["changes"][0]["value"]
        message = value.get("messages", [{}])[0]
        phone_id_receptor = value.get("metadata", {}).get("phone_number_id", WHATSAPP_PHONE_ID)

        background_tasks.add_task(gestionar_mensaje_entrante_bg, value, message, phone_id_receptor)
        
        print("✅ [WEBHOOK] Mensaje transferido al Motor de IA Veltrix con éxito.")
        print("----------------------------------\n")
        return {"status": "ok"}

    except Exception as e:
        print(f"⚠️ [WEBHOOK ERROR] Fallo al procesar mensaje: {str(e)}")
        return {"status": "error", "reason": str(e)}

# ==========================================================
# 🏁 ANCLAJE FINAL Y ARRANQUE DEL SERVIDOR
# ==========================================================
app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    import os
    puerto_render = int(os.environ.get("PORT", 10000))
    logger.info(f"⚡ Iniciando Uvicorn Server en el puerto {puerto_render}...")
    uvicorn.run("main:app", host="0.0.0.0", port=puerto_render, reload=False)
