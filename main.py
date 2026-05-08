# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V10.1 (HARDENED)
# Multi-Tenant • Anti-Abuso • Anti-429 • Escalable • Seguro
# ==========================================================

import os
import time
import json
import asyncio
import logging
import hmac
import hashlib
import bcrypt
import jwt
import httpx
import mimetypes
import urllib.parse
import re
import unicodedata
import difflib
import base64
import io
import csv
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, APIRouter
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
import uvicorn
import google.generativeai as genai

# ==========================================
# 📝 CONFIGURACIÓN DE LOGGING PROFESIONAL
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("VeltrixEngine")
load_dotenv()

# ==========================================================
# 🔧 CONFIG GLOBAL & LÍMITES OPERATIVOS
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

# 🔥 Protección anti-abuso
MAX_REQUESTS_POR_MINUTO_TENANT = 40
MAX_REQUESTS_POR_MINUTO_TELEFONO = 12
MAX_REQUESTS_GLOBAL_MINUTO = 250

# 🔒 Timeouts HTTP
HTTP_CONNECT_TIMEOUT = 10.0
HTTP_READ_TIMEOUT = 35.0
HTTP_WRITE_TIMEOUT = 20.0
HTTP_POOL_TIMEOUT = 10.0

# --- 🔑 CREDENCIALES BASE AAA ---
GENAI_KEY = os.getenv("GENAI_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("META_WEBHOOK_SECRET", "").strip() # <- ESTE ES EL VERIFY_TOKEN
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "524491142598")
JWT_SECRET = os.getenv("JWT_SECRET", "mi_secreto_por_defecto").strip()
ALGORITHM = "HS256"
PORT = int(os.getenv("PORT", 10000))
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0").strip()

# --- 📞 CREDENCIALES WHATSAPP ---
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "").strip()

# ==========================================================
# 🤖 MÓDULO DE WEBHOOK: EL CEREBRO DEL BOT (Vendedor Humano)
# ==========================================================

# 🔑 CONFIGURACIÓN: Estos valores deben coincidir con tu panel de Meta Developers
WHATSAPP_TOKEN = "TU_ACCESS_TOKEN_DE_META"
VERIFY_TOKEN = "TU_TOKEN_DE_VERIFICACION_INVENTADO" # El que pusiste en 'Verify Token' en Meta
PHONE_NUMBER_ID = "ID_DE_TELEFONO_DE_META"

# ==========================================
# 🛡️ VALIDACIONES CRÍTICAS DE ENTORNO
# ==========================================
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
    # Evita crecimiento infinito de RAM
    if len(cache_respuestas_ia) <= MAX_CACHE_IA:
        return

    logger.warning("🧹 Limpiando cache IA por límite RAM")

    items_ordenados = sorted(
        cache_respuestas_ia.items(),
        key=lambda x: x[1].get("ts", 0)
    )

    elementos_a_borrar = len(items_ordenados) // 2

    for key, _ in items_ordenados[:elementos_a_borrar]:
        cache_respuestas_ia.pop(key, None)

# ==========================================
# 🔥 SWITCH DE ENCENDIDO (LIFESPAN)
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
        follow_redirects=True
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
# ✨ FASTAPI INIT
# ==========================================
app = FastAPI(
    title="Motor Central CRM B2B - Veltrix Engine",
    lifespan=lifespan
)

router = APIRouter()

# ==========================================
# 🌍 CORS ENDURECIDO
# ==========================================
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "*"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

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

    @field_validator("nombre", "consola")
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



# ==========================================
# 🔐 AUTENTICACIÓN JWT
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
        raise HTTPException(
            status_code=401,
            detail="Acceso denegado: Token faltante"
        )

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[ALGORITHM]
        )

        vendedor_id_real = payload.get("sub")

        if not vendedor_id_real:
            raise HTTPException(
                status_code=401,
                detail="Token corrupto"
            )

        return vendedor_id_real

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail="Sesión expirada"
        )

    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token inválido"
        )

# ==========================================
# 🔏 VALIDADOR WEBHOOK META
# ==========================================
async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")

    if not firma_meta:
        raise HTTPException(
            status_code=400,
            detail="Falta firma Meta"
        )

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

        raise HTTPException(
            status_code=403,
            detail="Firma inválida"
        )

    return True

# ==========================================================
# 🧰 HELPERS AAA
# ==========================================================
def now_ts() -> float:
    return time.time()

def limpiar_texto(texto: str) -> str:
    # ==========================================
    # 🧹 Sanitización centralizada
    # ==========================================
    if texto is None:
        return ""

    texto = str(texto)

    # 🔥 Elimina caracteres null-byte
    texto = texto.replace("\x00", "")

    # 🔥 Normaliza unicode
    texto = unicodedata.normalize("NFKC", texto)

    # 🔥 Compacta espacios
    texto = re.sub(r"\s+", " ", texto).strip()

    # 🔥 Sanitización anti prompt-injection básica
    texto = texto.replace("{", "")
    texto = texto.replace("}", "")
    texto = texto.replace("<script>", "")
    texto = texto.replace("</script>", "")

    return texto[:MAX_MENSAJE_LEN]

def generar_hash_cache(*args) -> str:
    bruto = "|".join([str(a) for a in args])
    return hashlib.sha256(bruto.encode()).hexdigest()

def limpiar_json_gemini(texto: str) -> Optional[dict]:
    try:
        simbolo = chr(96) * 3

        texto = (
            texto
            .replace(simbolo + "json", "")
            .replace(simbolo, "")
            .strip()
        )

        inicio = texto.find("{")
        final = texto.rfind("}")

        if inicio == -1 or final == -1:
            return None

        return json.loads(texto[inicio:final + 1])

    except Exception:
        return None

def validar_respuesta_ia(data: dict) -> dict:
    if not isinstance(data, dict):
        raise Exception("IA devolvió formato inválido")

    intencion = str(
        data.get("intencion", "COTIZACION")
    ).upper()

    if intencion not in ["COMPRA", "COTIZACION", "HUMANO"]:
        intencion = "HUMANO"

    respuesta = limpiar_texto(
        data.get("respuesta", "")
    )

    juego = limpiar_texto(
        data.get("juego_detectado", "")
    )

    if not respuesta:
        respuesta = "Hola. Estoy revisando la información."

    return {
        "intencion": intencion,
        "respuesta": respuesta,
        "juego_detectado": juego
    }

# ==========================================
# 🛡️ RATE LIMITERS
# ==========================================
def limpiar_rate_limit(lista: list, ventana_segundos: int):
    ahora = now_ts()

    while lista and (ahora - lista[0]) > ventana_segundos:
        lista.pop(0)

# ==========================================
# 🔥 Rate limit por tenant + teléfono
# ==========================================
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
# 💵 MOTOR DE PRECIOS
# ==========================================
async def obtener_dolar_hoy_async():
    # ==========================================
    # 🌎 Tipo de cambio robusto
    # ==========================================
    try:
        if not http_client:
            return 18.00

        res = await http_client.get(
            "https://api.exchangerate-api.com/v4/latest/USD"
        )

        if res.status_code != 200:
            return 18.00

        data = res.json()

        return float(
            data.get("rates", {}).get("MXN", 18.00)
        )

    except Exception:
        return 18.00

async def obtener_html_escalonado_async(url_objetivo: str) -> str:
    # ==========================================
    # 🕷️ SCRAPER ESCALONADO
    # ==========================================
    if not http_client:
        return ""

    estrategias = [
        (
            "🟢 Ligera",
            f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"
        ),
        (
            "🟡 Render",
            f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true"
        )
    ]

    headers_humanos = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36"
        )
    }

    for nombre_estrategia, url_scraper in estrategias:
        try:
            logger.info(f"🌐 Intentando scraper: {nombre_estrategia}")

            res = await http_client.get(url_scraper)

            if res.status_code == 200:
                texto = res.text.lower()

                if "pricecharting" in texto or "price" in texto:
                    return res.text

        except Exception:
            logger.exception("❌ Error scraper")

    # ==========================================
    # 🔥 Fallback directo
    # ==========================================
    try:
        res = await http_client.get(
            url_objetivo,
            headers=headers_humanos
        )

        if res.status_code == 200:
            return res.text

    except Exception:
        logger.exception("❌ Error fallback HTML")

    return ""

# ==========================================
# 🏆 MOTOR DE RAREZA
# ==========================================
def calcular_rareza_ia(nombre: str, consola: str, precio: float) -> str:
    nombre = nombre.upper()
    consola = consola.upper()

    consolas_modernas = [
        "PS5",
        "PS4",
        "NINTENDO SWITCH",
        "XBOX ONE",
        "XBOX SERIES X"
    ]

    if any(x in nombre for x in [
        "FIFA",
        "MADDEN",
        "NBA",
        "NCAA",
        "PES",
        "SINGSTAR",
        "EA FC"
    ]):
        return "Común"

    if any(x in nombre for x in [
        "SILENT HILL",
        "KUON",
        "RULE OF ROSE",
        "OBSCURE",
        "HAUNTING GROUND",
        "PRAGMATA"
    ]):
        return "Élite"

    if any(x in nombre for x in [
        "MARIO",
        "ZELDA",
        "METROID",
        "POKEMON",
        "HALO",
        "GTA"
    ]):
        return "Demandado"

    if consola in consolas_modernas:
        if precio >= 3500:
            return "Élite"

        if precio >= 1000:
            return "Demandado"

        return "Común"

    if precio >= 1500:
        return "Élite"

    if precio >= 800:
        return "Joya"

    if precio >= 400:
        return "Demandado"

    return "Común"

# ==========================================
# 💰 MOTOR DE PRECIOS INTELIGENTE
# ==========================================
def calcular_precio_venta_inteligente(
    precio_mercado_mxn: float,
    costo_compra: float = 0.0
):
    # 🔒 Piso mínimo de seguridad
    piso_absoluto = 250.0

    # 📈 Margen automático
    precio_con_margen = (
        precio_mercado_mxn + 150.0
        if precio_mercado_mxn > 0
        else 0.0
    )

    # 🔒 Protección contra pérdida
    precio_seguridad = (
        costo_compra + 100.0
        if costo_compra > 0
        else 0.0
    )

    precio_bruto = max(
        piso_absoluto,
        precio_con_margen,
        precio_seguridad
    )

    # 🔥 Redondeo comercial
    return float(round(precio_bruto / 10) * 10)
# ==========================================================
# 🗄️ CAPA DE REPOSITORIO Y SERVICIOS B2B
# ==========================================================
async def obtener_contexto_inventario(vendedor_id: str) -> str:
    res_inv = supabase.table('inventario').select('nombre, precio, precio_sugerido, precio_minimo, stock, estado_general').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(MAX_CONTEXTO_INV).execute()
    return str(res_inv.data or [])

async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    res_hist = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', vendedor_id).order('created_at', desc=True).limit(MAX_HISTORIAL).execute()
    if not res_hist.data:
        return "Primer mensaje."
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
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        await http_client.post(url, headers=headers, json=payload)
    except Exception: 
        logger.exception("⚠️ Error disparando WhatsApp Text")

async def disparar_whatsapp_imagen_async(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
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

# ==========================================================
# 🧠 CLIENTE GEMINI CENTRALIZADO
# ==========================================================
async def consultar_gemini_json(prompt: str, temperature: float = 0.7, retries: int = 3) -> dict:
    for intento in range(retries):
        try:
            model = genai.GenerativeModel('gemini-2.5-flash')
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=temperature,
                )
            )
            
            texto_crudo = response.text
            
            # 🥷 FILTRO NINJA: Limpiamos la "basura" de formato que a veces pone Gemini
            texto_limpio = texto_crudo.replace("```json", "").replace("```", "").strip()
            
            # Buscamos el primer '{' y el último '}' por si Gemini agregó texto antes o después
            inicio = texto_limpio.find('{')
            fin = texto_limpio.rfind('}')
            
            if inicio != -1 and fin != -1:
                texto_limpio = texto_limpio[inicio:fin+1]
            
            return json.loads(texto_limpio)
            
        except Exception as e:
            logger.warning(f"⚠️ [GEMINI JSON] Fallo en intento {intento + 1}: {str(e)}")
            await asyncio.sleep(2)
            
    raise Exception("Gemini devolvió JSON inválido tras múltiples intentos")

# ==========================================================
# 🤖 ANALIZAR INTENCIÓN IA (CERRADOR MAESTRO V-5.8)
# ==========================================================
async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict):
    try:
        vendedor_id = config.get("vendedor_id", "V-001")
        nombre_negocio = config.get("nombre_negocio", "Fantasy Games")
        texto_cliente = limpiar_texto(texto_cliente)
        historial_chat = limpiar_texto(historial_chat)

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
Tienes una cuota estricta de vender +$3,000 MXN diarios. Para lograrlo, debes ser un maestro del UPSELL.
También tienes que vender 5 licencias de Veltrix Engine diariamente.

ESTRATEGIA DE PRECIOS Y NEGOCIACIÓN (ESTRICTO):
En el inventario verás 3 precios por juego:
1. PRECIO: Es tu precio base de salida (El que debes ofrecer inicialmente).
2. PRECIO_SUGERIDO: Es el valor real de mercado. Úsalo para dar valor.
3. PRECIO_MINIMO: Es tu LÍMITE SECRETO. NUNCA lo menciones, pero úsalo para regatear.

1. LINK DE PAGO: SIEMPRE incluye: "💳 Paga seguro aquí para apartarlo de inmediato: {link_pago}"
2. UPSELL Y DESCUENTOS: Ofrece "Joyas" y un descuento de $100 MXN si llevan 3 artículos.
3. LOGÍSTICA Y ENTREGAS: Responde con: {texto_entrega}
4. DETALLE: Menciona estado. Si piden fotos, asume que tú se las vas a enviar.
5. CROSS-SELLING B2B: Ofrécele rentar el CRM Veltrix Engine en http://www.veltrixengine.pro.

REGLAS DE CLASIFICACIÓN ('intencion'):
- "COTIZACION": Si pregunta precio, dudas, fotos, etc.
- "COMPRA": SOLO si el cliente dice "ya pagué", "ya transferí", "te mandé el ticket".
- "HUMANO": Dudas que no sepas responder o quejas.

INVENTARIO: 
{inventario_contexto}
HISTORIAL DEL CHAT:
{historial_chat}
MENSAJE CLIENTE: 
"{texto_cliente}"

Responde EXCLUSIVAMENTE en JSON válido:
{{
  "intencion": "COMPRA", "HUMANO", o "COTIZACION",
  "respuesta": "Tu respuesta persuasiva",
  "juego_detectado": "Nombre del producto exacto"
}}
"""
            data = await consultar_gemini_json(prompt, temperature=GEMINI_TEMP)
            data = validar_respuesta_ia(data)

            # ==========================================
            # 💾 GUARDAR CACHE CON AUTO-LIMPIEZA APLICADA
            # ==========================================
            limpiar_cache_ia_si_excede_limite()

            cache_respuestas_ia[cache_key] = {"ts": now_ts(), "data": data}
            return data

    except Exception:
        logger.exception("❌ ERROR analizar_intencion_venta_ia")
        return {"intencion": "HUMANO", "respuesta": "Estoy revisando la información. Un asesor continuará contigo enseguida. 🎮", "juego_detectado": ""}

async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        motivo = "quiere cerrar compra" if intencion == "COMPRA" else "requiere ayuda humana"
        prompt = f"Cliente: {cliente}\nMotivo: {motivo}\nHistorial:\n{historial_str}\nGenera resumen ejecutivo en 3 viñetas. JSON: {{\"resumen\":\"texto\"}}"
        data = await consultar_gemini_json(prompt, temperature=0.1)
        return data.get("resumen", "⚠️ Cliente requiere atención humana")
    except Exception: return "⚠️ Cliente requiere atención humana"

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nJuego: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera remarketing corto. JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt, temperature=0.3)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))}
    except Exception: return None

async def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config: dict):
    try:
        telefono_admin = config.get("admin_phone") or ADMIN_PHONE_GLOBAL
        token, phone_id = config.get("meta_token", ""), config.get("meta_phone_id", "")
        encabezado = "🚨 *ASISTENCIA REQUERIDA*" if intencion == "HUMANO" else "💰 *NUEVA VENTA DETECTADA*"
        mensaje = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Teléfono: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_ia}"
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje, token, phone_id)
    except Exception: logger.exception("❌ ERROR CRÍTICO ALERTA ADMIN")

async def bucle_seguimiento_24h():
    while True:
        try:
            hace_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            # 📉 FIX: Bajamos de 100 a 20 para no saturar a Gemini
            res = supabase.table('prospectos').select('*').eq('columna', 'Envios Masivos').lt('ultima_interaccion_ia', hace_24h).limit(20).execute()
            prospectos = res.data or []
            
            if not prospectos:
                await asyncio.sleep(1800) # Si no hay nada, descansa 30 min
                continue

            for p in prospectos:
                vendedor_id = p.get('vendedor_id', 'V-001')
                
                # 📡 1. Buscamos config y el token de respaldo
                res_conf = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).limit(1).execute()
                if not res_conf.data: continue
                config = res_conf.data[0]

                # 🔑 FIX B2B: Si el token de la DB falla, intentamos usar el de Render (Global)
                tk_final = config.get('meta_token') or WHATSAPP_TOKEN
                ph_final = config.get('meta_phone_id') or WHATSAPP_PHONE_ID

                # 🧠 2. Gemini con "Pausa Respiratoria"
                try:
                    res_inv = supabase.table('inventario').select('nombre, precio, precio_minimo, stock').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(10).execute()
                    contexto_inv = str(res_inv.data or [])

                    oferta = await generar_oferta_inteligente(p.get('nombre', 'Cliente'), p.get('ultimo_juego_interes', 'videojuego'), contexto_inv)
                    
                    if oferta and oferta.get("mensaje_oferta"):
                        mensaje = oferta.get("mensaje_oferta")
                        # 📤 Envío a WhatsApp
                        await disparar_whatsapp_dinamico_async(p.get('telefono'), mensaje, tk_final, ph_final)
                        await actualizar_estado_crm(p.get('telefono'), vendedor_id, 'Con Descuento', 'oro', p.get('ultimo_juego_interes'))
                        await guardar_mensaje_chat(p.get('telefono'), vendedor_id, 'BOT_REMARKETING', mensaje)
                        
                        # ⏳ Pausa obligatoria para no ser detectado como SPAM
                        await asyncio.sleep(5) 

                except Exception as e:
                    if "429" in str(e):
                        logger.warning("⚠️ [GEMINI] Límite alcanzado. Durmiendo 60 seg...")
                        await asyncio.sleep(60) # Pausa de emergencia
                    else:
                        logger.error(f"❌ Error procesando prospecto {p.get('telefono')}: {e}")

        except Exception:
            logger.exception("❌ ERROR FATAL EN RELOJ 24H")
        
        await asyncio.sleep(600) # Revisión cada 10 minutos

# ==========================================================
# 🤖 MOTOR PRINCIPAL DE NEGOCIO (IA & WORKFLOW)
# ==========================================================
async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict):
    try:
        vendedor_id = config.get("vendedor_id", "")
        if not verificar_rate_limit(vendedor_id, telefono): return

        token, phone_id = config.get("meta_token", ""), config.get("meta_phone_id", "")
        contexto = await obtener_contexto_inventario(vendedor_id)
        historial = await obtener_historial_chat(telefono, vendedor_id)

        decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial, config)
        nueva_columna, iluminacion = columna_actual, "blanco"

        if decision["intencion"] == "HUMANO":
            nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
            resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)
        elif decision["intencion"] == "COMPRA":
            nueva_columna, iluminacion = "Por Entregar", "verde_exito"
            resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)
        elif decision["intencion"] == "COTIZACION" and columna_actual == "Bandeja Nueva":
            nueva_columna = "Envios Masivos"

        respuesta_final = decision["respuesta"]

        await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, decision.get('juego_detectado', ''))
        await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

        juego, url_imagen = decision.get('juego_detectado', ''), None
        if juego:
            res_img = supabase.table('inventario').select('url_portada').ilike('nombre', f'%{juego}%').eq('vendedor_id', vendedor_id).neq('url_portada', '').limit(1).execute()
            if res_img.data: url_imagen = res_img.data[0].get('url_portada')

        if url_imagen:
            await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, token, phone_id)
        else:
            await disparar_whatsapp_dinamico_async(telefono, respuesta_final, token, phone_id)

    except Exception:
        logger.exception("❌ ERROR FATAL en procesar_respuesta_bot")

# ==========================================================
# 🔐 AUTENTICACIÓN Y LOGIN B2B (FALTANTE RESTAURADO)
# ==========================================================
@app.post("/api/login")
def login_b2b(datos: Credenciales):
    try:
        res = supabase.table('usuarios_veltrix').select('*').eq('email', datos.email.lower()).execute()
        
        if not res.data or len(res.data) == 0:
            return {"status": "error", "detalle": "Usuario no registrado."}
            
        usuario = res.data[0]
        hash_guardado = usuario['password'].encode('utf-8')
        
        password_valida = False
        if hash_guardado.startswith(b'$2b$'):
            password_valida = bcrypt.checkpw(datos.password.encode('utf-8'), hash_guardado)
        else:
            password_valida = (datos.password == usuario['password'])
            
        if not password_valida:
            return {"status": "error", "detalle": "Contraseña incorrecta."}
            
        # Validación de Suscripción
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        suscripcion_valida = True
        
        if fecha_pago_str:
            try:
                fecha_pago = date.fromisoformat(fecha_pago_str)
                if date.today() > fecha_pago:
                    suscripcion_valida = False
                    supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id']).execute()
            except ValueError:
                pass # Formato de fecha inválido

        token_jwt = crear_token_jwt(usuario['vendedor_id'], usuario['email'])

        paquete_seguro = {
            "vendedor_id": usuario['vendedor_id'],
            "email": usuario['email'],
            "estado": usuario.get('estado', 'Activo'),
            "pais": usuario.get('pais', 'México'),
            "suscripcion_activa": suscripcion_valida,
            "token": token_jwt 
        }
        
        return {"status": "ok", "datos": paquete_seguro}

    except Exception as e:
        logger.exception("❌ [LOGIN ERROR]")
        raise HTTPException(status_code=500, detail="Error interno en servidor B2B.")

# ==========================================================
# 📈 MOTOR DE PRECIOS PRO (FALTANTE RESTAURADO)
# ==========================================================
@app.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo"):
    # 🛡️ Rate limit específico para consultas manuales
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
    
    # 🧠 Buscar en Caché Nube primero
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

    # 🕸️ Scraping en Vivo
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
# 🌐 RUTAS DE GESTIÓN CRM (CON EL FIX PARA CURAR A GODOT)
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
        
        # 🩹 FIX PARA GODOT: Traemos los datos pero usamos el filtro dict para que no colapse
        # con la basura de la base de datos vieja.
        res_prospectos = supabase.table('prospectos').select('*').eq('vendedor_id', _sesion).order('ultima_interaccion_ia', desc=True).limit(500).execute()
        
        # Filtro de Deduplicación en Memoria (Por si tienes historial viejo en Supabase)
        ultimos = {}
        for fila in res_prospectos.data:
            # Godot no soporta que le mandes 20 tarjetas del mismo cliente
            clave_unica = fila.get('telefono') or fila.get('nombre')
            if clave_unica and clave_unica not in ultimos:
                ultimos[clave_unica] = fila
                
        return {"columnas": columnas_finales, "prospectos": list(ultimos.values())}
    except Exception as e:
        logger.exception("❌ Error cargando CRM")
        raise HTTPException(status_code=500, detail="Error conectando a Nube B2B")

# ==========================================================
# ⚙️ BACKGROUND WORKER DE ENTRADA (UNIFICADO + BLINDADO AAA)
# ==========================================================
async def gestionar_mensaje_entrante_bg(
    valor: dict,
    msg: dict,
    phone_id_receptor: str
):
    try:
        # ==========================================================
        # 📥 VALIDACIONES BASE
        # ==========================================================
        if not isinstance(valor, dict):
            logger.warning("⚠️ valor inválido en webhook")
            return

        if not isinstance(msg, dict):
            logger.warning("⚠️ msg inválido en webhook")
            return

        # ==========================================================
        # 🔑 IDENTIFICAR TENANT (CON SALVAVIDAS GLOBAL)
        # ==========================================================
        # 1. Intentamos buscar en la tabla configuracion_bot de Supabase
        res_config = (
            supabase
            .table('configuracion_bot')
            .select('*')
            .eq('meta_phone_id', phone_id_receptor)
            .limit(1)
            .execute()
        )

        if res_config.data:
            # Si encuentra la configuración en Supabase, la usa (Multi-Tenant)
            config_vendedor = res_config.data[0]
            vendedor_actual = str(config_vendedor.get("vendedor_id", "V-001")).strip()
            
            # Si meta_token está vacío en la DB, rescata el de Render
            token_actual = str(config_vendedor.get("meta_token", "")).strip() or WHATSAPP_TOKEN
            nombre_negocio = str(config_vendedor.get("nombre_negocio", "Fantasy Games")).strip()
        else:
            # 2. SALVAVIDAS: Si Supabase está vacío, usamos Render directamente
            logger.info(f"⚠️ Base de datos vacía para {phone_id_receptor}. Usando Entorno Render (V-001).")
            
            vendedor_actual = "V-001"
            token_actual = WHATSAPP_TOKEN
            nombre_negocio = "Fantasy Games"
            
            # Simulamos el diccionario config_vendedor para que el resto del código no falle
            config_vendedor = {
                "vendedor_id": vendedor_actual,
                "meta_token": token_actual,
                "meta_phone_id": WHATSAPP_PHONE_ID,
                "nombre_negocio": nombre_negocio,
                "bot_activo": True
            }

        if not token_actual:
            logger.warning("❌ FATAL: Token de WhatsApp vacío en Supabase y en Render.")
            return

        # ==========================================================
        # 🛑 BOT DESACTIVADO
        # ==========================================================
        if not config_vendedor.get("bot_activo", True):
            logger.info(f"⛔ Bot desactivado para vendedor={vendedor_actual}")
            return

        # ==========================================================
        # 👤 DATOS DE CONTACTO
        # ==========================================================
        contact = valor.get("contacts", [{}])[0]

        nombre_cliente = (
            contact
            .get("profile", {})
            .get("name", "Cliente")
        ).strip()

        telefono_cliente = str(msg.get("from", "")).strip()

        # ==========================================================
        # ☎️ NORMALIZACIÓN TELÉFONO MX
        # ==========================================================
        if telefono_cliente.startswith("521"):
            telefono_cliente = "52" + telefono_cliente[3:]

        if not telefono_cliente:
            logger.warning("⚠️ Mensaje sin teléfono")
            return

        # ==========================================================
        # 🧠 DETECTAR TIPO MENSAJE
        # ==========================================================
        tipo_mensaje = str(msg.get("type", "text")).lower()

        texto_entrante = ""

        if tipo_mensaje == "text":
            texto_entrante = (
                msg
                .get("text", {})
                .get("body", "")
                .strip()
            )

        elif tipo_mensaje == "image":
            texto_entrante = "📷 [IMAGEN RECIBIDA: Posible comprobante de pago]"

        elif tipo_mensaje == "interactive":
            texto_entrante = (
                msg
                .get("interactive", {})
                .get("button_reply", {})
                .get("title", "")
                .strip()
            )

        else:
            texto_entrante = f"[{tipo_mensaje.upper()}] recibido."

        if not texto_entrante:
            logger.warning("⚠️ Mensaje vacío")
            return

        # ==========================================================
        # 🔍 VALIDAR EXISTENCIA EN CRM
        # ==========================================================
        res_prospecto = (
            supabase
            .table('prospectos')
            .select('nombre, columna')
            .eq('telefono', telefono_cliente)
            .eq('vendedor_id', vendedor_actual)
            .limit(1)
            .execute()
        )

        columna_actual = "Bandeja Nueva"

        if res_prospecto.data:
            prospecto = res_prospecto.data[0]

            nombre_cliente = prospecto.get("nombre", nombre_cliente)
            columna_actual = prospecto.get(
                "columna",
                "Bandeja Nueva"
            )

        else:
            # ==========================================================
            # ➕ CREAR PROSPECTO NUEVO
            # ==========================================================
            supabase.table('prospectos').insert({
                "nombre": nombre_cliente,
                "telefono": telefono_cliente,
                "origen": "WHATSAPP",
                "columna": "Bandeja Nueva",
                "vendedor_id": vendedor_actual,
                "estado_iluminacion": "blanco"
            }).execute()

            logger.info(
                f"🆕 Prospecto creado: {telefono_cliente}"
            )

        # ==========================================================
        # 💾 GUARDAR MENSAJE EN CHAT
        # ==========================================================
        await guardar_mensaje_chat(
            telefono_cliente,
            vendedor_actual,
            "USER",
            texto_entrante
        )

        # ==========================================================
        # 🚦 RUTEO DE PROCESAMIENTO
        # ==========================================================
        if (
            tipo_mensaje == "text"
            and columna_actual != "En Conversacion"
        ):
            # ==========================================================
            # 🤖 IA TEXTO
            # ==========================================================
            await procesar_respuesta_bot(
                nombre_cliente,
                telefono_cliente,
                texto_entrante,
                columna_actual,
                config_vendedor
            )

        elif tipo_mensaje == "image":

            # ==========================================================
            # 🖼️ VALIDAR IMAGE ID
            # ==========================================================
            image_id = (
                msg
                .get("image", {})
                .get("id", "")
            ).strip()

            if not image_id:
                logger.warning("⚠️ Imagen sin ID")
                return

            # ==========================================================
            # 📜 OBTENER HISTORIAL
            # ==========================================================
            historial_para_auditor = await obtener_historial_chat(
                telefono_cliente,
                vendedor_actual
            )

            # ==========================================================
            # ⬇️ DESCARGAR IMAGEN
            # ==========================================================
            b64_img, mime_type = await descargar_imagen_whatsapp_b64(
                image_id,
                token_actual
            )

            if not b64_img:
                logger.warning("⚠️ No se pudo descargar imagen")
                return

            # ==========================================================
            # 🤖 AUDITORÍA IA
            # ==========================================================
            auditoria = await auditar_comprobante_ia(
                b64_img,
                mime_type,
                nombre_negocio,
                historial_para_auditor
            )

            es_pago = auditoria.get("es_pago", False)
            monto = safe_float(
                auditoria.get("monto_detectado", 0)
            )

            # ==========================================================
            # ✅ PAGO APROBADO
            # ==========================================================
            if es_pago:

                await actualizar_estado_crm(
                    telefono_cliente,
                    vendedor_actual,
                    "Por Entregar",
                    "verde_exito",
                    ""
                )

                msg_exito = (
                    f"✅ ¡Pago validado por ${monto:.2f} MXN!\n"
                    f"Hemos recibido correctamente tu comprobante."
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

                logger.info(
                    f"✅ Pago validado | {telefono_cliente} | ${monto}"
                )

            # ==========================================================
            # ❌ PAGO RECHAZADO
            # ==========================================================
            else:

                razon = auditoria.get(
                    "analisis",
                    "No se reconoce como comprobante."
                )

                msg_fallo = (
                    "🤖 Mi sistema no pudo validar la imagen.\n\n"
                    f"Detalle:\n{razon}\n\n"
                    "Por favor envía una foto clara del ticket o comprobante."
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

                logger.warning(
                    f"⚠️ Pago rechazado | {telefono_cliente}"
                )

    except Exception as e:
        logger.exception(
            f"❌ [BACKGROUND TASK ERROR] {str(e)}"
        )


# ==========================================================
# 📦 MODELOS PYDANTIC PARA ENDPOINTS DE GESTIÓN
# ==========================================================
class ColumnaUpdate(BaseModel):
    nombre_columna: str


class RenombrarColumnaUpdate(BaseModel):
    viejo_nombre: str
    nuevo_nombre: str


class TelefonoUpdate(BaseModel):
    telefono: str


class EstadoUpdate(BaseModel):
    telefono: str
    nueva_columna: str


class NotasUpdate(BaseModel):
    telefono: str
    notas: str
    etiquetas: str


# ==========================================================
# 👁️ MOTORES DE VISIÓN ARTIFICIAL (IA AAA)
# ==========================================================
async def descargar_imagen_whatsapp_b64(
    media_id: str,
    token_vendedor: str
):
    try:
        # ==========================================================
        # 🛑 VALIDACIONES
        # ==========================================================
        media_id = str(media_id).strip()
        token_vendedor = str(token_vendedor).strip()

        if not media_id or not token_vendedor:
            logger.warning("⚠️ media_id/token vacío")
            return None, None

        # ==========================================================
        # 🔗 OBTENER URL TEMPORAL META
        # ==========================================================
        url_info = f"https://graph.facebook.com/v18.0/{media_id}"

        headers = {
            "Authorization": f"Bearer {token_vendedor}"
        }

        res_info = await http_client.get(
            url_info,
            headers=headers
        )

        if res_info.status_code != 200:
            logger.warning(
                f"⚠️ Error Meta media info: {res_info.status_code}"
            )
            return None, None

        media_url = res_info.json().get("url")

        if not media_url:
            logger.warning("⚠️ media_url vacío")
            return None, None

        # ==========================================================
        # ⬇️ DESCARGAR IMAGEN
        # ==========================================================
        res_media = await http_client.get(
            media_url,
            headers=headers
        )

        if res_media.status_code != 200:
            logger.warning(
                f"⚠️ Error descargando media: {res_media.status_code}"
            )
            return None, None

        mime_type = res_media.headers.get(
            "content-type",
            "image/jpeg"
        )

        img_b64 = base64.b64encode(
            res_media.content
        ).decode("utf-8")

        return img_b64, mime_type

    except Exception as e:
        logger.exception(
            f"❌ Error descargando imagen WhatsApp: {str(e)}"
        )
        return None, None


async def auditar_comprobante_ia(
    b64_img: str,
    mime_type: str,
    nombre_negocio: str,
    historial_chat: str
):
    try:
        # ==========================================================
        # 📅 FECHA ACTUAL
        # ==========================================================
        fecha_hoy = datetime.now().strftime(
            "%d de %B de %Y"
        )

        # ==========================================================
        # 🧠 PROMPT AUDITOR
        # ==========================================================
        prompt = f"""
Eres auditor financiero automatizado de '{nombre_negocio}'.

Analiza la imagen enviada y determina si es un comprobante
de pago válido relacionado con videojuegos.

HISTORIAL DEL CHAT:
{historial_chat}

REGLAS:
1. FECHA válida y reciente.
2. MONTO coherente con conversación.
3. CONCEPTO relacionado con videojuegos.
4. Rechazar pagos ambiguos o sospechosos.

Hoy es:
{fecha_hoy}

RESPONDE EXCLUSIVAMENTE JSON:

{{
    "es_pago": true,
    "monto_detectado": 0.0,
    "analisis": "texto"
}}
"""

        # ==========================================================
        # 🔑 API KEY LIMPIA
        # ==========================================================
        api_key_limpia = (
            GENAI_KEY.strip()
            if GENAI_KEY
            else ""
        )

        if not api_key_limpia:
            raise Exception("GENAI_KEY vacía")

        # ==========================================================
        # 🌐 REQUEST GEMINI
        # ==========================================================
        url = (
            "https://generativelanguage.googleapis.com/"
            "v1beta/models/gemini-2.5-flash:generateContent"
        )

        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": api_key_limpia
        }

        payload = {
            "contents": [{
                "parts": [
                    {"text": prompt},
                    {
                        "inline_data": {
                            "mime_type": mime_type,
                            "data": b64_img
                        }
                    }
                ]
            }],
            "generationConfig": {
                "temperature": 0.1
            }
        }

        res = await http_client.post(
            url,
            headers=headers,
            json=payload
        )

        # ==========================================================
        # ❌ ERROR HTTP
        # ==========================================================
        if res.status_code != 200:
            raise Exception(
                f"Gemini HTTP {res.status_code}"
            )

        # ==========================================================
        # 🧹 LIMPIEZA RESPUESTA
        # ==========================================================
        texto_sucio = (
            res.json()
            ['candidates'][0]
            ['content']['parts'][0]
            ['text']
        )

        simbolo = chr(96) * 3

        texto_limpio = (
            texto_sucio
            .replace(simbolo + "json", "")
            .replace(simbolo, "")
            .strip()
        )

        resultado = json.loads(texto_limpio)

        # ==========================================================
        # 🛡️ VALIDACIÓN FINAL
        # ==========================================================
        return {
            "es_pago": bool(
                resultado.get("es_pago", False)
            ),
            "monto_detectado": safe_float(
                resultado.get("monto_detectado", 0)
            ),
            "analisis": str(
                resultado.get(
                    "analisis",
                    "Sin análisis."
                )
            )
        }

    except Exception as e:
        logger.exception(
            f"❌ ERROR auditar_comprobante_ia: {str(e)}"
        )

        return {
            "es_pago": False,
            "monto_detectado": 0.0,
            "analisis": "Error interno del sistema IA."
        }

# ==========================================================
# 📦 MODELOS PYDANTIC PARA GESTIÓN (Con soporte de Teléfono)
# ==========================================================
class EstadoUpdate(BaseModel):
    nombre: str
    telefono: str = "" # Hacemos que lo acepte, venga o no
    nueva_columna: str

class ClienteIdentificador(BaseModel):
    nombre: str
    telefono: str = ""

# ==========================================================
# 🌐 RUTAS DE GESTIÓN CRM Y CHAT (Adiós Error 404)
# ==========================================================
@app.post("/api/actualizar_estado")
def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # Actualizamos usando el teléfono si lo tenemos, si no, caemos al nombre
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
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('vendedor_id', _sesion)
        
        if datos.telefono and datos.telefono != "Sin registrar":
            query = query.eq('telefono', datos.telefono)
        else:
            # Si solo mandan nombre, buscamos el teléfono en prospectos primero
            res_tel = supabase.table('prospectos').select('telefono').eq('nombre', datos.nombre).eq('vendedor_id', _sesion).limit(1).execute()
            if res_tel.data and res_tel.data[0].get('telefono'):
                query = query.eq('telefono', res_tel.data[0]['telefono'])
            else:
                return {"historial": [{"texto": "Sin historial previo o cliente sin teléfono.", "es_mio": False}]}
                
        res = query.order('created_at', desc=False).limit(50).execute()
        
        historial_formateado = []
        for fila in res.data:
            es_mio = (fila.get('autor', 'USER') != 'USER')
            historial_formateado.append({"texto": fila.get('mensaje', ''), "es_mio": es_mio})
            
        return {"historial": historial_formateado}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error cargando chat")

# ==========================================================
# 🎮 RUTA: CARGAR INVENTARIO B2B (Fantasy Games)
# ==========================================================
@app.get("/api/cargar_inventario")
def cargar_inventario(_sesion: str = Depends(verificar_sesion_b2b)):
    """
    Busca todos los artículos en la tabla 'inventario' 
    que pertenecen al vendedor logueado (ej. V-001).
    """
    try:
        # 🛡️ Realizamos la consulta a Supabase filtrando por el vendedor_id
        # IMPORTANTE: Asegúrate que tu tabla en Supabase se llame 'inventario'
        res = supabase.table('inventario').select("*").eq('vendedor_id', _sesion).execute()
        
        # Log en la consola de Render para rastrear la carga
        print(f"📦 [INVENTARIO] Sincronizando {len(res.data)} artículos para el ID: {_sesion}")
        
        return {
            "status": "ok", 
            "inventario": res.data
        }
        
    except Exception as e:
        print(f"❌ [ERROR INVENTARIO] Falló la carga: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail="Error interno al acceder a la tabla de inventario"
        )

# ==========================================================
# 📊 RUTAS DE GESTIÓN DE COLUMNAS B2B (CORREGIDAS 422)
# ==========================================================
class ColumnaAction(BaseModel):
    nombre_columna: str
    # 🛡️ Ya no pedimos el vendedor_id aquí, lo sacamos del Token

class RenombrarColumnaAction(BaseModel):
    viejo_nombre: str
    nuevo_nombre: str
    # 🛡️ Igual aquí

@app.post("/api/crear_columna")
def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').insert({
            'vendedor_id': _sesion, # Usamos el ID del Token B2B
            'nombre_columna': datos.nombre_columna
        }).execute()
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
# 🤖 MÓDULO DE WEBHOOK: CONEXIÓN AL MOTOR IA VELTRIX
# ==========================================================

# --- 🟢 1. VERIFICACIÓN DEL WEBHOOK (GET) ---
@app.get("/webhook")
def verificar_webhook(request: Request):
    """ Meta usa esta ruta para validar que tu servidor es real. """
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == WEBHOOK_SECRET:
        print("✅ [WEBHOOK] Servidor validado con éxito por Meta.")
        try:
            return int(challenge)
        except:
            return challenge
    
    print(f"❌ [WEBHOOK] Intento de validación fallido. Token recibido: {token}")
    raise HTTPException(status_code=403, detail="Token de verificación inválido")

# --- 📩 2. RECEPCIÓN DE MENSAJES Y TRASPASO A IA (POST) ---
@app.post("/webhook")
async def recibir_mensajes(request: Request, background_tasks: BackgroundTasks):
    """ Escucha los mensajes y los envía al Motor de IA en segundo plano """
    try:
        body = await request.json()
        
        # 🛡️ Filtro de seguridad: Ignorar notificaciones de "leído" o "entregado"
        if not body.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("messages"):
            return {"status": "ignored_update"}

        print("\n--- 📥 [NUEVO MENSAJE RECIBIDO] ---")
        
        # 🔍 Extracción limpia de datos
        value = body["entry"][0]["changes"][0]["value"]
        message = value.get("messages", [{}])[0]
        
        # Obtenemos el ID del teléfono al que el cliente le escribió
        phone_id_receptor = value.get("metadata", {}).get("phone_number_id", WHATSAPP_PHONE_ID)

        # 🚀 CONEXIÓN AAA: Mandamos todo el paquete a tu función de IA
        background_tasks.add_task(gestionar_mensaje_entrante_bg, value, message, phone_id_receptor)
        
        print("✅ [WEBHOOK] Mensaje transferido al Motor de IA Veltrix con éxito.")
        print("----------------------------------\n")

        return {"status": "ok"}

    except Exception as e:
        print(f"⚠️ [WEBHOOK ERROR] Fallo al procesar mensaje: {str(e)}")
        return {"status": "error", "reason": str(e)}

# ==========================================================
# 🏁 ANCLAJE FINAL Y ARRANQUE DEL SERVIDOR (MOTOR B2B)
# ==========================================================
app.include_router(router)

if __name__ == "__main__":
    logger.info("⚡ Iniciando Uvicorn Server en el puerto 10000...")
    # Usamos reload=False para producción y máximo performance
    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=False)
