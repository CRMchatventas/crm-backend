# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V10 (SECURE SAAS)
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
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, APIRouter
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
import uvicorn

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

GEMINI_TIMEOUT = 35.0
GEMINI_REINTENTOS = 3
GEMINI_TEMP = 0.2

# 🔥 Protección anti-abuso
MAX_REQUESTS_POR_MINUTO_TENANT = 40
MAX_REQUESTS_POR_MINUTO_TELEFONO = 12
MAX_REQUESTS_GLOBAL_MINUTO = 250

# --- 🔑 CREDENCIALES BASE ---
GENAI_KEY = os.getenv("GENAI_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY")
WEBHOOK_SECRET = os.getenv("META_WEBHOOK_SECRET") 
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "524491142598")
JWT_SECRET = os.getenv("JWT_SECRET", "clave_secreta_provisional_veltrix") 
ALGORITHM = "HS256"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ ERROR CRÍTICO: Faltan credenciales de Supabase en el archivo .env")

# Conexión a Nube B2B
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================================================
# 🧠 MEMORIA RAM OPERATIVA (ESTADO GLOBAL & SINGLETONS)
# ==========================================================
registro_actividad_b2b = {} 
historial_hashes_b2b = {}   
procesados_recientemente = deque(maxlen=1000) # Memoria circular anti-colapso RAM
cache_respuestas_ia = {}
locks_por_tenant = {}

# Rate limiters
rate_limit_tenant = defaultdict(list)
rate_limit_phone = defaultdict(list)
rate_limit_global = []

# ⚡ HTTPX Singleton (Evita colapso de sockets - Auditoría F)
http_client: httpx.AsyncClient = None

# ==========================================
# 🔥 SWITCH DE ENCENDIDO (MOTOR 24H & SINGLETONS) 🔥
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    # Inicializamos el cliente HTTP global una sola vez
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(GEMINI_TIMEOUT), limits=limits)
    
    logger.info("🚀 [SISTEMA] Motor Central Veltrix Iniciado...")
    asyncio.create_task(bucle_seguimiento_24h())
    yield
    # Limpieza al apagar
    await http_client.aclose()
    logger.info("🛑 [SISTEMA] Motor Central Apagado.")

# ✨ INICIALIZACIÓN
app = FastAPI(title="Motor Central CRM B2B - Veltrix Engine", lifespan=lifespan)
router = APIRouter()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# --- 📦 MODELOS DE DATOS (PYDANTIC BLINDADOS) ---
# ==========================================
class Credenciales(BaseModel):
    email: str
    password: str

class ProspectoUpdate(BaseModel): 
    nombre: str
    nueva_columna: str
    vendedor_id: str = Field(..., description="ID del tenant obligatorio")
    
class NotaUpdate(BaseModel): 
    nombre: str
    notas: str
    etiquetas: str
    vendedor_id: str = Field(..., description="ID del tenant obligatorio")
    
class MensajeSaliente(BaseModel): 
    cliente: str
    texto: str
    vendedor_id: str = Field(..., description="ID del tenant obligatorio")

class InventarioItem(BaseModel):
    nombre: str
    consola: str
    precio: float
    costo: float = 0.0                
    stock: int = 1                    
    codigo_barras: str = ""           
    url_portada: str = ""             
    estado_general: str = "Bueno"      
    rareza: str = "" 
    vendedor_id: str = Field(..., description="ID del tenant obligatorio")             
    tiene_caja: bool = False
    tiene_manual: bool = False
    es_portada_original: bool = False
    descripcion_detallada: str = ""

class VentaItem(BaseModel):
    nombre: str
    consola: str
    estado_general: str = ""
    nuevo_stock: int
    vendedor_id: str = Field(..., description="ID del tenant obligatorio")

class BotConfig(BaseModel):
    vendedor_id: str
    link_pago: str
    texto_entrega: str
    admin_phone: str
    bot_activo: bool

# ==========================================
# 🔐 SISTEMA DE AUTENTICACIÓN B2B Y FIRMAS
# ==========================================
def crear_token_jwt(vendedor_id: str, email: str):
    expiracion = datetime.utcnow() + timedelta(days=1)
    payload = {"sub": vendedor_id, "email": email, "exp": expiracion}
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)

async def verificar_sesion_b2b(authorization: str = Header(None), auth_token: str = Header(None)):
    token = None
    if authorization and authorization.startswith("Bearer "):
        token = authorization.split(" ")[1]
    elif auth_token:
        token = auth_token
        
    if not token:
        raise HTTPException(status_code=401, detail="Acceso denegado: Credenciales faltantes")
        
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        vendedor_id_real = payload.get("sub") 
        if not vendedor_id_real:
            raise HTTPException(status_code=401, detail="Token corrupto: Identidad no encontrada")
        return vendedor_id_real 
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Sesión expirada.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido.")

async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta:
        raise HTTPException(status_code=400, detail="Falta la firma de Meta")

    cuerpo_bytes = await request.body()
    firma_calculada = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), cuerpo_bytes, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(firma_meta, firma_calculada):
        logger.warning("🚨 Intento de inyección de webhook Meta.")
        raise HTTPException(status_code=403, detail="Firma inválida")
    return True

# ==========================================================
# 🧰 HELPERS AAA (LIMPIEZA, CACHÉ, RATE LIMIT)
# ==========================================================
def now_ts() -> float:
    return time.time()

def limpiar_texto(texto: str) -> str:
    if not texto: return ""
    texto = str(texto).strip().replace("\x00", "")
    texto = re.sub(r"\s+", " ", texto) # Remueve saltos y espacios extra
    texto = texto.replace("{", "").replace("}", "") # Sanitización anti prompt-injection
    return texto[:MAX_MENSAJE_LEN]

def generar_hash_cache(*args) -> str:
    bruto = "|".join([str(a) for a in args])
    return hashlib.sha256(bruto.encode()).hexdigest()

def limpiar_json_gemini(texto: str) -> Optional[dict]:
    try:
        simbolo = chr(96) * 3
        texto = texto.replace(simbolo + "json", "").replace(simbolo, "").strip()
        inicio = texto.find("{")
        final = texto.rfind("}")
        if inicio == -1 or final == -1: return None
        return json.loads(texto[inicio:final + 1])
    except Exception:
        return None

def validar_respuesta_ia(data: dict) -> dict:
    if not isinstance(data, dict): raise Exception("IA devolvió formato inválido")
    intencion = str(data.get("intencion", "COTIZACION")).upper()
    if intencion not in ["COMPRA", "COTIZACION", "HUMANO"]:
        intencion = "HUMANO"
    respuesta = limpiar_texto(data.get("respuesta", ""))
    juego = limpiar_texto(data.get("juego_detectado", ""))
    if not respuesta: respuesta = "Hola. Estoy revisando la información."
    return {"intencion": intencion, "respuesta": respuesta, "juego_detectado": juego}

def limpiar_rate_limit(lista: list, ventana_segundos: int):
    ahora = now_ts()
    while lista and (ahora - lista[0]) > ventana_segundos:
        lista.pop(0)

def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    ahora = now_ts()
    limpiar_rate_limit(rate_limit_global, 60)
    if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO: return False

    limpiar_rate_limit(rate_limit_tenant[vendedor_id], 60)
    if len(rate_limit_tenant[vendedor_id]) >= MAX_REQUESTS_POR_MINUTO_TENANT: return False

    limpiar_rate_limit(rate_limit_phone[telefono], 60)
    if len(rate_limit_phone[telefono]) >= MAX_REQUESTS_POR_MINUTO_TELEFONO: return False

    rate_limit_global.append(ahora)
    rate_limit_tenant[vendedor_id].append(ahora)
    rate_limit_phone[telefono].append(ahora)
    return True

# ==========================================
# 💵 MOTOR DE PRECIOS & SCRAPER ASYNC (Auditoría L)
# ==========================================
async def obtener_dolar_hoy_async():
    try:
        res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD")
        return float(res.json().get("rates", {}).get("MXN", 18.00))
    except Exception: return 18.00

async def obtener_html_escalonado_async(url_objetivo: str) -> str:
    estrategias = [
        ("🟢 Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("🟡 Media", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true")
    ]
    headers_humanos = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    for _, url_scraper in estrategias:
        try:
            res = await http_client.get(url_scraper)
            if res.status_code == 200 and "scraperapi" not in res.text.lower():
                if "pricecharting" in res.text.lower() or "price" in res.text.lower():
                    return res.text
        except Exception: pass
    try:
        res = await http_client.get(url_objetivo, headers=headers_humanos)
        if res.status_code == 200: return res.text
    except Exception: pass
    return ""

def calcular_rareza_ia(nombre: str, consola: str, precio: float) -> str:
    nombre = nombre.upper()
    consolas_modernas = ["PS5", "PS4", "NINTENDO SWITCH", "XBOX ONE", "XBOX SERIES X"]
    if any(x in nombre for x in ["FIFA", "MADDEN", "NBA", "NCAA", "PES", "SINGSTAR", "EA FC"]): return "Común"
    if any(x in nombre for x in ["SILENT HILL", "KUON", "RULE OF ROSE", "OBSCURE", "HAUNTING GROUND", "PRAGMATA"]): return "Élite"
    if any(x in nombre for x in ["MARIO", "ZELDA", "METROID", "POKEMON", "HALO", "GTA"]): return "Demandado"
    if consola.upper() in consolas_modernas:
        if precio >= 3500: return "Élite"
        if precio >= 1000: return "Demandado"
        return "Común"
    else:
        if precio >= 1500: return "Élite"
        if precio >= 800:  return "Joya"
        if precio >= 400:  return "Demandado"
        return "Común"

def calcular_precio_venta_inteligente(precio_mercado_mxn: float, costo_compra: float = 0.0):
    piso_absoluto = 250.0
    precio_con_margen = precio_mercado_mxn + 150.0 if precio_mercado_mxn > 0 else 0.0
    precio_seguridad = costo_compra + 100.0 if costo_compra > 0 else 0.0
    precio_bruto = max(piso_absoluto, precio_con_margen, precio_seguridad)
    return float(round(precio_bruto / 10) * 10)

# ==========================================
# 📥 MOTOR MULTIMEDIA & WHATSAPP ASYNC (Auditoría L)
# ==========================================
async def descargar_y_subir_multimedia_async(media_id: str, mime_type: str, extension_default: str, token_vendedor: str):
    url_info = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {token_vendedor}"}
    try:
        res_info = await http_client.get(url_info, headers=headers)
        if res_info.status_code == 200:
            media_url = res_info.json().get("url")
            res_media = await http_client.get(media_url, headers=headers)
            if res_media.status_code == 200:
                file_bytes = res_media.content
                timestamp = int(now_ts())
                ext = mimetypes.guess_extension(mime_type) or extension_default
                file_path = f"archivo_{timestamp}{ext}"
                # Storage upload (sincrónico de Supabase, lo mantenemos)
                supabase.storage.from_("multimedia").upload(file_path, file_bytes, {"content-type": mime_type})
                return supabase.storage.from_("multimedia").get_public_url(file_path)
    except Exception as e:
        logger.exception("❌ Error Nube B2B Multimedia")
    return None

async def disparar_whatsapp_dinamico_async(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        await http_client.post(url, headers=headers, json=payload)
    except Exception as e: 
        logger.exception("⚠️ Error disparando WhatsApp Text")

async def disparar_whatsapp_imagen_async(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "image", "image": {"link": url_imagen, "caption": texto_mensaje}}
    try: 
        await http_client.post(url, headers=headers, json=payload)
    except Exception as e: 
        logger.exception("⚠️ Error disparando WhatsApp Imagen")

async def cazar_portada_y_guardar_background(juego_id_supabase: str, nombre_juego: str, consola: str):
    logger.info(f"🖼️ [PORTADAS] Buscando en background para: {nombre_juego}")
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
# 🧠 CLIENTE GEMINI CENTRALIZADO (NÚCLEO IA SINGLETON)
# ==========================================================
async def consultar_gemini_json(prompt: str, temperature: float = 0.2):
    api_key_limpia = GENAI_KEY.strip() if GENAI_KEY else ""
    if not api_key_limpia: raise Exception("GENAI_KEY VACÍA")

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key_limpia}
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature, "topP": 0.8, "topK": 20, "maxOutputTokens": 400}
    }

    # Usamos el singleton global HTTPX en lugar de crear uno nuevo (Auditoría F)
    for intento in range(GEMINI_REINTENTOS):
        try:
            res = await http_client.post(url, headers=headers, json=payload)
            if res.status_code == 200:
                data = res.json()
                texto = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                json_limpio = limpiar_json_gemini(texto)
                if json_limpio: return json_limpio
                raise Exception("Gemini devolvió JSON inválido")

            elif res.status_code in [429, 500, 502, 503, 504]:
                espera = (2 ** intento)
                logger.warning(f"⚠️ Gemini ocupado ({res.status_code}) | Espera: {espera}s")
                await asyncio.sleep(espera)
                continue
            else:
                raise Exception(f"Gemini Error {res.status_code}: {res.text[:300]}")

        except asyncio.TimeoutError:
            logger.warning("⚠️ Timeout Gemini")
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.exception(f"❌ Gemini Exception en intento {intento}")
            if intento >= GEMINI_REINTENTOS - 1: raise
            await asyncio.sleep(1.5)
            
    raise Exception("Gemini agotó reintentos")

# ==========================================================
# 🤖 ANALIZAR INTENCIÓN IA (CERRADOR MAESTRO V-5.8)
# ==========================================================
async def analizar_intencion_venta_ia(
    texto_cliente: str,
    inventario_contexto: str,
    historial_chat: str,
    config: dict
):
    try:
        vendedor_id = config.get("vendedor_id", "V-001")
        nombre_negocio = config.get("nombre_negocio", "Fantasy Games")

        texto_cliente = limpiar_texto(texto_cliente)
        historial_chat = limpiar_texto(historial_chat)

        # ⚡ CACHE ANTI-GASTO TOKENS
        cache_key = generar_hash_cache(vendedor_id, texto_cliente, historial_chat[-200:])
        cache_item = cache_respuestas_ia.get(cache_key)

        if cache_item and (now_ts() - cache_item["ts"]) < 90:
            logger.info("⚡ CACHE HIT IA")
            return cache_item["data"]

        # 🔒 LOCK POR TENANT
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
2. PRECIO_SUGERIDO: Es el valor real de mercado. Úsalo para dar valor ("Su valor real en el mercado es de $... pero hoy lo tengo en $...").
3. PRECIO_MINIMO: Es tu LÍMITE SECRETO. NUNCA lo menciones, pero úsalo para regatear. Tienes permiso de bajar el precio hasta este número para cerrar la venta HOY.

1. LINK DE PAGO: 
SIEMPRE que confirmes disponibilidad y precio, incluye el link oficial de cobro de esta empresa: 
"💳 Paga seguro aquí para apartarlo de inmediato: {link_pago}"

2. UPSELL Y DESCUENTOS (REGLA DE 3):
- Si el cliente está cotizando 1 o 2 artículos, recomiéndale activamente una "Joya" o "Elite" del inventario.
- "💡 Por cierto, si te llevas 3 artículos, te hago $100 MXN de descuento directo en tu total. ¡Anímate a llevarte esa joya que te falta!"
- Si el cliente ya eligió 3 artículos, aplícale automáticamente el descuento.
- NUNCA des un descuento que haga que el total baje de la suma de los PRECIOS_MINIMOS.

3. LOGÍSTICA Y ENTREGAS:
Si el cliente te pregunta "¿dónde entregan?", "¿hacen envíos?" o cualquier duda de entrega, RESPÓNDELE TÚ usando estrictamente esta información: {texto_entrega}

4. DETALLE DEL PRODUCTO:
Menciona características y estado (Completo, Sin Librito, Solo Disco). Si el cliente pide fotos, asume que tú (el bot) se las vas a enviar en este mismo mensaje.

5. CROSS-SELLING B2B (VELTRIX ENGINE - Meta: 5 licencias):
OJO: Solo si el cliente ya confirmó una compra, o si se asombra por tu velocidad de atención, ofrécele sutilmente rentar tu "cerebro" (el CRM Veltrix Engine) por $990 MXN al mes. 

REGLAS DE CLASIFICACIÓN ('intencion') - ¡SÍGUELAS AL PIE DE LA LETRA!:
- "COTIZACION": (Usa esta el 95% de las veces). Si el cliente pregunta "¿dónde entregan?", "¿cuánto cuesta?", pide fotos, o dice que apenas va a pagar. ¡Tú respondes!
- "COMPRA": ¡SOLO USAR si el cliente dice EXPLÍCITAMENTE "ya pagué", "ya transferí", "te mandé el ticket"!
- "HUMANO": Dudas que no sepas responder, quejas, o si pide explícitamente hablar con un humano.

INVENTARIO DISPONIBLE Y DETALLADO: 
{inventario_contexto}

HISTORIAL DEL CHAT:
{historial_chat}

MENSAJE CLIENTE: 
"{texto_cliente}"

Responde EXCLUSIVAMENTE en JSON válido:
{{
  "intencion": "COMPRA", "HUMANO", o "COTIZACION",
  "respuesta": "Tu respuesta persuasiva aplicando todas las reglas, buscando siempre aumentar el ticket",
  "juego_detectado": "Nombre del producto exacto (Omitir si es charla general)"
}}
"""
            data = await consultar_gemini_json(prompt, temperature=GEMINI_TEMP)
            data = validar_respuesta_ia(data)

            # 💾 GUARDAR CACHE
            cache_respuestas_ia[cache_key] = {"ts": now_ts(), "data": data}
            return data

    except Exception as e:
        logger.exception("❌ ERROR analizar_intencion_venta_ia")
        return {
            "intencion": "HUMANO",
            "respuesta": "Estoy revisando la información. Un asesor continuará contigo enseguida. 🎮",
            "juego_detectado": ""
        }

# ==========================================================
# 🚨 RESUMEN HANDOFF IA
# ==========================================================
async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        motivo = "quiere cerrar compra" if intencion == "COMPRA" else "requiere ayuda humana"
        prompt = f"""
Cliente: {cliente}
Motivo: {motivo}

Historial:
{historial_str}

Genera resumen ejecutivo en 3 viñetas.
JSON:
{{
  "resumen":"texto"
}}
"""
        data = await consultar_gemini_json(prompt, temperature=0.1)
        return data.get("resumen", "⚠️ Cliente requiere atención humana")
    except Exception as e:
        logger.exception("❌ ERROR HANDOFF")
        return "⚠️ Cliente requiere atención humana"

# ==========================================================
# 💰 OFERTA INTELIGENTE
# ==========================================================
async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"""
Cliente: {cliente}
Juego: {juego_detectado}

Inventario:
{inventario_contexto}

Genera remarketing corto.
JSON:
{{
  "nuevo_precio_ofrecido":"0",
  "mensaje_oferta":"texto"
}}
"""
        data = await consultar_gemini_json(prompt, temperature=0.3)
        if not data: return None

        return {
            "nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")),
            "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))
        }
    except Exception as e:
        logger.exception("❌ ERROR OFERTA IA")
        return None
        
# ==========================================================
# 🚨 SERVICIO DE ALERTAS (CAPA DE NOTIFICACIÓN)
# ==========================================================
async def enviar_alerta_whatsapp_admin(
    cliente: str,
    telefono_cliente: str,
    intencion: str,
    resumen_ia: str,
    config: dict
):
    try:
        telefono_admin = config.get("admin_phone")
        if not telefono_admin:
            telefono_admin = ADMIN_PHONE_GLOBAL

        token = config.get("meta_token", "")
        phone_id = config.get("meta_phone_id", "")

        encabezado = "🚨 *ASISTENCIA REQUERIDA*" if intencion == "HUMANO" else "💰 *NUEVA VENTA DETECTADA*"

        mensaje = (
            f"{encabezado}\n\n"
            f"👤 Cliente: {cliente}\n"
            f"📱 Teléfono: {telefono_cliente}\n\n"
            f"🧠 Análisis IA:\n{resumen_ia}"
        )

        # Usamos el singleton asíncrono para no bloquear
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje, token, phone_id)

    except Exception as e:
        logger.exception("❌ ERROR CRÍTICO ALERTA ADMIN")


# ==========================================================
# ⏱️ WORKER BACKGROUND: RELOJ 24H (CARRITOS ABANDONADOS)
# ==========================================================
async def bucle_seguimiento_24h():
    while True:
        try:
            logger.info("🕒 Escaneando prospectos abandonados (Multi-Tenant)...")
            hace_24h = (datetime.now() - timedelta(hours=24)).isoformat()

            res = (
                supabase
                .table('prospectos')
                .select('*')
                .eq('columna', 'Envios Masivos')
                .lt('ultima_interaccion_ia', hace_24h)
                .limit(100) # Paginación preventiva
                .execute()
            )

            prospectos = res.data or []

            if not prospectos:
                await asyncio.sleep(3600)
                continue

            # Agrupación por tenant para optimizar consultas de inventario
            agrupados = defaultdict(list)
            for p in prospectos:
                agrupados[p.get('vendedor_id', 'V-001')].append(p)

            for vendedor_id, items in agrupados.items():
                
                # 🔒 Configuración Aislada
                res_conf = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).limit(1).execute()
                if not res_conf.data:
                    continue
                config = res_conf.data[0]

                # 📦 Contexto de Inventario
                res_inv = supabase.table('inventario').select('nombre, precio, precio_minimo, stock').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(MAX_CONTEXTO_INV).execute()
                contexto_inv = str(res_inv.data or [])

                for p in items:
                    telefono_cliente = p.get('telefono', '')
                    cliente_nombre = p.get('nombre', 'Cliente')
                    
                    oferta = await generar_oferta_inteligente(
                        cliente_nombre,
                        p.get('ultimo_juego_interes', 'videojuego'),
                        contexto_inv
                    )

                    if not oferta or not oferta.get("mensaje_oferta"):
                        continue

                    mensaje_remarketing = oferta.get("mensaje_oferta", "")

                    # 📤 Envío asíncrono
                    await disparar_whatsapp_dinamico_async(
                        telefono_cliente,
                        mensaje_remarketing,
                        config.get('meta_token', ''),
                        config.get('meta_phone_id', '')
                    )

                    # 💾 Actualización de Estado (CRM)
                    supabase.table('prospectos').update({
                        'columna': 'Con Descuento',
                        'estado_iluminacion': 'oro',
                        'ultima_interaccion_ia': datetime.now().isoformat()
                    }).eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_id).execute()
                    
                    # 💬 Inserción en Tabla de Chat (Arquitectura Separada)
                    supabase.table('mensajes_chat').insert({
                        'telefono': telefono_cliente,
                        'vendedor_id': vendedor_id,
                        'autor': 'BOT_REMARKETING',
                        'mensaje': mensaje_remarketing
                    }).execute()

                    await asyncio.sleep(2) # Respiro anti-spam Meta

        except Exception as e:
            logger.exception("❌ ERROR FATAL EN RELOJ 24H")

        await asyncio.sleep(3600)


# ==========================================================
# 🗄️ CAPA DE REPOSITORIO Y SERVICIOS B2B
# ==========================================================
async def obtener_contexto_inventario(vendedor_id: str) -> str:
    """Extrae el catálogo activo del tenant con límites seguros."""
    res_inv = supabase.table('inventario').select('nombre, precio, precio_sugerido, precio_minimo, stock, estado_general').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(MAX_CONTEXTO_INV).execute()
    return str(res_inv.data or [])

async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    """Extrae el historial limpio desde la tabla mensajes_chat."""
    res_hist = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', vendedor_id).order('created_at', desc=True).limit(MAX_HISTORIAL).execute()
    if not res_hist.data:
        return "Primer mensaje."
    
    mensajes = reversed(res_hist.data) # Cronológico
    return "\n".join([f"{m.get('autor', 'USER')}: {m.get('mensaje', '')}" for m in mensajes])

async def actualizar_estado_crm(telefono: str, vendedor_id: str, columna: str, iluminacion: str, juego: str):
    """Actualiza estrictamente los metadatos del CRM."""
    supabase.table('prospectos').update({
        'columna': columna,
        'estado_iluminacion': iluminacion,
        'ultimo_juego_interes': juego,
        'ultima_interaccion_ia': datetime.now().isoformat()
    }).eq('telefono', telefono).eq('vendedor_id', vendedor_id).execute()

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str):
    """Guarda un registro inmutable en el historial."""
    supabase.table('mensajes_chat').insert({
        'telefono': telefono,
        'vendedor_id': vendedor_id,
        'autor': autor,
        'mensaje': mensaje
    }).execute()


# ==========================================================
# 🤖 MOTOR PRINCIPAL DE NEGOCIO (IA & WORKFLOW)
# ==========================================================
async def procesar_respuesta_bot(
    cliente: str,
    telefono: str,
    texto_entrante: str,
    columna_actual: str,
    config: dict
):
    try:
        vendedor_id = config.get("vendedor_id", "")

        # 🛡️ RATE LIMIT
        if not verificar_rate_limit(vendedor_id, telefono):
            logger.warning("⚠️ Mensaje bloqueado por rate limit.")
            return

        token = config.get("meta_token", "")
        phone_id = config.get("meta_phone_id", "")

        # 📦 PREPARACIÓN DE CONTEXTO
        contexto = await obtener_contexto_inventario(vendedor_id)
        historial = await obtener_historial_chat(telefono, vendedor_id)

        # 🧠 DECISIÓN IA
        decision = await analizar_intencion_venta_ia(
            texto_entrante,
            contexto,
            historial,
            config
        )

        nueva_columna = columna_actual
        iluminacion = "blanco"

        # 🚦 RUTEO DE ESTADOS
        if decision["intencion"] == "HUMANO":
            nueva_columna = "Requiere Asistencia"
            iluminacion = "verde_alerta"
            resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)

        elif decision["intencion"] == "COMPRA":
            nueva_columna = "Por Entregar"
            iluminacion = "verde_exito"
            resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)

        elif decision["intencion"] == "COTIZACION":
            if columna_actual == "Bandeja Nueva":
                nueva_columna = "Envios Masivos"

        respuesta_final = decision["respuesta"]

        # 💾 ACTUALIZACIÓN DE ESTADOS AISLADOS
        await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, decision.get('juego_detectado', ''))
        await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

        # 🖼️ BÚSQUEDA MULTIMEDIA
        juego = decision.get('juego_detectado', '')
        url_imagen = None

        if juego:
            res_img = supabase.table('inventario').select('url_portada').ilike('nombre', f'%{juego}%').eq('vendedor_id', vendedor_id).neq('url_portada', '').limit(1).execute()
            if res_img.data:
                url_imagen = res_img.data[0].get('url_portada')

        # 📤 ENVÍO META API
        if url_imagen:
            await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, token, phone_id)
        else:
            await disparar_whatsapp_dinamico_async(telefono, respuesta_final, token, phone_id)

    except Exception as e:
        logger.exception("❌ ERROR FATAL en procesar_respuesta_bot")


# ==========================================================
# ⚙️ BACKGROUND WORKER DE ENTRADA
# ==========================================================
async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    try:
        telefono_cliente = msg.get("from", "")
        
        # Extracción segura de payload Meta
        texto_entrante = ""
        if "text" in msg:
            texto_entrante = msg["text"]["body"]
        elif "image" in msg:
            texto_entrante = "[IMAGEN RECIBIDA]"
        elif "interactive" in msg:
            texto_entrante = msg["interactive"].get("button_reply", {}).get("title", "")
            
        if not texto_entrante or not telefono_cliente:
            return
            
        # 🔑 Identificar Tenant
        res_conf = supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1).execute()
        if not res_conf.data:
            return
            
        config = res_conf.data[0]
        vendedor_id = config.get("vendedor_id")
        
        if not config.get("bot_activo", True):
            return
            
        # 💾 Registrar mensaje de usuario
        await guardar_mensaje_chat(telefono_cliente, vendedor_id, "USER", texto_entrante)
        
        # 🔍 Validar existencia en CRM
        res_prospecto = supabase.table('prospectos').select('nombre', 'columna').eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_id).limit(1).execute()
        
        cliente_nombre = "Cliente"
        columna_actual = "Bandeja Nueva"
        
        if res_prospecto.data:
            cliente_nombre = res_prospecto.data[0].get("nombre", "Cliente")
            columna_actual = res_prospecto.data[0].get("columna", "Bandeja Nueva")
        else:
            nombre_perfil = valor.get("contacts", [{}])[0].get("profile", {}).get("name", "Nuevo Contacto")
            supabase.table('prospectos').insert({
                "telefono": telefono_cliente,
                "nombre": nombre_perfil,
                "vendedor_id": vendedor_id,
                "columna": "Bandeja Nueva",
                "estado_iluminacion": "blanco"
            }).execute()
            cliente_nombre = nombre_perfil
            
        # 🚀 Despachar a IA
        await procesar_respuesta_bot(cliente_nombre, telefono_cliente, texto_entrante, columna_actual, config)

    except Exception as e:
        logger.exception("❌ ERROR en gestionar_mensaje_entrante_bg")


# ==========================================================
# 📦 MODELOS PYDANTIC PARA ENDPOINTS DE GESTIÓN (Auditoría K)
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
async def descargar_imagen_whatsapp_b64(media_id: str, token_vendedor: str):
    url_info = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {token_vendedor}"}
    try:
        res_info = await http_client.get(url_info, headers=headers)
        if res_info.status_code == 200:
            media_url = res_info.json().get("url")
            res_media = await http_client.get(media_url, headers=headers)
            if res_media.status_code == 200:
                return base64.b64encode(res_media.content).decode("utf-8"), res_media.headers.get("content-type", "image/jpeg")
    except Exception as e:
        logger.exception("❌ Error descargando imagen de WhatsApp B64")
    return None, None

async def auditar_comprobante_ia(b64_img: str, mime_type: str, nombre_negocio: str, historial_chat: str):
    fecha_hoy = datetime.now().strftime("%d de %B de %Y")
    prompt = f"""
    Eres Auditor de '{nombre_negocio}'. Analiza esta imagen. ¿Es un pago válido?
    HISTORIAL: {historial_chat}
    REGLAS:
    1. FECHA: Hoy es {fecha_hoy}.
    2. MONTO: Debe coincidir con la plática.
    3. CONCEPTO: Debe decir videojuego o estar vacío. Rechaza "renta", "vidrios", etc.
    JSON: {{"es_pago": true/false, "monto_detectado": 0.0, "analisis": "Razón"}}
    """
    api_key_limpia = GENAI_KEY.strip() if GENAI_KEY else ""
    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key_limpia}
    payload = {
        "contents": [{"parts": [{"text": prompt}, {"inline_data": {"mime_type": mime_type, "data": b64_img}}]}],
        "generationConfig": {"temperature": 0.1}
    }
    
    try:
        res = await http_client.post(url, headers=headers, json=payload)
        if res.status_code == 200:
            texto_sucio = res.json()['candidates'][0]['content']['parts'][0]['text']
            simbolo = chr(96) * 3
            return json.loads(texto_sucio.replace(simbolo + "json", "").replace(simbolo, "").strip())
        raise Exception(f"Fallo IA Visión: {res.status_code}")
    except Exception as e:
        logger.exception("❌ ERROR en auditar_comprobante_ia")
        return {"es_pago": False, "monto_detectado": 0.0, "analisis": "Error interno del sistema de visión."}

# ==========================================================
# ⚙️ BACKGROUND WORKER DE ENTRADA (TEXTO + VISIÓN)
# ==========================================================
async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    try:
        res_config = supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1).execute()
        if not res_config.data: return
            
        config_vendedor = res_config.data[0]
        vendedor_actual = config_vendedor["vendedor_id"]
        token_actual = config_vendedor["meta_token"]
        nombre_negocio = config_vendedor.get("nombre_negocio", "Fantasy Games")

        if not config_vendedor.get("bot_activo", True): return

        contact = valor.get("contacts", [{}])[0]
        nombre = contact.get("profile", {}).get("name", "Cliente")
        tel = msg.get("from", "")
        if tel.startswith("521"): tel = "52" + tel[3:]
        
        tipo = msg.get("type", "text")
        if tipo == "text": texto = msg["text"]["body"]
        elif tipo == "image": texto = "📷 [IMAGEN RECIBIDA: Posible comprobante de pago]"
        else: texto = f"[{tipo.upper()}] recibida."

        # 🔍 Validar existencia en CRM
        res_ex = supabase.table('prospectos').select('columna').eq('telefono', tel).eq('vendedor_id', vendedor_actual).limit(1).execute()
        col_destino = res_ex.data[0]['columna'] if res_ex.data else "Bandeja Nueva"

        if not res_ex.data:
            supabase.table('prospectos').insert({
                "nombre": nombre, "telefono": tel, "origen": "WHATSAPP", 
                "columna": col_destino, "vendedor_id": vendedor_actual, "estado_iluminacion": "blanco"
            }).execute()

        # 💾 Registrar mensaje de usuario en Historial Chat
        await guardar_mensaje_chat(tel, vendedor_actual, "USER", texto)
        
        # 🚦 RUTEO DE PROCESAMIENTO
        if tipo == "text" and col_destino != "En Conversacion":
            await procesar_respuesta_bot(nombre, tel, texto, col_destino, config_vendedor)
            
        elif tipo == "image":
            image_id = msg["image"]["id"]
            historial_para_auditor = await obtener_historial_chat(tel, vendedor_actual)
            
            b64_img, mime_type = await descargar_imagen_whatsapp_b64(image_id, token_actual)
            if b64_img:
                auditoria = await auditar_comprobante_ia(b64_img, mime_type, nombre_negocio, historial_para_auditor)
                
                if auditoria.get("es_pago") == True:
                    monto = auditoria.get('monto_detectado', 0)
                    await actualizar_estado_crm(tel, vendedor_actual, "Por Entregar", "verde_exito", "")
                    msg_exito = f"✅ ¡Pago validado por ${monto}! Hemos recibido tu comprobante."
                    await disparar_whatsapp_dinamico_async(tel, msg_exito, token_actual, phone_id_receptor)
                    await guardar_mensaje_chat(tel, vendedor_actual, "BOT", msg_exito)
                else:
                    razon = auditoria.get('analisis', 'No se reconoce como comprobante.')
                    msg_fallo = f"Hmm, mi sistema no validó esa imagen. 🤖\nDetalle: {razon}\n¿Podrías enviarme una foto clara del ticket o comprobante?"
                    await actualizar_estado_crm(tel, vendedor_actual, "Requiere Asistencia", "verde_alerta", "")
                    await disparar_whatsapp_dinamico_async(tel, msg_fallo, token_actual, phone_id_receptor)
                    await guardar_mensaje_chat(tel, vendedor_actual, "BOT", msg_fallo)
                    
    except Exception as e: 
        logger.exception("❌ [BACKGROUND TASK ERROR]")

# ==========================================
# 🟢 ENVIAR MENSAJES DESDE GODOT
# ==========================================
@app.post("/api/enviar_mensaje")
async def api_enviar_mensaje(datos: MensajeSaliente, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).limit(1).execute()
        if not res_config.data: return {"status": "error", "detalle": "Configuración de bot no encontrada."}
            
        config = res_config.data[0]
        # Nota: Asumimos que datos.cliente ahora contiene el TELEFONO para cumplir con Auditoría B
        telefono_destino = datos.cliente 
        
        await guardar_mensaje_chat(telefono_destino, _sesion, "TÚ (ADMIN)", datos.texto)
        await actualizar_estado_crm(telefono_destino, _sesion, "En Conversacion", "blanco", "")
        
        await disparar_whatsapp_dinamico_async(telefono_destino, datos.texto, config['meta_token'], config['meta_phone_id'])
        return {"status": "ok"}
    except Exception as e:
        logger.exception("❌ Error api_enviar_mensaje")
        raise HTTPException(status_code=500, detail="Error interno enviando mensaje")

# ==========================================
# 🌐 RUTAS DE GESTIÓN CRM (BLINDADAS AAA)
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        columnas_izq = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
        columnas_der = ["Por Entregar", "Vendidos", "Papelera"]
        res_cols = supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', _sesion).execute()
        
        columnas_custom = [r['nombre_columna'] for r in res_cols.data if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)] and r['nombre_columna'].upper() != "EN ATENCION"]
        if not columnas_custom: columnas_custom = ["+"]
                
        columnas_finales = columnas_izq + columnas_custom + columnas_der
        # Limite preventivo de RAM (Auditoría I)
        res_prospectos = supabase.table('prospectos').select('*').eq('vendedor_id', _sesion).order('ultima_interaccion_ia', desc=True).limit(500).execute()
        
        return {"columnas": columnas_finales, "prospectos": res_prospectos.data}
    except Exception as e:
        logger.exception("❌ Error cargando CRM")
        raise HTTPException(status_code=500, detail="Error conectando a Nube B2B")

@app.post("/api/crear_columna")
def crear_columna(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').insert({'nombre_columna': datos.nombre_columna, 'vendedor_id': _sesion}).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error crear_columna")
        raise HTTPException(status_code=500)

@app.post("/api/borrar_columna")
def borrar_columna(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').delete().eq('nombre_columna', datos.nombre_columna).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error borrar_columna")
        raise HTTPException(status_code=500)

@app.post("/api/renombrar_columna")
def renombrar_columna(datos: RenombrarColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').update({'nombre_columna': datos.nuevo_nombre}).eq('nombre_columna', datos.viejo_nombre).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error renombrar_columna")
        raise HTTPException(status_code=500)

@app.post("/api/historial_chat")
def historial_chat(datos: TelefonoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', datos.telefono).eq('vendedor_id', _sesion).order('created_at', desc=False).limit(50).execute()
        historial = []
        for fila in res.data:
            autor = fila.get('autor', 'USER')
            es_mio = autor != 'USER'
            historial.append({"texto": fila.get('mensaje', ''), "es_mio": es_mio})
        return {"historial": historial}
    except Exception as e:
        logger.exception("❌ Error cargando historial_chat")
        raise HTTPException(status_code=500)

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error actualizar_estado")
        raise HTTPException(status_code=500)

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'notas': datos.notas, 'etiquetas': datos.etiquetas}).eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error actualizar_notas")
        raise HTTPException(status_code=500)

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: TelefonoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'columna': 'Papelera'}).eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error borrar_prospecto")
        raise HTTPException(status_code=500)

@app.post("/api/borrar_permanente")
def borrar_permanente(datos: TelefonoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').delete().eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        supabase.table('mensajes_chat').delete().eq('telefono', datos.telefono).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error borrar_permanente")
        raise HTTPException(status_code=500)

@app.get("/api/buscar_maestro")
def buscar_maestro(q: str):
    try:
        return {"status": "ok", "resultados": supabase.table('catalogo_maestro').select('*').ilike('nombre', f'%{q}%').limit(10).execute().data}
    except Exception as e: 
        logger.exception("❌ Error buscar_maestro")
        raise HTTPException(status_code=500)

@app.post("/api/inyectar_starter")
def inyectar_starter(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        maestros = supabase.table('catalogo_maestro').select('*').eq('starter_pack', True).execute()
        lote = [{"nombre": m["nombre"], "consola": m["consola"], "precio": m["precio_sugerido"], "costo": 0, "stock": 0, "estado_general": "Solo disco", "codigo_barras": "", "vendedor_id": _sesion} for m in maestros.data]
        if lote: supabase.table('inventario').insert(lote).execute()
        return {"status": "ok", "inyectados": len(lote)}
    except Exception as e: 
        logger.exception("❌ Error inyectar_starter")
        raise HTTPException(status_code=500)

# ==========================================
# 📥 GENERADOR DINÁMICO DE PLANTILLAS B2B
# ==========================================
@app.get("/api/descargar_plantilla")
def api_descargar_plantilla(vendedor_id_real: str = Depends(verificar_sesion_b2b)):
    logger.info(f"📥 [SISTEMA] Generando plantilla técnica para: {vendedor_id_real}")
    try:
        res_maestro = supabase.table('catalogo_maestro').select('*').execute()
        items_maestros = res_maestro.data if res_maestro.data else []
        
        res_privado = supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id_real).execute()
        dict_privado = {item['nombre']: item for item in res_privado.data} if res_privado.data else {}

        output = io.StringIO()
        writer = csv.writer(output)
        
        instrucciones = 'INSTRUCCIONES: No borrar filas 1 y 2. Datos inician en fila 3. PRECIOS: Solo números (ej: 500.50), NO usar signo $. Rareza: Dejar vacío para autocompletado por IA.'
        writer.writerow([instrucciones])
        writer.writerow(["nombre", "consola", "costo", "precio", "stock", "estado_general", "rareza", "codigo_barras", "detalles"])

        for m in items_maestros:
            nombre, consola = m['nombre'], m['consola']
            if nombre in dict_privado:
                inv = dict_privado[nombre]
                writer.writerow([
                    nombre, consola, inv.get('costo', 0), inv.get('precio', 0), inv.get('stock', 0), 
                    inv.get('estado_general', 'Completo'), inv.get('rareza', ''), inv.get('codigo_barras', ''), inv.get('descripcion_detallada', '')
                ])
            else:
                writer.writerow([nombre, consola, 0, 0, 0, "Completo", "", "", ""])

        contenido_csv = output.getvalue().encode("utf-8-sig")
        output.close()
        
        return StreamingResponse(
            io.BytesIO(contenido_csv), 
            media_type="text/csv", 
            headers={"Content-Disposition": f"attachment; filename=Plantilla_Veltrix_{vendedor_id_real}.csv"}
        )
    except Exception as e:
        logger.exception("❌ Fallo al generar plantilla técnica")
        raise HTTPException(status_code=500, detail="Error interno al generar el archivo CSV.")

# ==========================================
# 🧰 HELPERS CORE
# ==========================================
def normalizar(texto: str) -> str:
    if not texto: return ""
    t = str(texto).lower().strip()
    t = "".join(c for c in unicodedata.normalize("NFD", t) if unicodedata.category(c) != "Mn")
    return " ".join(t.split())

def safe_float(val) -> float:
    try: return float(str(val).replace("$", "").replace(",", "").strip())
    except: return 0.0

def safe_int(val, default=0) -> int:
    try: return int(float(str(val)))
    except: return default

def generar_sku(vendedor: str, n: str, c: str, e: str) -> str:
    base = f"{vendedor}|{n}|{c}|{e}"
    return hashlib.sha1(base.encode()).hexdigest()

# ==========================================
# 🎮 NORMALIZADORES DE NEGOCIO
# ==========================================
def normalizar_consola(raw: str):
    try:
        consolas_oficiales = ["PS5","PS4","PS3","PS2","PS1","Xbox One","Xbox 360","Xbox Clasico","Nintendo Switch","Nintendo 3DS","Nintendo DS","Nintendo 64","GameCube","GameBoy Advance","GameBoy Color","Wii","Wii U","SNES","NES","Genesis"]
        n = normalizar(raw)
        raw_title = str(raw).strip().title()
        match = difflib.get_close_matches(raw_title, consolas_oficiales, n=1, cutoff=0.7)
        if match: return str(match[0]), normalizar(match[0])
        return raw_title, n
    except Exception as e:
        logger.error(f"❌ Error en normalizar_consola: {e}")
        return "Otro (PC/Varios)", "otro"

def normalizar_estado(raw: str):
    # Auditoría O: Eliminación estricta de código muerto y redundancias
    r = normalizar(raw)
    if any(x in r for x in ["nuevo", "sellado", "new", "sealed"]): return "Nuevo/Sellado"
    if any(x in r for x in ["sin librito", "no manual", "sin manual", "incomplete"]): return "Sin librito"
    if any(x in r for x in ["loose", "suelto", "disco", "cartucho", "solo"]): return "Solo disco"
    return "Completo"

# ==========================================
# 📦 INVENTARIO & DB (BLINDADO B2B)
# ==========================================
@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)): 
    try:
        nombre_limpio = datos.nombre.strip()
        consola_limpia = datos.consola.strip()
        estado = datos.estado_general.strip()
        
        paquete_datos = datos.dict()
        paquete_datos["rareza"] = calcular_rareza_ia(nombre_limpio, consola_limpia, datos.precio)
        paquete_datos["vendedor_id"] = _sesion 
        
        # Sku B2B Estricto
        sku_b2b = generar_sku(_sesion, normalizar(nombre_limpio), normalizar(consola_limpia), normalizar(estado))
        paquete_datos["sku_b2b"] = sku_b2b
        
        # Upsert transaccional seguro
        res = supabase.table("inventario").upsert(paquete_datos, on_conflict="sku_b2b").execute()
        item_id = res.data[0]['id'] if res.data else None
            
        # 👻 TRABAJO FANTASMA
        if item_id and not datos.url_portada:
            background_tasks.add_task(cazar_portada_y_guardar_background, str(item_id), nombre_limpio, consola_limpia)
            
        # 🛡️ Alerta Radar B2B Seguro
        res_alertas = supabase.table('alertas_mercado').select('*').ilike('juego', f"%{nombre_limpio}%").eq('activa', True).execute()
        if res_alertas.data:
            res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).execute()
            if res_config.data:
                config = res_config.data[0]
                admin_ph = config.get("admin_phone", ADMIN_PHONE_GLOBAL)
                for alerta in res_alertas.data:
                    if alerta['precio_maximo'] >= datos.precio and datos.precio > 0:
                        asyncio.create_task(disparar_whatsapp_dinamico_async(admin_ph, f"🎯 *RADAR B2B*\nAlta:\n🎮 {datos.nombre}\n💰 ${datos.precio}", config['meta_token'], config['meta_phone_id']))

        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error guardar_inventario")
        raise HTTPException(status_code=500)

@app.post("/api/borrar_item")
def borrar_item(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('inventario').delete().eq('vendedor_id', _sesion).eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error borrar_item")
        raise HTTPException(status_code=500)

@app.get("/api/cargar_inventario")
def cargar_inventario(vendedor_id_real: str = Depends(verificar_sesion_b2b)):
    try:
        # Límite RAM
        return {"status": "ok", "inventario": supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id_real).order('nombre', desc=False).limit(2000).execute().data}
    except Exception as e: 
        logger.exception("❌ Error cargar_inventario")
        raise HTTPException(status_code=500)

@app.post("/api/actualizar_stock")
def actualizar_stock(datos: VentaItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('inventario').select('precio, costo').eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).eq('vendedor_id', _sesion).execute()
        if res.data and len(res.data) > 0:
            precio_venta, costo_compra = res.data[0].get('precio', 0.0), res.data[0].get('costo', 0.0)
            supabase.table('inventario').update({'stock': datos.nuevo_stock}).eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).eq('vendedor_id', _sesion).execute()
            supabase.table('registro_ventas').insert({"nombre_juego": datos.nombre, "precio_venta": precio_venta, "costo": costo_compra, "ganancia": precio_venta - costo_compra, "vendedor_id": _sesion}).execute()
            return {"status": "ok"}
        return {"status": "error"}
    except Exception as e: 
        logger.exception("❌ Error actualizar_stock")
        raise HTTPException(status_code=500)

@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).eq('vendedor_id', _sesion).limit(1).execute()
        if res.data: return {"status": "ok", "juego": res.data[0]}
        return {"status": "error"}
    except Exception as e: 
        logger.exception("❌ Error buscar_por_codigo")
        raise HTTPException(status_code=500)

@app.get("/api/metricas")
def obtener_metricas(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res_inv = supabase.table('inventario').select('precio, costo, stock').eq('vendedor_id', _sesion).execute()
        total_piezas = sum(i.get('stock', 0) for i in res_inv.data if i.get('stock', 0) > 0)
        valor_inventario = sum((i.get('stock', 0) * i.get('precio', 0.0)) for i in res_inv.data if i.get('stock', 0) > 0)
        costo_inventario = sum((i.get('stock', 0) * i.get('costo', 0.0)) for i in res_inv.data if i.get('stock', 0) > 0)
        
        res_ventas = supabase.table('registro_ventas').select('ganancia, precio_venta').eq('vendedor_id', _sesion).execute()
        return {
            "status": "ok", "piezas": total_piezas, "valor": valor_inventario,
            "costo_inv": costo_inventario, "ganancia_potencial": valor_inventario - costo_inventario,
            "ventas_totales": sum(v.get('precio_venta', 0.0) for v in res_ventas.data), "ganancia_real": sum(v.get('ganancia', 0.0) for v in res_ventas.data)
        }
    except Exception as e: 
        logger.exception("❌ Error obtener_metricas")
        raise HTTPException(status_code=500)

@app.get("/api/radar_b2b")
def radar_b2b(q: str = ""):
    try:
        query = supabase.table('inventario').select('nombre, consola, precio, estado_general, rareza, vendedor_id').gt('stock', 0)
        if q: query = query.ilike('nombre', f'%{q}%')
        return {"status": "ok", "resultados": query.limit(50).execute().data}
    except Exception as e: 
        logger.exception("❌ Error radar_b2b")
        raise HTTPException(status_code=500)

@app.get("/api/bot_config")
def obtener_config_bot(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).limit(1).execute()
        if res.data: return {"status": "ok", "datos": res.data[0]}
        return {"status": "error", "detalle": "Configuración no encontrada"}
    except Exception as e: 
        logger.exception("❌ Error obtener_config_bot")
        raise HTTPException(status_code=500)

@app.post("/api/bot_config")
def guardar_config_bot(datos: BotConfig, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        paquete = {"vendedor_id": _sesion, "link_pago": datos.link_pago, "texto_entrega": datos.texto_entrega, "admin_phone": datos.admin_phone, "bot_activo": datos.bot_activo}
        res_ex = supabase.table('configuracion_bot').select('vendedor_id').eq('vendedor_id', _sesion).execute()
        if res_ex.data: supabase.table('configuracion_bot').update(paquete).eq('vendedor_id', _sesion).execute()
        else: supabase.table('configuracion_bot').insert(paquete).execute()
        return {"status": "ok"}
    except Exception as e: 
        logger.exception("❌ Error guardar_config_bot")
        raise HTTPException(status_code=500)

# ==========================================
# 🚀 ENDPOINT DE IMPORTACIÓN MASIVA
# ==========================================
@router.post("/api/importar_inventario")
async def api_importar_inventario(
    datos: dict,
    background_tasks: BackgroundTasks,
    _sesion: str = Depends(verificar_sesion_b2b)
):
    inicio = time.time()
    lote = datos.get("inventario", [])

    if not isinstance(lote, list) or len(lote) == 0:
        return {"status": "error", "detalle": "Inventario vacío o inválido"}

    logger.info(f"🚀 IMPORT START | vendedor={_sesion} | items={len(lote)}")

    mapa_maestro = {}
    try:
        res = supabase.table("catalogo_maestro").select("nombre, consola, precio_sugerido").execute()
        for i in (res.data or []):
            key = f"{normalizar(i['nombre'])}|{normalizar(i['consola'])}"
            mapa_maestro[key] = i["precio_sugerido"]
    except Exception as e:
        logger.exception("❌ Error precargando catálogo maestro")

    dict_inv = {}
    dict_maestro = {}

    for item in lote:
        try:
            nombre_raw = str(item.get("nombre", "")).strip()
            if not nombre_raw: continue

            n_norm = normalizar(nombre_raw)
            n_disp = nombre_raw.title()
            c_disp, c_norm = normalizar_consola(item.get("consola", ""))
            e_disp = normalizar_estado(item.get("estado_general", ""))
            e_norm = normalizar(e_disp)

            sku = generar_sku(_sesion, n_norm, c_norm, e_norm)

            if sku in dict_inv:
                dict_inv[sku]["stock"] += safe_int(item.get("stock", 1), 1)
                continue

            precio = safe_float(item.get("precio", 0))
            key_m = f"{n_norm}|{c_norm}"

            if precio <= 0:
                precio = mapa_maestro.get(key_m, PRECIO_FALLBACK)

            obj = {
                "vendedor_id": _sesion, "sku_b2b": sku, "nombre": n_disp, "consola": c_disp,
                "estado_general": e_disp, "precio": precio, "costo": safe_float(item.get("costo", 0)),
                "stock": max(0, safe_int(item.get("stock", 1), 1)), "rareza": str(item.get("rareza", "Comun")),
                "codigo_barras": str(item.get("codigo_barras", "")), "descripcion_detallada": str(item.get("detalles", ""))
            }
            dict_inv[sku] = obj

            if key_m not in mapa_maestro:
                dict_maestro[key_m] = {
                    "nombre": n_disp, "consola": c_disp, "precio_sugerido": precio, "rareza": obj["rareza"]
                }
        except Exception as e:
            continue

    try:
        lista_inv = list(dict_inv.values())
        lista_maestro = list(dict_maestro.values())
        total_bg = 0

        for i in range(0, len(lista_inv), BATCH_SIZE):
            chunk = lista_inv[i:i + BATCH_SIZE]
            res = supabase.table("inventario").upsert(chunk, on_conflict="sku_b2b").execute()

            if res.data:
                for row in res.data:
                    if total_bg >= MAX_BG_TASKS: break
                    background_tasks.add_task(cazar_portada_y_guardar_background, str(row["id"]), row["nombre"], row["consola"])
                    total_bg += 1

        for i in range(0, len(lista_maestro), BATCH_SIZE):
            chunk = lista_maestro[i:i + BATCH_SIZE]
            supabase.table("catalogo_maestro").upsert(chunk, on_conflict="nombre,consola").execute()

        duracion = round(time.time() - inicio, 2)
        logger.info(f"✅ IMPORT OK | items={len(lista_inv)} | time={duracion}s")

        return {"status": "ok", "procesados": len(lista_inv), "nuevos_maestro": len(lista_maestro), "tiempo": f"{duracion}s"}

    except Exception as e:
        logger.exception("❌ UPSERT ERROR")
        raise HTTPException(status_code=500, detail="Error interno durante el volcado de inventario")

# ==========================================
# 🌐 WEBHOOK META & RUNNER FINAL
# ==========================================
@app.get("/webhook")
def verificar_webhook(request: Request):
    if request.query_params.get("hub.verify_token") == WEBHOOK_SECRET:
        return PlainTextResponse(content=request.query_params.get("hub.challenge"), status_code=200)
    return PlainTextResponse(content="CRM B2B ACTIVO", status_code=200)

@app.post("/webhook", dependencies=[Depends(validar_firma_meta)])
async def recibir_mensaje_meta(request: Request, background_tasks: BackgroundTasks):
    try:
        datos = await request.json()
        if not isinstance(datos, dict) or "entry" not in datos or not datos["entry"]:
            return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

        cambios = datos["entry"][0].get("changes", [])
        if not cambios: return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

        valor = cambios[0].get("value", {})
        mensajes = valor.get("messages", [])
        if not mensajes: return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

        msg = mensajes[0]
        msg_id = msg.get("id", "")

        if msg_id in procesados_recientemente:
            logger.info("⚡ Evento duplicado de Meta ignorado.")
            return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

        procesados_recientemente.append(msg_id)
        phone_id_receptor = valor.get("metadata", {}).get("phone_number_id", "")

        background_tasks.add_task(gestionar_mensaje_entrante_bg, valor, msg, phone_id_receptor)
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

    except Exception as e:
        logger.exception("❌ ERROR CRÍTICO WEBHOOK")
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)

# ==========================================
# 🏁 ANCLAJE FINAL
# ==========================================
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
