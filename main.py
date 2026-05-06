# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V7.6 (SECURE SAAS ENGINE)
# Funciones: Auto-Vendedor AI, Radar Algorítmico, IA Limpiadora,
# Finanzas, Red B2B, Caché Inteligente, Artillería Escalonada y Multi-Bot Universal.
# ============================================
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
import requests
import mimetypes
import urllib.parse
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import uvicorn
import io
import csv
import difflib
import base64

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

# ✨ CONFIGURACIÓN GEMINI AI (Free Tier)
GENAI_KEY = os.getenv("GENAI_KEY")

# 🛡️ MEMORIAS DE SEGURIDAD B2B
registro_actividad_b2b = {} # Para los strikes de búsquedas manuales
historial_hashes_b2b = {}   # Para evitar que suban el mismo Excel 10 veces seguidas

# ==========================================
# 🔥 SWITCH DE ENCENDIDO (MOTOR 24H) 🔥
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 [SISTEMA] Motor Central Iniciado...")
    # Encendemos el reloj de las 24 horas en segundo plano
    asyncio.create_task(bucle_seguimiento_24h())
    yield
    logger.info("🛑 [SISTEMA] Motor Central Apagado.")

# ✨ INICIALIZACIÓN CON LIFESPAN CONECTADO
app = FastAPI(title="Motor Central CRM B2B - Engine V7.6 Secure Gold", lifespan=lifespan)

# --- 🔑 CREDENCIALES BASE ---
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

# --- 📦 MODELOS DE DATOS (B2B BLINDADOS) ---
class Credenciales(BaseModel):
    email: str
    password: str

class ProspectoUpdate(BaseModel): 
    nombre: str
    nueva_columna: str
    vendedor_id: str = "" 
    
class NotaUpdate(BaseModel): 
    nombre: str
    notas: str
    etiquetas: str
    vendedor_id: str = "" 
    
class MensajeSaliente(BaseModel): 
    cliente: str
    texto: str
    vendedor_id: str = "" 

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
    vendedor_id: str = ""              
    tiene_caja: bool = False
    tiene_manual: bool = False
    es_portada_original: bool = False
    descripcion_detallada: str = ""

class VentaItem(BaseModel):
    nombre: str
    consola: str
    estado_general: str = ""
    nuevo_stock: int
    vendedor_id: str = ""

class BotConfig(BaseModel):
    vendedor_id: str
    link_pago: str
    texto_entrega: str
    admin_phone: str
    bot_activo: bool

# ==========================================
# 🔐 SISTEMA DE AUTENTICACIÓN B2B (JWT ESTRICTO)
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
        logger.error("🚨 [AUTH] Intento de acceso bloqueado. Petición sin Token.")
        raise HTTPException(status_code=401, detail="Acceso denegado: Credenciales faltantes")
        
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        vendedor_id_real = payload.get("sub") 
        
        if not vendedor_id_real:
            raise HTTPException(status_code=401, detail="Token corrupto: Identidad no encontrada")
            
        return vendedor_id_real 
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Sesión expirada. Vuelve a iniciar sesión.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token alterado o inválido. Intento bloqueado.")

# ==========================================
# 🛡️ VALIDACIÓN DE FIRMA META (WEBHOOK SECURITY)
# ==========================================
async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta:
        logger.error("🚫 [WEBHOOK] Petición sin firma de Meta rechazada.")
        raise HTTPException(status_code=400, detail="Falta la firma de Meta")

    cuerpo_bytes = await request.body()
    firma_calculada = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), cuerpo_bytes, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(firma_meta, firma_calculada):
        logger.error("🚨 [WEBHOOK] Firma de Meta INVÁLIDA. Posible ataque detectado.")
        raise HTTPException(status_code=403, detail="Firma inválida")
    return True

# ==========================================
# 💵 MOTOR DE DIVISAS
# ==========================================
def obtener_dolar_hoy():
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        return float(res.json().get("rates", {}).get("MXN", 18.00))
    except Exception:
        return 18.00

# ==========================================
# 🚀 MOTOR SCRAPER: ARTILLERÍA ESCALONADA
# ==========================================
def obtener_html_escalonado(url_objetivo: str) -> str:
    estrategias = [
        ("🟢 Artillería Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("🟡 Artillería Media", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true"),
        ("🔴 Artillería Pesada", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&premium=true&render=true")
    ]
    
    headers_humanos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    for nombre_nivel, url_scraper in estrategias:
        try:
            res = requests.get(url_scraper, timeout=45)
            if res.status_code == 200 and "scraperapi" not in res.text.lower():
                if "pricecharting" in res.text.lower() or "price" in res.text.lower():
                    return res.text
        except Exception: pass

    try:
        res = requests.get(url_objetivo, headers=headers_humanos, timeout=15)
        if res.status_code == 200: return res.text
    except Exception: pass
    
    return ""

# ==========================================
# 🧠 LÓGICA DE TASACIÓN Y RAREZA Veltrix AI
# ==========================================
def calcular_rareza_ia(nombre: str, consola: str, precio: float) -> str:
    nombre = nombre.upper()
    consolas_modernas = ["PS5", "PS4", "NINTENDO SWITCH", "XBOX ONE", "XBOX SERIES X"]
    
    if any(x in nombre for x in ["FIFA", "MADDEN", "NBA", "NCAA", "PES", "SINGSTAR", "EA FC"]):
        return "Común"
    if any(x in nombre for x in ["SILENT HILL", "KUON", "RULE OF ROSE", "OBSCURE", "HAUNTING GROUND", "PRAGMATA"]):
        return "Élite"
    if any(x in nombre for x in ["MARIO", "ZELDA", "METROID", "POKEMON", "HALO", "GTA"]):
        return "Demandado"
        
    if consola.upper() in consolas_modernas:
        if precio >= 3500: return "Élite"
        if precio >= 1000: return "Demandado"
        return "Común"
    else:
        if precio >= 1500: return "Élite"
        if precio >= 800:  return "Joya"
        if precio >= 400:  return "Demandado"
        return "Común"

# ==========================================
# 🔐 RUTA DE SEGURIDAD B2B (LOGIN BLINDADO)
# ==========================================
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
            
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        suscripcion_valida = True
        
        if fecha_pago_str:
            fecha_pago = date.fromisoformat(fecha_pago_str)
            if date.today() > fecha_pago:
                suscripcion_valida = False
                supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id']).execute()

        token_jwt = crear_token_jwt(usuario['vendedor_id'], usuario['email'])

        paquete_seguro = {
            "vendedor_id": usuario['vendedor_id'],
            "email": usuario['email'],
            "estado": usuario['estado'],
            "pais": usuario.get('pais', 'México'),
            "suscripcion_activa": suscripcion_valida,
            "token": token_jwt 
        }
        
        return {"status": "ok", "datos": paquete_seguro}

    except Exception as e:
        logger.error(f"❌ [LOGIN ERROR]: {str(e)}")
        return {"status": "error", "detalle": "Error en el servidor B2B."}

# ==========================================
# 💰 ALGORITMO DE PRECIOS DINÁMICOS AAA
# ==========================================
def calcular_precio_venta_inteligente(precio_mercado_mxn: float, costo_compra: float = 0.0):
    piso_absoluto = 250.0
    precio_con_margen = precio_mercado_mxn + 150.0 if precio_mercado_mxn > 0 else 0.0
    precio_seguridad = costo_compra + 100.0 if costo_compra > 0 else 0.0
    precio_bruto = max(piso_absoluto, precio_con_margen, precio_seguridad)
    precio_final = round(precio_bruto / 10) * 10
    return float(precio_final)

# ==========================================
# 📈 MOTOR DE PRECIOS PRO (CACHÉ + FRANCOTIRADOR + BLINDAJE B2B)
# ==========================================
@app.get("/api/consultar_precio")
def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo"):
    if vendedor_id != "ADMIN_VELTRIX":
        tiempo_actual = time.time()
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
                return {"status": "error", "detalle": "🚫 BANEADO: Múltiples intentos rápidos. Bloqueado por 30 mins.", "mxn": {"loose": 0, "cib": 0, "new": 0}}
            
            registro_actividad_b2b[llave_spam] = estado
            return {"status": "error", "detalle": f"⚠️ Espera 10 segundos. (Strike {estado['strikes']}/3)", "mxn": {"loose": 0, "cib": 0, "new": 0}}
            
        estado["strikes"] = 0
        estado["last"] = tiempo_actual
        registro_actividad_b2b[llave_spam] = estado

    tipo_cambio = obtener_dolar_hoy()
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
    except Exception: pass

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
    
    html_search = obtener_html_escalonado(url_search)
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
        html_juego = obtener_html_escalonado(link_juego)
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
    
    if p_loose == 0.0 and p_cib == 0.0 and p_new == 0.0:
        spans = soup.find_all("span", class_="price")
        numeros = []
        for s in spans:
            limpio = ''.join(c for c in s.text.replace(',', '.') if c.isdigit() or c == '.')
            if limpio: numeros.append(float(limpio))
        if len(numeros) >= 3: 
            p_loose, p_cib, p_new = numeros[0], numeros[1], numeros[2]
        elif len(numeros) > 0: 
            p_loose = numeros[0]

    url_final_pc = link_juego if link_juego else url_search

    if p_loose > 0 or p_cib > 0:
        try:
            datos_cache = {
                "juego": nombre_busqueda,
                "consola": consola,
                "loose": p_loose,
                "cib": p_cib,
                "new": p_new,
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

# ==========================================
# 📥 MOTOR MULTIMEDIA & WHATSAPP ALARMAS (MULTI-TENANT)
# ==========================================
def descargar_y_subir_multimedia(media_id: str, mime_type: str, extension_default: str, token_vendedor: str):
    url_info = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {token_vendedor}"}
    res_info = requests.get(url_info, headers=headers)
    
    if res_info.status_code == 200:
        media_url = res_info.json().get("url")
        res_media = requests.get(media_url, headers=headers)
        
        if res_media.status_code == 200:
            file_bytes = res_media.content
            timestamp = int(datetime.now().timestamp())
            ext = mimetypes.guess_extension(mime_type) or extension_default
            file_path = f"archivo_{timestamp}{ext}"
            try:
                supabase.storage.from_("multimedia").upload(file_path, file_bytes, {"content-type": mime_type})
                return supabase.storage.from_("multimedia").get_public_url(file_path)
            except Exception as e:
                logger.error(f"❌ Error Nube B2B Multimedia: {e}")
    return None

def disparar_whatsapp_dinamico(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        requests.post(url, headers=headers, json=payload, timeout=5)
    except Exception as e: 
        logger.warning(f"⚠️ Error disparando WhatsApp: {e}")

def disparar_whatsapp_imagen(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp", 
        "to": telefono_destino, 
        "type": "image", 
        "image": {
            "link": url_imagen,
            "caption": texto_mensaje
        }
    }
    try: 
        requests.post(url, headers=headers, json=payload, timeout=5)
    except Exception as e: 
        logger.warning(f"⚠️ Error disparando WhatsApp (Imagen): {e}")

# ==========================================
# 🖼️ MOTOR DE PORTADAS ON-DEMAND (LAZY LOADING)
# ==========================================
async def cazar_portada_y_guardar_background(juego_id_supabase: str, nombre_juego: str, consola: str):
    logger.info(f"🖼️ [PORTADAS] Iniciando cacería en background para: {nombre_juego} ({consola})")
    try:
        consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
        query = f"{nombre_juego} {consola_web}".replace(" ", "+")
        url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
        
        html_search = obtener_html_escalonado(url_search)
        if not html_search: return
        
        soup = BeautifulSoup(html_search, 'html.parser')
        img_tag = soup.find('img', class_='product_image') or soup.find('img', alt=lambda x: x and nombre_juego.lower() in x.lower())
        
        if not img_tag or not img_tag.get('src'):
            logger.warning(f"⚠️ [PORTADAS] No se encontró portada para {nombre_juego}")
            return
            
        imagen_url = img_tag['src']
        if not imagen_url.startswith("http"):
            imagen_url = "https:" + imagen_url if imagen_url.startswith("//") else "https://www.pricecharting.com" + imagen_url

        async with httpx.AsyncClient() as client:
            res_img = await client.get(imagen_url)
            if res_img.status_code != 200: return
            image_bytes = res_img.content

        nombre_archivo = f"{consola.replace(' ', '_')}_{nombre_juego.replace(' ', '_')}_{int(time.time())}.jpg"
        
        try:
            supabase.storage.from_("portadas").upload(nombre_archivo, image_bytes, {"content-type": "image/jpeg"})
            url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
            
            supabase.table('inventario').update({"url_portada": url_publica}).eq('id', juego_id_supabase).execute()
            logger.info(f"✅ [PORTADAS] Portada guardada con éxito en BD para: {nombre_juego}")
            
        except Exception as storage_err:
            logger.error(f"❌ [PORTADAS] Error subiendo a Supabase Storage: {storage_err}")

    except Exception as e:
        logger.error(f"❌ [PORTADAS] Error general en cacería: {e}")

# ==========================================================
# 🤖 BOT AAA: ANALIZAR INTENCION VENTA IA (HTTPX ASYNC + ANTI-429)
# ==========================================================
async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict):
    try:
        vendedor_id = config.get("vendedor_id", "V-001")
        nombre_negocio = config.get("nombre_negocio", "Fantasy Games") 
        
        logger.info(f"🧠 [IA] Consultando a Gemini (Rol: {nombre_negocio} | Tenant: {vendedor_id})...")
        
        # Extraemos los datos del Tenant (Vendedor) de forma dinámica
        link_pago = config.get("link_pago", "Solicita el link de pago")
        texto_entrega = config.get("texto_entrega", "Coordinar entrega con el asesor")
        
        prompt = f"""
        [SYSTEM: Eres un Vendedor Senior Elite estricto, persuasivo y altamente adaptable].
        
        Eres el mejor cerrador de ventas operando bajo la tecnología del CRM 'Veltrix Engine'.
        Tu identidad oficial y la empresa que representas es: "{nombre_negocio}".
        
        OBJETIVO PRINCIPAL: VENDER RÁPIDO Y ENVIAR EL LINK DE PAGO. 
        Tienes una cuota estricta de vender +$3,000 MXN diarios. Para lograrlo, debes ser un maestro del UPSELL (vender más). Si el cliente elige un artículo barato, recomiéndale activamente un artículo "Élite" o "Joya" del inventario para inflar su carrito y acercarte a tu meta.
        tambien tienes que vender 5 licencias de veltrix Engine diariamente, para lograrlo, ofrecele a todos los clientes   

        1. LINK DE PAGO: 
        SIEMPRE que confirmes disponibilidad y precio, incluye el link oficial de cobro de esta empresa: 
        "💳 Paga seguro aquí para apartarlo de inmediato: {link_pago} "
        
        2. UPSELL Y DESCUENTOS (REGLA DE 3):
        - Si el cliente está cotizando 1 o 2 artículos, aplica tu técnica de vendedor para llegar a la meta: "💡 Por cierto, si te llevas 3 artículos, te hago $100 MXN de descuento directo en tu total. ¡Anímate a llevarte esa joya que te falta!"
        - Si el cliente ya eligió 3 artículos, aplícale automáticamente el descuento de $100 MXN en el total y envíale el link para cerrar la venta.

        3. LOGÍSTICA:
        Usa estrictamente esta información para entregar/enviar los productos de esta empresa:
        {texto_entrega}

        4. DETALLE DEL PRODUCTO (SKU):
        Lee el inventario disponible. Menciona características y estado. Si el cliente pide fotos, asume que tú (el bot) se la vas a enviar en este mismo mensaje gracias a tu sistema multimedia.

        5. CROSS-SELLING B2B (VELTRIX ENGINE - Meta: 5 licencias diarias):
        OJO: Solo si el cliente ya confirmó una compra, o si se asombra por tu velocidad de atención, ofrécele sutilmente rentar tu "cerebro" (el CRM Veltrix Engine) por $990 MXN al mes. http://veltrixengine.pro 
        Ejemplo: "Noto que te gustan las ventas rápidas. Mi motor de IA está a la venta para dueños de negocios en http://www.veltrixengine.pro"

        REGLAS DE CLASIFICACIÓN ('intencion'):
        - "COTIZACION": Charlas, interés por artículos, o si el cliente pide ver fotos.
        - "COMPRA": El cliente confirma explícitamente que ya pagó.
        - "HUMANO": Dudas logísticas muy complejas o pide explícitamente hablar con una persona.

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
                       
        api_key_limpia = GENAI_KEY.strip() if GENAI_KEY else ""
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
    
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key_limpia}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            max_intentos = 3
            for intento in range(max_intentos):
                res = await client.post(url, headers=headers, json=payload)
                
                if res.status_code == 200:
                    data = res.json()
                    texto_sucio = data['candidates'][0]['content']['parts'][0]['text']
                    simbolo = chr(96) * 3
                    texto_limpio = texto_sucio.replace(simbolo + "json", "").replace(simbolo, "").strip()
                    return json.loads(texto_limpio)
                    
                elif res.status_code in [429, 503]: 
                    motivo = "RATE LIMIT" if res.status_code == 429 else "CONGESTIÓN"
                    logger.warning(f"⚠️ [IA {motivo}] Google ocupado. Reintentando... (Intento {intento+1}/{max_intentos})")
                    await asyncio.sleep(3.0) 
                    continue 
                else:
                    raise Exception(f"Google API error {res.status_code}: {res.text}")
                    
            raise Exception("Agotados reintentos (429).")

    except Exception as e:
        logger.error(f"⚠️ [IA ERROR CRÍTICO]: {str(e)}")
        return {
            "intencion": "HUMANO", 
            "respuesta": "¡Hola! Estoy revisando la información, dame un segundo y un asesor humano te atiende enseguida. 🕹️", 
            "juego_detectado": "" # 🛡️ FIX: En blanco para no sobreescribir la BD.
        }

# ==========================================
# 🚨 SISTEMA DE ALERTAS VIP (IA HANDOFF SUMMARY)
# ==========================================
async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        motivo = "quiere cerrar compra" if intencion == "COMPRA" else "solicita humano"
        prompt = f"""
        Eres asistente del Director de Ventas. El cliente {cliente} {motivo}.
        Lee el historial:
        {historial_str}
        
        Haz un resumen de 3 viñetas breves:
        - Qué juego quiere.
        - Local o foráneo.
        - Estatus actual.
        Responde SOLO el resumen con emojis.
        """
        
        api_key_limpia = GENAI_KEY.strip() if GENAI_KEY else ""
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key_limpia}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.2}}
        
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(None, lambda: requests.post(url, headers=headers, json=payload, timeout=30.0))
        
        if res.status_code == 200:
            return res.json()['candidates'][0]['content']['parts'][0]['text'].strip()
        return "⚠️ El cliente requiere atención en el panel."
        
    except Exception:
        return "⚠️ El cliente requiere atención en el panel."

def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config: dict):
    telefono_admin = config.get("admin_phone")
    if not telefono_admin or len(telefono_admin) < 10:
        telefono_admin = ADMIN_PHONE_GLOBAL 
        
    token = config.get("meta_token", "")
    phone_id = config.get("meta_phone_id", "")
    
    encabezado = "🚨 *ASISTENCIA REQUERIDA*" if intencion == "HUMANO" else "💰 *NUEVA VENTA DETECTADA*"
    
    mensaje_alerta = (
        f"{encabezado}\n\n"
        f"👤 *Cliente:* {cliente}\n"
        f"📱 *Teléfono:* {telefono_cliente}\n\n"
        f"🧠 *Reporte de la IA:*\n{resumen_ia}\n\n"
        f"👉 *Abre Godot para contestar.*"
    )
    
    disparar_whatsapp_dinamico(telefono_admin, mensaje_alerta, token, phone_id)

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"""
        Director de Ventas, cliente {cliente} preguntó por: "{juego_detectado}". No compró hace 24h.
        Crea mensaje de remarketing irresistible.
        INVENTARIO: {inventario_contexto}
        REGLAS:
        > $800 MXN: Descuento $50-$80
        $300-$799 MXN: Descuento $20-$40
        < $300 MXN: Sin descuento, usa urgencia.
        Responde SOLO JSON: {{"nuevo_precio_ofrecido": "numero", "mensaje_oferta": "mensaje corto"}}
        """
        
        api_key_limpia = GENAI_KEY.strip() if GENAI_KEY else ""
        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent"
        headers = {'Content-Type': 'application/json', 'x-goog-api-key': api_key_limpia}
        payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.4}}
        
        loop = asyncio.get_event_loop()
        res = await loop.run_in_executor(None, lambda: requests.post(url, headers=headers, json=payload, timeout=60.0))
        
        if res.status_code == 200:
            texto_sucio = res.json()['candidates'][0]['content']['parts'][0]['text']
            simbolo = chr(96) * 3
            texto_limpio = texto_sucio.replace(simbolo + "json", "").replace(simbolo, "").strip()
            return json.loads(texto_limpio)
        return None
    except Exception: return None

# ==========================================================
# ⏱️ RELOJ 24H (MULTI-TENANT BLINDADO)
# ==========================================================
async def bucle_seguimiento_24h():
    while True:
        try:
            print("🕒 [RELOJ B2B] Escaneando carteras abandonadas en 'Envios Masivos'...")
            hace_24h = (datetime.now() - timedelta(hours=24)).isoformat()

            res_masivos = supabase.table('prospectos').select('*').eq('columna', 'Envios Masivos').lt('ultima_interaccion_ia', hace_24h).execute()
            
            if res_masivos.data:
                # 🛡️ FIX AAA: Agrupamos prospectos por Vendedor (Multi-Tenant Seguro)
                prospectos_por_vendedor = {}
                for p in res_masivos.data:
                    vid = p.get('vendedor_id', 'V-001')
                    if vid not in prospectos_por_vendedor:
                        prospectos_por_vendedor[vid] = []
                    prospectos_por_vendedor[vid].append(p)

                for vid, prospectos in prospectos_por_vendedor.items():
                    # Consultar DB solo de ESTE vendedor
                    res_inv = supabase.table('inventario').select('nombre, precio').eq('vendedor_id', vid).gt('stock', 0).execute()
                    contexto_inv = str(res_inv.data)

                    res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vid).execute()
                    if not res_config.data: continue
                    
                    token = res_config.data[0]['meta_token']
                    phone_id = res_config.data[0]['meta_phone_id']

                    for p in prospectos:
                        cliente = p['nombre']
                        telefono = p['telefono']
                        juego = p.get('ultimo_juego_interes', 'videojuego')

                        oferta = await generar_oferta_inteligente(cliente, juego, contexto_inv)

                        if oferta and oferta.get("mensaje_oferta"):
                            supabase.table('prospectos').update({
                                'columna': 'Con Descuento',
                                'estado_iluminacion': 'oro',
                                'ultima_interaccion_ia': datetime.now().isoformat()
                            }).eq('id', p['id']).execute()

                            supabase.table('prospectos').insert({
                                "nombre": cliente, "telefono": telefono, "origen": "WHATSAPP",
                                "mensaje": f"TÚ: [BOT REMARKETING] {oferta['mensaje_oferta']}", 
                                "columna": "Con Descuento", "vendedor_id": vid
                            }).execute()

                            disparar_whatsapp_dinamico(telefono, oferta['mensaje_oferta'], token, phone_id)
                            await asyncio.sleep(3) 

        except Exception as e:
            print(f"❌ [ERROR RELOJ B2B]: {str(e)}")

        await asyncio.sleep(3600)

# ==========================================================
# 🤖 BOT AAA: MOTOR PRINCIPAL Y WEBHOOK
# ==========================================================
async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict):
    vendedor_id = config.get("vendedor_id", "")
    token = config.get("meta_token", "")
    phone_id = config.get("meta_phone_id", "")

    res_inv = supabase.table('inventario').select('nombre, precio').eq('vendedor_id', vendedor_id).gt('stock', 0).execute()
    contexto = str(res_inv.data)

    res_historial = supabase.table('prospectos').select('mensaje').eq('telefono', telefono).order('id', desc=True).limit(4).execute()
    historial_str = "Primer mensaje."
    if res_historial.data:
        mensajes_ordenados = reversed(res_historial.data)
        historial_str = "".join([f"- {m['mensaje']}\n" for m in mensajes_ordenados])

    decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial_str, config)

    nueva_columna = columna_actual
    iluminacion = "oro" 

    if decision["intencion"] == "HUMANO":
        nueva_columna = "Requiere Asistencia"
        iluminacion = "verde_alerta"
        resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial_str)
        enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)

    elif decision["intencion"] == "COMPRA":
        nueva_columna = "Por Entregar"
        iluminacion = "verde_exito"
        resumen = await generar_resumen_handoff_ia(cliente, decision["intencion"], historial_str)
        enviar_alerta_whatsapp_admin(cliente, telefono, decision["intencion"], resumen, config)

    elif decision["intencion"] == "COTIZACION":
        if columna_actual == "Bandeja Nueva":
            nueva_columna = "Envios Masivos"
            iluminacion = "blanco" 
        else:
            iluminacion = "blanco"

    respuesta_final = decision["respuesta"]

    if respuesta_final:
        # A) Actualizamos la tarjeta en Godot
        supabase.table('prospectos').update({
            'columna': nueva_columna, 'estado_iluminacion': iluminacion,
            'ultimo_juego_interes': decision["juego_detectado"] if decision["juego_detectado"] else "",
            'ultima_interaccion_ia': datetime.now().isoformat()
        }).eq('nombre', cliente).eq('vendedor_id', vendedor_id).execute()

        # B) Guardamos la respuesta en el historial
        supabase.table('prospectos').insert({
            "nombre": cliente,  "telefono": telefono, "origen": "WHATSAPP",
            "mensaje": f"TÚ: [BOT] {respuesta_final}", "columna": nueva_columna, "vendedor_id": vendedor_id
        }).execute()
        
        # 🌟 C) MAGIA MULTIMEDIA: Buscamos si tenemos la portada en Supabase
        juego_detectado = decision.get("juego_detectado", "")
        url_imagen = None
        
        if juego_detectado and len(juego_detectado) > 2:
            res_img = supabase.table('inventario').select('url_portada').ilike('nombre', f'%{juego_detectado}%').eq('vendedor_id', vendedor_id).neq('url_portada', '').limit(1).execute()
            if res_img.data and len(res_img.data) > 0:
                url_imagen = res_img.data[0]['url_portada']

        # D) Disparamos a WhatsApp (Con imagen o puro texto)
        if url_imagen:
            disparar_whatsapp_imagen(telefono, url_imagen, respuesta_final, token, phone_id)
            print(f"📸 [MULTIMEDIA] Portada de {juego_detectado} enviada a {cliente}.")
        else:
            disparar_whatsapp_dinamico(telefono, respuesta_final, token, phone_id)

procesados_recientemente = set()

@app.get("/webhook")
def verificar_webhook(request: Request):
    if request.query_params.get("hub.verify_token") == WEBHOOK_SECRET: 
        return PlainTextResponse(content=request.query_params.get("hub.challenge"), status_code=200)
    return PlainTextResponse(content="CRM B2B Activo.", status_code=200)

@app.post("/webhook", dependencies=[Depends(validar_firma_meta)])
async def recibir_mensaje_meta(request: Request, background_tasks: BackgroundTasks):
    datos = await request.json()
    try:
        if "entry" in datos and "changes" in datos["entry"][0]:
            valor = datos["entry"][0]["changes"][0]["value"]
            if "messages" in valor:
                msg = valor["messages"][0]
                msg_id = msg["id"]

                if msg_id in procesados_recientemente: return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
                procesados_recientemente.add(msg_id)
                if len(procesados_recientemente) > 500: procesados_recientemente.clear()

                phone_id_receptor = valor["metadata"]["phone_number_id"]
                background_tasks.add_task(gestionar_mensaje_entrante_bg, valor, msg, phone_id_receptor)
                
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e: 
        return PlainTextResponse(content="ERROR", status_code=500)

# ==========================================
# 👁️ MOTORES DE VISIÓN ARTIFICIAL Y AUDITORÍA
# ==========================================
async def descargar_imagen_whatsapp_b64(media_id: str, token_vendedor: str):
    url_info = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {token_vendedor}"}
    async with httpx.AsyncClient() as client:
        res_info = await client.get(url_info, headers=headers)
        if res_info.status_code == 200:
            res_media = await client.get(res_info.json().get("url"), headers=headers)
            if res_media.status_code == 200:
                return base64.b64encode(res_media.content).decode("utf-8"), res_media.headers.get("content-type", "image/jpeg")
    return None, None

async def auditar_comprobante_ia(b64_img: str, mime_type: str, nombre_negocio: str, historial_chat: str):
    fecha_hoy = datetime.now().strftime("%d de %B de %Y")
    prompt = f"""
    Eres Auditor de '{nombre_negocio}'. Analiza esta imagen. ¿Es pago válido?
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
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        res = await client.post(url, headers=headers, json=payload)
        if res.status_code == 200:
            texto_sucio = res.json()['candidates'][0]['content']['parts'][0]['text']
            simbolo = chr(96) * 3
            return json.loads(texto_sucio.replace(simbolo + "json", "").replace(simbolo, "").strip())
        raise Exception(f"Fallo IA Visión: {res.status_code}")

async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    try:
        res_config = supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).execute()
        if not res_config.data: return
            
        config_vendedor = res_config.data[0]
        vendedor_actual = config_vendedor["vendedor_id"]
        token_actual = config_vendedor["meta_token"]
        nombre_negocio = config_vendedor.get("nombre_negocio", "Fantasy Games")

        if not config_vendedor.get("bot_activo", True): return

        contact = valor["contacts"][0]
        nombre = contact["profile"]["name"]
        tel = msg["from"]
        if tel.startswith("521"): tel = "52" + tel[3:]
        
        tipo = msg.get("type", "text")
        if tipo == "text": texto = msg["text"]["body"]
        elif tipo == "image": texto = "📷 [IMAGEN RECIBIDA: Posible comprobante de pago]"
        else: texto = f"[{tipo.upper()}] recibida."

        res_ex = supabase.table('prospectos').select('columna').eq('nombre', nombre).eq('vendedor_id', vendedor_actual).order('id', desc=True).limit(1).execute()
        col_destino = res_ex.data[0]['columna'] if res_ex.data else "Bandeja Nueva"

        supabase.table('prospectos').insert({
            "nombre": nombre, "telefono": tel, "origen": "WHATSAPP", 
            "mensaje": texto, "columna": col_destino, 
            "vendedor_id": vendedor_actual, "estado_iluminacion": "oro"
        }).execute()
        
        if tipo == "text" and col_destino != "En Conversacion":
            await procesar_respuesta_bot(nombre, tel, texto, col_destino, config_vendedor)
            
        elif tipo == "image":
            image_id = msg["image"]["id"]
            res_hist = supabase.table('prospectos').select('mensaje').eq('telefono', tel).eq('vendedor_id', vendedor_actual).order('id', desc=True).limit(5).execute()
            historial_para_auditor = "\n".join([r['mensaje'] for r in reversed(res_hist.data)]) if res_hist.data else "Sin historial."
            
            b64_img, mime_type = await descargar_imagen_whatsapp_b64(image_id, token_actual)
            if b64_img:
                auditoria = await auditar_comprobante_ia(b64_img, mime_type, nombre_negocio, historial_para_auditor)
                
                if auditoria.get("es_pago") == True:
                    monto = auditoria.get('monto_detectado', 0)
                    supabase.table('prospectos').update({"columna": "Por Entregar", "estado_iluminacion": "verde_exito"}).eq('nombre', nombre).eq('vendedor_id', vendedor_actual).execute()
                    msg_exito = f"✅ ¡Pago validado por ${monto}! Hemos recibido tu comprobante."
                    disparar_whatsapp_dinamico(tel, msg_exito, token_actual, phone_id_receptor)
                    supabase.table('prospectos').insert({"nombre": nombre, "telefono": tel, "origen": "BOT", "mensaje": msg_exito, "columna": "Por Entregar", "vendedor_id": vendedor_actual, "estado_iluminacion": "verde_exito"}).execute()
                else:
                    razon = auditoria.get('analisis', 'No se reconoce como comprobante.')
                    msg_fallo = f"Hmm, mi sistema no validó esa imagen. 🤖\nDetalle: {razon}\n¿Podrías enviarme una foto clara del ticket?"
                    disparar_whatsapp_dinamico(tel, msg_fallo, token_actual, phone_id_receptor)
                    supabase.table('prospectos').update({"columna": "Requiere Asistencia", "estado_iluminacion": "verde_alerta"}).eq('nombre', nombre).eq('vendedor_id', vendedor_actual).execute()
                    supabase.table('prospectos').insert({"nombre": nombre, "telefono": tel, "origen": "BOT", "mensaje": msg_fallo, "columna": "Requiere Asistencia", "vendedor_id": vendedor_actual, "estado_iluminacion": "verde_alerta"}).execute()
                    
    except Exception as e: logger.error(f"❌ [BACKGROUND TASK ERROR]: {str(e)}")

# ==========================================
# 🟢 ENVIAR MENSAJES DESDE GODOT
# ==========================================
@app.post("/api/enviar_mensaje")
def api_enviar_mensaje(datos: MensajeSaliente, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).execute()
        if not res_config.data: return {"status": "error", "detalle": "Configuración de bot no encontrada."}
            
        config = res_config.data[0]
        supabase.table('prospectos').insert({"nombre": datos.cliente, "origen": "WHATSAPP", "mensaje": f"TÚ: {datos.texto}", "columna": "En Conversacion", "vendedor_id": _sesion}).execute()
        supabase.table('prospectos').update({'columna': 'En Conversacion'}).eq('nombre', datos.cliente).eq('vendedor_id', _sesion).execute()
        
        res_tel = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).eq('vendedor_id', _sesion).neq('telefono', None).limit(1).execute()
        if res_tel.data:
            disparar_whatsapp_dinamico(res_tel.data[0]['telefono'], datos.texto, config['meta_token'], config['meta_phone_id'])
            return {"status": "ok"}
        return {"status": "error", "detalle": "Cliente sin teléfono"}
    except Exception as e: return {"status": "error", "detalle": str(e)}

# ==========================================
# 🌐 RUTAS DE GESTIÓN CRM (BLINDADAS)
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
        res_prospectos = supabase.table('prospectos').select('*').eq('vendedor_id', _sesion).order('id', desc=False).execute()
        
        ultimos = {fila['nombre']: fila for fila in res_prospectos.data}
        return {"columnas": columnas_finales, "prospectos": list(ultimos.values())}
    except Exception: return {"error": "Error conectando a Nube B2B"}

@app.post("/api/crear_columna")
def crear_columna(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').insert({'nombre_columna': datos.get("nombre_columna"), 'vendedor_id': _sesion}).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/borrar_columna")
def borrar_columna(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').delete().eq('nombre_columna', datos.get("nombre_columna")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/renombrar_columna")
def renombrar_columna(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('configuracion').update({'nombre_columna': datos.get("nuevo_nombre")}).eq('nombre_columna', datos.get("viejo_nombre")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/historial_chat")
def historial_chat(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    res = supabase.table('prospectos').select('mensaje').eq('nombre', datos["nombre"]).eq('vendedor_id', _sesion).order('id', desc=False).execute()
    historial = []
    for fila in res.data:
        texto = fila['mensaje']
        es_mio = texto.startswith("TÚ: ")
        if es_mio: texto = texto.replace("TÚ: ", "", 1)
        historial.append({"texto": texto, "es_mio": es_mio})
    return {"historial": historial}

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'columna': datos.get("nueva_columna")}).eq('nombre', datos.get("nombre")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'notas': datos.get("notas"), 'etiquetas': datos.get("etiquetas")}).eq('nombre', datos.get("nombre")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').update({'columna': 'Papelera'}).eq('nombre', datos.get("nombre")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.post("/api/borrar_permanente")
def borrar_permanente(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('prospectos').delete().eq('nombre', datos.get("nombre")).eq('vendedor_id', _sesion).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.get("/api/buscar_maestro")
def buscar_maestro(q: str):
    try:
        return {"status": "ok", "resultados": supabase.table('catalogo_maestro').select('*').ilike('nombre', f'%{q}%').limit(10).execute().data}
    except Exception: return {"status": "error"}

@app.post("/api/inyectar_starter")
def inyectar_starter(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        maestros = supabase.table('catalogo_maestro').select('*').eq('starter_pack', True).execute()
        lote = [{"nombre": m["nombre"], "consola": m["consola"], "precio": m["precio_sugerido"], "costo": 0, "stock": 0, "estado_general": "Solo disco (Loose)", "codigo_barras": "", "vendedor_id": _sesion} for m in maestros.data]
        if lote: supabase.table('inventario').insert(lote).execute()
        return {"status": "ok", "inyectados": len(lote)}
    except Exception: return {"status": "error"}

@app.get("/api/descargar_plantilla")
def api_descargar_plantilla(vendedor_id_real: str = Depends(verificar_sesion_b2b)):
    try:
        items_maestros = supabase.table('catalogo_maestro').select('*').execute().data or []
        res_privado = supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id_real).execute()
        dict_privado = {item['nombre']: item for item in res_privado.data} if res_privado.data else {}

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['INSTRUCCIONES: No borrar filas 1 y 2.los Datos inician en fila 3...'])
        writer.writerow(["nombre", "consola", "costo", "precio", "stock", "estado_general", "detalles"])

        for m in items_maestros:
            nombre, consola = m['nombre'], m['consola']
            if nombre in dict_privado:
                inv = dict_privado[nombre]
                writer.writerow([nombre, consola, inv.get('costo', 0), inv.get('precio', 0), inv.get('stock', 0), inv.get('estado_general', 'Completo (CIB)'), inv.get('descripcion_detallada', '')])
            else:
                writer.writerow([nombre, consola, 0, m.get('precio_sugerido', 0), 0, "Completo (CIB)", ""])

        output.seek(0)
        return StreamingResponse(io.BytesIO(output.getvalue().encode("utf-8")), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=Plantilla_{vendedor_id_real}.csv"})
    except Exception as e: return {"status": "error", "detalle": str(e)}

# ==========================================
# 📦 INVENTARIO & DB (BLINDADO B2B + FANTASMAS)
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
        paquete_datos["sku_b2b"] = f"{nombre_limpio}_{consola_limpia}_{estado}".lower().replace(" ", "-").replace(":", "").replace("/", "")
        
        res = supabase.table('inventario').select('id').ilike('nombre', nombre_limpio).ilike('consola', consola_limpia).ilike('estado_general', estado).eq('vendedor_id', _sesion).execute()
        item_id = None
        
        if res.data and len(res.data) > 0:
            item_id = res.data[0]['id']
            supabase.table('inventario').update(paquete_datos).eq('id', item_id).execute()
        else:
            insert_res = supabase.table('inventario').insert(paquete_datos).execute()
            if insert_res.data: item_id = insert_res.data[0]['id']
            
        # 👻 TRABAJO FANTASMA: Mandamos cazar la portada sin detener el panel de Godot
        if item_id and not datos.url_portada:
            background_tasks.add_task(cazar_portada_y_guardar_background, str(item_id), nombre_limpio, consola_limpia)
            
        # 🛡️ FIX AAA: Alerta Radar con Token Dinámico
        res_alertas = supabase.table('alertas_mercado').select('*').ilike('juego', f"%{nombre_limpio}%").eq('activa', True).execute()
        if res_alertas.data:
            res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).execute()
            if res_config.data:
                config = res_config.data[0]
                admin_ph = config.get("admin_phone", ADMIN_PHONE_GLOBAL)
                for alerta in res_alertas.data:
                    if alerta['precio_maximo'] >= datos.precio and datos.precio > 0:
                        disparar_whatsapp_dinamico(admin_ph, f"🎯 *RADAR B2B*\nAlta:\n🎮 {datos.nombre}\n💰 ${datos.precio}", config['meta_token'], config['meta_phone_id'])

        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_item")
def borrar_item(datos: dict, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        supabase.table('inventario').delete().eq('vendedor_id', _sesion).eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

@app.get("/api/cargar_inventario")
def cargar_inventario(vendedor_id_real: str = Depends(verificar_sesion_b2b)):
    try:
        return {"status": "ok", "inventario": supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id_real).order('nombre', desc=False).execute().data}
    except Exception as e: return {"status": "error", "detalle": str(e)}

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
    except Exception: return {"status": "error"}

@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).eq('vendedor_id', _sesion).execute()
        if res.data: return {"status": "ok", "juego": res.data[0]}
        return {"status": "error"}
    except Exception: return {"status": "error"}

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
    except Exception: return {"status": "error"}

# ==========================================
# 🧠 MÓDULOS B2B E IA LIMPIADORA (CSV UPSERT + PORTADAS)
# ==========================================
@app.post("/api/importar_inventario")
def api_importar_inventario(datos: dict, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)):
    lote_juegos = datos.get("inventario", [])
    if not lote_juegos: return {"status": "error", "detalle": "CSV vacío."}

    consolas_oficiales = ["PS5", "PS4", "PS3", "PS2", "PS1", "Xbox One", "Xbox 360", "Xbox Clasico", "Nintendo Switch", "Nintendo 3DS", "Nintendo DS", "Nintendo 64", "GameCube", "GameBoy Advance", "GameBoy Color", "Wii", "Wii U", "SNES", "NES", "Genesis", "Otro (PC/Varios)"]
    mapa_estados = {"NUEVO": "Nuevo/Sellado", "SELLADO": "Nuevo/Sellado", "NUEVO/SELLADO": "Nuevo/Sellado", "COMPLETO": "Completo", "CIB": "Completo", "COMPLETO (CIB)": "Completo", "SIN LIBRITO": "Sin librito", "SIN MANUAL": "Sin librito", "SOLO DISCO": "Solo disco", "SUELTO": "Solo disco", "LOOSE": "Solo disco"}
    diccionario_sinonimos = {"PLAY 1": "PS1", "PLAY 2": "PS2", "PLAY 3": "PS3", "XBOX NORMAL": "Xbox Clasico", "SUPER NINTENDO": "SNES", "GB": "GameBoy Color"}

    try:
        res_maestro = supabase.table('catalogo_maestro').select('nombre, precio_sugerido').execute()
        nombres_maestros = [item['nombre'] for item in res_maestro.data]
        diccionario_precios = {item['nombre'].lower(): item['precio_sugerido'] for item in res_maestro.data}
    except Exception:
        nombres_maestros, diccionario_precios = [], {}

    def limpiar_campo(texto_usuario, lista_oficial):
        t_upper = str(texto_usuario).strip().upper()
        if t_upper in diccionario_sinonimos: return diccionario_sinonimos[t_upper]
        coincidencias = difflib.get_close_matches(str(texto_usuario).strip(), lista_oficial, n=1, cutoff=0.5)
        return coincidencias[0] if coincidencias else str(texto_usuario).strip()

    conteo_actualizados, conteo_nuevos, reporte_ia = 0, 0, []

    for juego in lote_juegos:
        nombre_original = str(juego.get("nombre", "")).strip()
        nombre_corregido = nombre_original.title() 
        precio_asignado = float(juego.get("precio", 0.0))
        
        if nombres_maestros and nombre_corregido not in nombres_maestros:
            matches = difflib.get_close_matches(nombre_corregido, nombres_maestros, n=1, cutoff=0.7)
            if matches: nombre_corregido = matches[0]
                
        consola_final = limpiar_campo(juego.get("consola", ""), consolas_oficiales)
        estado_final = mapa_estados.get(str(juego.get("estado_general", "")).strip().upper(), "Solo disco")

        if precio_asignado <= 0.0:
            nom_limpio = nombre_corregido.lower()
            if nom_limpio in diccionario_precios:
                precio_asignado = diccionario_precios[nom_limpio]
            else:
                try:
                    datos_pc = api_consultar_precio(nombre_corregido, consola_final, _sesion)
                    if datos_pc and datos_pc.get("status") == "ok":
                        precio_asignado = datos_pc["mxn"]["cib"] if estado_final in ["Completo", "Nuevo/Sellado"] else datos_pc["mxn"]["loose"]
                        if precio_asignado <= 0.0: precio_asignado = 0.0 
                except Exception: precio_asignado = 0.0

        rareza_final = calcular_rareza_ia(nombre_corregido, consola_final, precio_asignado)
        sku_b2b = f"{nombre_corregido}_{consola_final}_{estado_final}".lower().replace(" ", "-").replace(":", "").replace("/", "")

        paquete_datos = {
            "nombre": nombre_corregido, "consola": consola_final, "estado_general": estado_final,
            "precio": precio_asignado, "costo": float(juego.get("costo", 0.0)), "stock": int(juego.get("stock", 0)),
            "rareza": rareza_final, "sku_b2b": sku_b2b, "codigo_barras": str(juego.get("codigo_barras", "")),
            "vendedor_id": _sesion, "descripcion_detallada": str(juego.get("detalles", ""))
        }

        try:
            res_ex = supabase.table('inventario').select('id').eq('sku_b2b', sku_b2b).eq('vendedor_id', _sesion).execute()
            if res_ex.data and len(res_ex.data) > 0:
                supabase.table('inventario').update(paquete_datos).eq('id', res_ex.data[0]['id']).execute()
                conteo_actualizados += 1
            else:
                insert_res = supabase.table('inventario').insert(paquete_datos).execute()
                conteo_nuevos += 1
                # 👻 Disparamos cacería de portada solo a las piezas nuevas para no saturar al scraper
                if insert_res.data:
                    background_tasks.add_task(cazar_portada_y_guardar_background, str(insert_res.data[0]['id']), nombre_corregido, consola_final)

            if not supabase.table('catalogo_maestro').select('id').eq('nombre', nombre_corregido).eq('consola', consola_final).execute().data:
                supabase.table('catalogo_maestro').insert({"nombre": nombre_corregido, "consola": consola_final, "precio_sugerido": precio_asignado, "rareza": rareza_final}).execute()

        except Exception: pass

    return {"status": "ok", "insertados": conteo_nuevos, "actualizados": conteo_actualizados, "mensaje": f"Sincronización B2B exitosa."}

@app.get("/api/radar_b2b")
def radar_b2b(q: str = ""):
    try:
        query = supabase.table('inventario').select('nombre, consola, precio, estado_general, rareza, vendedor_id').gt('stock', 0)
        if q: query = query.ilike('nombre', f'%{q}%')
        return {"status": "ok", "resultados": query.limit(50).execute().data}
    except Exception: return {"status": "error", "detalle": "Falla en el Radar B2B"}

@app.get("/api/bot_config")
def obtener_config_bot(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = supabase.table('configuracion_bot').select('*').eq('vendedor_id', _sesion).execute()
        if res.data: return {"status": "ok", "datos": res.data[0]}
        return {"status": "error", "detalle": "Configuración no encontrada"}
    except Exception: return {"status": "error"}

@app.post("/api/bot_config")
def guardar_config_bot(datos: BotConfig, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        paquete = {"vendedor_id": _sesion, "link_pago": datos.link_pago, "texto_entrega": datos.texto_entrega, "admin_phone": datos.admin_phone, "bot_activo": datos.bot_activo}
        res_ex = supabase.table('configuracion_bot').select('vendedor_id').eq('vendedor_id', _sesion).execute()
        if res_ex.data: supabase.table('configuracion_bot').update(paquete).eq('vendedor_id', _sesion).execute()
        else: supabase.table('configuracion_bot').insert(paquete).execute()
        return {"status": "ok"}
    except Exception: return {"status": "error"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
