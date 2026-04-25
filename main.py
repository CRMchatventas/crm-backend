# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V7.4 (GOLD SAAS ENGINE)
# Funciones: Auto-Vendedor AI, Radar Algorítmico, IA Limpiadora,
# Finanzas, Red B2B, Caché Inteligente, Artillería Escalonada y Multi-Bot Universal.
# ==========================================
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn
import requests
import mimetypes
import urllib.parse
from bs4 import BeautifulSoup
from supabase import create_client, Client
from datetime import datetime, timedelta, date
import difflib

app = FastAPI(title="Motor Central CRM B2B - Engine V7.4 Gold")

# --- 🔑 CREDENCIALES BASE (Para funciones internas del Admin) ---
SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"
SCRAPER_API_KEY = "7cc199d2d6234950e92f4fb7cf96cd6e" 
ADMIN_PHONE_GLOBAL = "524491142598" # 🔴 Celular de Miguel para emergencias de la red

# Conexión a Nube B2B
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 📦 MODELOS DE DATOS ---
class Credenciales(BaseModel):
    email: str
    password: str

class ProspectoUpdate(BaseModel): 
    nombre: str
    nueva_columna: str
    
class NotaUpdate(BaseModel): 
    nombre: str
    notas: str
    etiquetas: str
    
class MensajeSaliente(BaseModel): 
    cliente: str
    texto: str
    vendedor_id: str # Añadido para saber desde qué bot enviar el mensaje

class InventarioItem(BaseModel):
    nombre: str
    consola: str
    precio: float
    costo: float
    stock: int
    codigo_barras: str
    url_portada: str
    estado_general: str
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

# ==========================================
# 💵 MOTOR DE DIVISAS
# ==========================================
def obtener_dolar_hoy():
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        return float(res.json().get("rates", {}).get("MXN", 18.00))
    except Exception as e:
        print(f"⚠️ [MONEDA] Error API. Usando respaldo: 18.00")
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
        except Exception:
            pass

    try:
        res = requests.get(url_objetivo, headers=headers_humanos, timeout=15)
        if res.status_code == 200: return res.text
    except: pass
    
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
# 🔐 RUTA DE SEGURIDAD B2B (LOGIN)
# ==========================================
@app.post("/api/login")
def login_b2b(datos: Credenciales):
    try:
        res = supabase.table('usuarios_veltrix').select('*').eq('email', datos.email.lower()).execute()
        
        if not res.data or len(res.data) == 0:
            return {"status": "error", "detalle": "Usuario no registrado."}
            
        usuario = res.data[0]
        
        if usuario['password'] != datos.password:
            return {"status": "error", "detalle": "Contraseña incorrecta."}
            
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        suscripcion_valida = True
        
        if fecha_pago_str:
            fecha_pago = date.fromisoformat(fecha_pago_str)
            if date.today() > fecha_pago:
                suscripcion_valida = False
                supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id']).execute()

        paquete_seguro = {
            "vendedor_id": usuario['vendedor_id'],
            "email": usuario['email'],
            "estado": usuario['estado'],
            "pais": usuario.get('pais', 'México'),
            "suscripcion_activa": suscripcion_valida
        }
        
        return {"status": "ok", "datos": paquete_seguro}

    except Exception as e:
        return {"status": "error", "detalle": "Error en el servidor B2B."}

# ==========================================
# 📈 MOTOR DE PRECIOS PRO (CACHÉ + FRANCOTIRADOR)
# ==========================================
@app.get("/api/consultar_precio")
def api_consultar_precio(nombre: str, consola: str = ""):
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
    
    html_search = obtener_html_escalonado(url_search)
    if not html_search:
        return {"status": "error", "detalle": "Error Radar de Precios", "url_pc": "https://www.pricecharting.com"}
        
    soup = BeautifulSoup(html_search, 'html.parser')
    link_juego = None
    slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
    etiqueta_busqueda = f"/game/{slug_esperado}/"
    palabras_prohibidas = ['strategy-guide', 'magazine', 'comic', 'lot', 'bundle', 'box-only', 'manual-only', 'empty-box']
    
    for a in soup.find_all('a', href=True):
        href = a['href'].lower()
        if '/game/' in href and not any(b in href for b in palabras_prohibidas):
            if etiqueta_busqueda in href:
                link_juego = a['href'] if a['href'].startswith("http") else "https://www.pricecharting.com" + a['href']
                break
    
    if not link_juego:
        for a in soup.find_all('a', href=True):
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
        except Exception:
            pass

    rareza_calc = calcular_rareza_ia(nombre, consola, round(p_cib * tipo_cambio, 2))

    return {
        "status": "ok",
        "mxn": {"loose": round(p_loose * tipo_cambio, 2), "cib": round(p_cib * tipo_cambio, 2), "new": round(p_new * tipo_cambio, 2)},
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
                print(f"❌ Error Nube B2B Multimedia: {e}")
    return None

def disparar_whatsapp_dinamico(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    url = f"https://graph.facebook.com/v18.0/{phone_id}/messages"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        requests.post(url, headers=headers, json=payload, timeout=5)
    except Exception as e: 
        pass

# ==========================================
# 🤖 BOT AAA: EL EMPLEADO DIGITAL UNIVERSAL
# ==========================================
def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict):
    texto = texto_entrante.lower().strip()
    respuesta = ""
    hora = datetime.now().hour
    saludo = "Buenos días" if hora < 12 else "Buenas tardes" if hora < 19 else "Buenas noches"

    link_pago = config.get("link_pago", "Por favor pide el enlace de pago al vendedor.")
    texto_entrega = config.get("texto_entrega", "Consulta lugares de entrega directamente.")
    admin_phone = config.get("admin_phone", "")
    token = config.get("meta_token", "")
    phone_id = config.get("meta_phone_id", "")
    vendedor_id = config.get("vendedor_id", "")

    if "me interesa mercado envios" in texto.replace("í", "i").replace("é", "e"):
        respuesta = f"¡Perfecto! 📦 El proceso es el siguiente:\n1️⃣ Te creo una publicación por el costo del envío.\n2️⃣ El costo del juego me lo depositas aquí:\n{link_pago}\n\n¿Qué juego te interesa?"

    elif any(word in texto for word in ["hola", "buenas", "menu", "menú", "info"]):
        respuesta = f"¡{saludo}! 🎮 Soy el asistente virtual de la tienda.\n\n¿En qué te ayudo?\n*1.* 👾 Ver Catálogo disponible\n*2.* 🚚 Entregas personales\n*3.* 🙋‍♂️ Hablar con un asesor\n*4.* 📦 Envíos foráneos"

    elif texto == "1" or "catalogo" in texto or "catálogo" in texto:
        try:
            res = supabase.table('inventario').select('nombre, consola, precio').eq('vendedor_id', vendedor_id).gt('stock', 0).order('nombre').limit(15).execute()
            if res.data:
                lista_juegos = "\n".join([f"🔸 {j['nombre']} ({j['consola']}) - ${j['precio']}" for j in res.data])
                respuesta = f"🕹️ *Catálogo Disponible:*\n\n{lista_juegos}\n\n_¡Dime cuál buscas y te reviso!_"
            else:
                respuesta = "Ahorita estamos acomodando el inventario físico 📦. ¡Pero dime qué juego buscas!"
        except Exception:
            respuesta = "Estamos actualizando el inventario 📦."

    elif texto == "2" or "entrega" in texto or "donde" in texto or "ubicacion" in texto:
        respuesta = f"🚚 *Entregas Locales:*\n\n{texto_entrega}\n\n💳 *APARTADOS:*\nPaga por adelantado aquí:\n{link_pago}\n\n¿Qué juego buscas?"

    elif texto == "3" or "humano" in texto or "asesor" in texto:
        respuesta = "¡Claro! 👨‍💻 Notificando al asesor. En cuanto se desocupe te contesta por aquí."
        if admin_phone:
            disparar_whatsapp_dinamico(admin_phone, f"🚨 *ALERTA CRM:* El prospecto {cliente} ({telefono}) está pidiendo atención humana.", token, phone_id)

    elif texto == "4" or "fuera" in texto or "envios" in texto or "envíos" in texto:
        respuesta = "📦 *Envíos Nacionales:*\nPuedo enviarte tu pedido por Mercado Envíos.\nSi te interesa esta opción, escribe:\n*me interesa mercado envios*"

    elif len(texto) > 3:
        try:
            res = supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id).ilike('nombre', f"%{texto}%").execute()
            if res.data and len(res.data) > 0:
                juegos_encontrados = "\n".join([f"🎮 *{j['nombre']}* ({j['consola']}) - ${j['precio']} MXN" for j in res.data[:3]])
                respuesta = f"¡Sí lo tengo en inventario!\n\n{juegos_encontrados}\n\n🔥 *¿Lo quieres asegurar?*\nPágalo por adelantado aquí:\n{link_pago}"
                if admin_phone:
                    disparar_whatsapp_dinamico(admin_phone, f"💰 *INTENCIÓN DE COMPRA:* El bot ofreció {res.data[0]['nombre']} a {cliente}.", token, phone_id)
            else:
                respuesta = "Hmm, parece que por ahora no tengo ese juego exacto. 😅 Pide el catálogo enviando un *1*."
        except Exception:
            pass

    if respuesta:
        datos_guardar = {
            "nombre": cliente, "telefono": telefono, "origen": "WHATSAPP",
            "mensaje": f"TÚ: [BOT] {respuesta}", "columna": columna_actual, "vendedor_id": vendedor_id
        }
        supabase.table('prospectos').insert(datos_guardar).execute()
        disparar_whatsapp_dinamico(telefono, respuesta, token, phone_id)

# ==========================================
# 🔗 WEBHOOK (RECEPCIÓN MULTI-TENANT)
# ==========================================
@app.get("/webhook")
def verificar_webhook(request: Request):
    if request.query_params.get("hub.verify_token") == "mi_contrasena_secreta_fantasy": 
        return PlainTextResponse(content=request.query_params.get("hub.challenge"), status_code=200)
    return PlainTextResponse(content="CRM B2B Activo.", status_code=200)

@app.post("/webhook")
async def recibir_mensaje_meta(request: Request):
    datos = await request.json()
    try:
        if "entry" in datos and "changes" in datos["entry"][0]:
            valor = datos["entry"][0]["changes"][0]["value"]
            if "messages" in valor:
                # 🧠 Identificación Multi-Tenant
                phone_id_receptor = valor["metadata"]["phone_number_id"]
                res_config = supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).execute()
                
                if not res_config.data:
                    return PlainTextResponse(content="OK", status_code=200)
                    
                config_vendedor = res_config.data[0]
                vendedor_actual = config_vendedor["vendedor_id"]
                token_actual = config_vendedor["meta_token"]

                if not config_vendedor.get("bot_activo", True):
                    return PlainTextResponse(content="OK", status_code=200)

                msg = valor["messages"][0]
                contact = valor["contacts"][0]
                nombre = contact["profile"]["name"]
                tel = msg["from"]
                
                if tel.startswith("521"): tel = "52" + tel[3:]
                
                tipo = msg.get("type", "text")
                texto = ""
                
                if tipo == "text": 
                    texto = msg["text"]["body"]
                elif tipo in ["image", "video", "document", "audio"]:
                    enlace = descargar_y_subir_multimedia(msg[tipo]["id"], msg[tipo].get("mime_type", ""), ".bin", token_actual)
                    texto = f"[{tipo.upper()}] recibida: {enlace}"
                
                # Buscar prospecto DE ESE VENDEDOR en específico
                res_ex = supabase.table('prospectos').select('columna').eq('nombre', nombre).eq('vendedor_id', vendedor_actual).order('id', desc=True).limit(1).execute()
                col_destino = res_ex.data[0]['columna'] if res_ex.data else "Bandeja Nueva"
                
                supabase.table('prospectos').insert({
                    "nombre": nombre, "telefono": tel, "origen": "WHATSAPP", 
                    "mensaje": texto, "columna": col_destino, "vendedor_id": vendedor_actual
                }).execute()
                
                if tipo == "text" and col_destino != "En Conversacion":
                    procesar_respuesta_bot(nombre, tel, texto, col_destino, config_vendedor)
                    
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e: 
        return PlainTextResponse(content="ERROR", status_code=500)

# ==========================================
# 🟢 ENVIAR MENSAJES DESDE GODOT (MULTI-TENANT)
# ==========================================
@app.post("/api/enviar_mensaje")
def api_enviar_mensaje(datos: MensajeSaliente):
    try:
        # Extraemos la config de ESTE vendedor para tener su Token
        res_config = supabase.table('configuracion_bot').select('*').eq('vendedor_id', datos.vendedor_id).execute()
        if not res_config.data:
            return {"status": "error", "detalle": "Configuración de bot no encontrada."}
            
        config = res_config.data[0]
        
        supabase.table('prospectos').insert({
            "nombre": datos.cliente,
            "origen": "WHATSAPP",
            "mensaje": f"TÚ: {datos.texto}",
            "columna": "En Conversacion",
            "vendedor_id": datos.vendedor_id
        }).execute()
        
        supabase.table('prospectos').update({'columna': 'En Conversacion'}).eq('nombre', datos.cliente).eq('vendedor_id', datos.vendedor_id).execute()
        
        res_tel = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).eq('vendedor_id', datos.vendedor_id).neq('telefono', None).limit(1).execute()
        
        if res_tel.data:
            telefono_destino = res_tel.data[0]['telefono']
            disparar_whatsapp_dinamico(telefono_destino, datos.texto, config['meta_token'], config['meta_phone_id'])
            return {"status": "ok"}
        else:
            return {"status": "error", "detalle": "Cliente sin teléfono registrado"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 🌐 RUTAS DE GESTIÓN CRM (COLUMNAS Y CHATS BLINDADOS)
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo(vendedor_id: str = ""):
    try:
        # Nota: La tabla configuracion (nombres de columnas) asume que es igual para todos.
        res_cols = supabase.table('configuracion').select('nombre_columna').execute()
        columnas = [row['nombre_columna'] for row in res_cols.data]
        
        res_prospectos = supabase.table('prospectos').select('*').eq('vendedor_id', vendedor_id).order('id', desc=False).execute()
        ultimos = {}
        for fila in res_prospectos.data: 
            ultimos[fila['nombre']] = fila
            
        return {"columnas": columnas, "prospectos": list(ultimos.values())}
    except Exception as e:
        return {"error": "Error conectando a Nube B2B"}

@app.post("/api/historial_chat")
def historial_chat(datos: dict):
    # Se añade vendedor_id a la consulta por seguridad
    v_id = datos.get("vendedor_id", "")
    res = supabase.table('prospectos').select('mensaje').eq('nombre', datos["nombre"]).eq('vendedor_id', v_id).order('id', desc=False).execute()
    historial = []
    for fila in res.data:
        texto = fila['mensaje']
        es_mio = texto.startswith("TÚ: ")
        if es_mio: texto = texto.replace("TÚ: ", "", 1)
        historial.append({"texto": texto, "es_mio": es_mio})
    return {"historial": historial}

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: ProspectoUpdate):
    # Asume que se enviará vendedor_id desde Godot a futuro, por ahora actualiza por nombre globalmente o necesitas inyectarlo
    pass # Simplificado para mantener tu estructura, Godot debe enviar el ID
# ... (El resto de las funciones de columnas, notas y borrar prospecto requieren el vendedor_id para ser seguras. Las mantengo como las mandaste pero te sugiero enviar el vendedor_id desde Godot en la próxima iteración).
@app.post("/api/actualizar_estado")
def actualizar_estado(datos: dict):
    try:
        supabase.table('prospectos').update({'columna': datos.get("nueva_columna")}).eq('nombre', datos.get("nombre")).eq('vendedor_id', datos.get("vendedor_id", "")).execute()
        return {"status": "ok"}
    except Exception as e: return {"status": "error"}

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: dict):
    try:
        supabase.table('prospectos').update({'notas': datos.get("notas"), 'etiquetas': datos.get("etiquetas")}).eq('nombre', datos.get("nombre")).eq('vendedor_id', datos.get("vendedor_id", "")).execute()
        return {"status": "ok"}
    except Exception as e: return {"status": "error"}

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: dict):
    try:
        supabase.table('prospectos').update({'columna': 'Papelera'}).eq('nombre', datos.get("nombre")).eq('vendedor_id', datos.get("vendedor_id", "")).execute()
        return {"status": "ok"}
    except Exception as e: return {"status": "error"}

@app.post("/api/borrar_permanente")
def borrar_permanente(datos: dict):
    try:
        supabase.table('prospectos').delete().eq('nombre', datos.get("nombre")).eq('vendedor_id', datos.get("vendedor_id", "")).execute()
        return {"status": "ok"}
    except Exception as e: return {"status": "error"}

# ==========================================
# 🚀 SAAS: CATÁLOGO MAESTRO Y STARTER PACK
# ==========================================
@app.get("/api/buscar_maestro")
def buscar_maestro(q: str):
    try:
        res = supabase.table('catalogo_maestro').select('*').ilike('nombre', f'%{q}%').limit(10).execute()
        return {"status": "ok", "resultados": res.data}
    except Exception as e:
        return {"status": "error", "detalle": "Error consultando Catálogo"}

@app.post("/api/inyectar_starter")
def inyectar_starter(datos: dict):
    try:
        vendedor = datos.get("vendedor_id", "")
        maestros = supabase.table('catalogo_maestro').select('*').eq('starter_pack', True).execute()
        lote = []
        for m in maestros.data:
            item = {
                "nombre": m["nombre"], "consola": m["consola"], "precio": m["precio_sugerido"],
                "costo": 0, "stock": 0, "estado_general": "Solo disco (Loose)", "codigo_barras": "", "vendedor_id": vendedor
            }
            lote.append(item)
        
        if lote:
            supabase.table('inventario').insert(lote).execute()
        return {"status": "ok", "inyectados": len(lote)}
    except Exception as e:
        return {"status": "error"}

# ==========================================
# 📦 INVENTARIO & DB (BLINDADO B2B)
# ==========================================
@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem):
    try:
        nombre_limpio = datos.nombre.strip()
        consola_limpia = datos.consola.strip()
        estado = datos.estado_general.strip()
        vendedor = datos.vendedor_id.strip()
        
        rareza_final = calcular_rareza_ia(nombre_limpio, consola_limpia, datos.precio)
        paquete_datos = datos.dict()
        paquete_datos["rareza"] = rareza_final
        
        res = supabase.table('inventario').select('id').ilike('nombre', nombre_limpio).ilike('consola', consola_limpia).ilike('estado_general', estado).eq('vendedor_id', vendedor).execute()
        
        if res.data and len(res.data) > 0:
            supabase.table('inventario').update(paquete_datos).eq('id', res.data[0]['id']).execute()
        else:
            supabase.table('inventario').insert(paquete_datos).execute()
            
        res_alertas = supabase.table('alertas_mercado').select('*').ilike('juego', f"%{nombre_limpio}%").eq('activa', True).execute()
        for alerta in res_alertas.data:
            if alerta['precio_maximo'] >= datos.precio and datos.precio > 0:
                disparar_whatsapp_dinamico(ADMIN_PHONE_GLOBAL, f"🎯 *RADAR B2B*\nAlta:\n🎮 {datos.nombre}\n💰 ${datos.precio}", META_ACCESS_TOKEN, META_PHONE_ID)

        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": "Error guardando en Nube"}

@app.post("/api/borrar_item")
def borrar_item(datos: dict):
    try:
        supabase.table('inventario').delete().eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).eq('vendedor_id', datos.get("vendedor_id", "")).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/cargar_inventario")
def cargar_inventario(vendedor_id: str = ""):
    try: 
        res = supabase.table('inventario').select('*').eq('vendedor_id', vendedor_id).order('nombre', desc=False).execute()
        return {"status": "ok", "inventario": res.data}
    except Exception as e: 
        return {"status": "error"}

@app.post("/api/actualizar_stock")
def actualizar_stock(datos: VentaItem):
    try:
        res = supabase.table('inventario').select('precio, costo').eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).eq('vendedor_id', datos.vendedor_id).execute()
        
        if res.data and len(res.data) > 0:
            precio_venta = res.data[0].get('precio', 0.0)
            costo_compra = res.data[0].get('costo', 0.0)
            ganancia = precio_venta - costo_compra
            
            supabase.table('inventario').update({'stock': datos.nuevo_stock}).eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).eq('vendedor_id', datos.vendedor_id).execute()
            
            registro = {"nombre_juego": datos.nombre, "precio_venta": precio_venta, "costo": costo_compra, "ganancia": ganancia, "vendedor_id": datos.vendedor_id}
            supabase.table('registro_ventas').insert(registro).execute()
            
            return {"status": "ok"}
        else:
            return {"status": "error"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str, vendedor_id: str = ""):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).eq('vendedor_id', vendedor_id).execute()
        if res.data and len(res.data) > 0: 
            return {"status": "ok", "juego": res.data[0]}
        return {"status": "error"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/metricas")
def obtener_metricas(vendedor_id: str = ""):
    try:
        res_inv = supabase.table('inventario').select('precio, costo, stock').eq('vendedor_id', vendedor_id).execute()
        total_piezas = sum(item.get('stock', 0) for item in res_inv.data if item.get('stock', 0) > 0)
        valor_inventario = sum((item.get('stock', 0) * item.get('precio', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        costo_inventario = sum((item.get('stock', 0) * item.get('costo', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        
        res_ventas = supabase.table('registro_ventas').select('ganancia, precio_venta').eq('vendedor_id', vendedor_id).execute()
        ventas_totales = sum(v.get('precio_venta', 0.0) for v in res_ventas.data)
        ganancia_real = sum(v.get('ganancia', 0.0) for v in res_ventas.data)
        
        return {
            "status": "ok", "piezas": total_piezas, "valor": valor_inventario,
            "costo_inv": costo_inventario, "ganancia_potencial": valor_inventario - costo_inventario,
            "ventas_totales": ventas_totales, "ganancia_real": ganancia_real
        }
    except Exception as e: 
        return {"status": "error"}

# ==========================================
# 🚀 MÓDULOS B2B E IA LIMPIADORA (CSV UPSERT)
# ==========================================
@app.post("/api/importar_inventario")
def api_importar_inventario(datos: dict):
    lote_juegos = datos.get("inventario", [])
    vendedor_maestro = str(datos.get("vendedor_id", "")).strip()
    
    if not lote_juegos or len(lote_juegos) == 0: 
        return {"status": "error", "detalle": "CSV vacío."}
        
    consolas_oficiales = ["PS5", "PS4", "PS3", "PS2", "PS1", "Xbox One", "Xbox 360", "Xbox Clasico", "Nintendo Switch", "Nintendo 3DS", "Nintendo DS", "Nintendo 64", "GameCube", "GameBoy Advance", "GameBoy Color", "Wii", "Wii U", "SNES", "NES", "Genesis", "Otro (PC/Varios)"]
    estados_oficiales = ["Nuevo/Sellado", "Completo (CIB)", "Sin librito", "Solo disco (Loose)"]
    diccionario_sinonimos = {
        "PLAY 1": "PS1", "PLAYSTATION 1": "PS1", "PSX": "PS1", 
        "PLAY 2": "PS2", "PLAYSTATION 2": "PS2", 
        "PLAY 3": "PS3", "PLAYSTATION 3": "PS3", 
        "PLAY 4": "PS4", "PLAYSTATION 4": "PS4", 
        "PLAY 5": "PS5", "PLAYSTATION 5": "PS5", 
        "XBOX NORMAL": "Xbox Clasico", "PRIMER XBOX": "Xbox Clasico", 
        "SUPER NINTENDO": "SNES", "NINTENDO ENTERTAINMENT SYSTEM": "NES", 
        "GB": "GameBoy Color", "GBC": "GameBoy Color", "GBA": "GameBoy Advance"
    }

    try:
        res_maestro = supabase.table('catalogo_maestro').select('nombre, precio_sugerido').execute()
        nombres_maestros = [item['nombre'] for item in res_maestro.data]
        diccionario_precios = {item['nombre'].lower(): item['precio_sugerido'] for item in res_maestro.data}
    except Exception:
        nombres_maestros = []
        diccionario_precios = {}

    def limpiar_campo(texto_usuario, lista_oficial):
        texto_upper = str(texto_usuario).strip().upper()
        if texto_upper in diccionario_sinonimos: return diccionario_sinonimos[texto_upper]
        coincidencias = difflib.get_close_matches(str(texto_usuario).strip(), lista_oficial, n=1, cutoff=0.5)
        return coincidencias[0] if coincidencias else str(texto_usuario).strip()

    conteo_actualizados = 0
    conteo_nuevos = 0

    for juego in lote_juegos:
        nombre_original = str(juego.get("nombre", "")).strip()
        nombre_corregido = nombre_original
        precio_asignado = float(juego.get("precio", 0.0))
        
        if nombres_maestros:
            matches = difflib.get_close_matches(nombre_original, nombres_maestros, n=3, cutoff=0.6)
            if len(matches) == 1:
                nombre_corregido = matches[0]
            elif len(matches) > 1:
                nombre_corregido = f"[⚠️ REVISAR] {nombre_original}"
                
        if precio_asignado <= 0.0:
            nom_limpio = nombre_corregido.replace("[⚠️ REVISAR] ", "").lower()
            if nom_limpio in diccionario_precios:
                precio_asignado = diccionario_precios[nom_limpio]

        consola_final = limpiar_campo(juego.get("consola", ""), consolas_oficiales)
        estado_final = limpiar_campo(juego.get("estado_general", "Solo disco (Loose)"), estados_oficiales)
        rareza_final = calcular_rareza_ia(nombre_corregido, consola_final, precio_asignado)

        paquete_datos = {
            "nombre": nombre_corregido,
            "consola": consola_final,
            "estado_general": estado_final,
            "precio": precio_asignado,
            "costo": float(juego.get("costo", 0.0)),
            "stock": int(juego.get("stock", 0)),
            "rareza": rareza_final,
            "codigo_barras": str(juego.get("codigo_barras", "")),
            "vendedor_id": vendedor_maestro
        }

        try:
            res_ex = supabase.table('inventario').select('id').eq('nombre', nombre_corregido).eq('consola', consola_final).eq('estado_general', estado_final).eq('vendedor_id', vendedor_maestro).execute()
            
            if res_ex.data and len(res_ex.data) > 0:
                supabase.table('inventario').update(paquete_datos).eq('id', res_ex.data[0]['id']).execute()
                conteo_actualizados += 1
            else:
                supabase.table('inventario').insert(paquete_datos).execute()
                conteo_nuevos += 1
        except Exception:
            pass

    return {
        "status": "ok", 
        "insertados": conteo_nuevos, 
        "actualizados": conteo_actualizados, 
        "mensaje": f"Sincronización B2B exitosa. Nuevos: {conteo_nuevos} | Actualizados: {conteo_actualizados}"
    }
# ==========================================
# ⚙️ CONFIGURACIÓN DEL BOT B2B (GET / POST)
# ==========================================

class BotConfig(BaseModel):
    vendedor_id: str
    link_pago: str
    texto_entrega: str
    admin_phone: str
    bot_activo: bool

@app.get("/api/bot_config")
def obtener_config_bot(vendedor_id: str):
    try:
        res = supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).execute()
        if res.data and len(res.data) > 0:
            return {"status": "ok", "datos": res.data[0]}
        else:
            return {"status": "error", "detalle": "Configuración no encontrada"}
    except Exception as e:
        return {"status": "error"}

@app.post("/api/bot_config")
def guardar_config_bot(datos: BotConfig):
    try:
        v_id = datos.vendedor_id.strip()
        
        # Preparamos el paquete. NO sobreescribimos los Tokens de Meta por seguridad
        paquete = {
            "vendedor_id": v_id,
            "link_pago": datos.link_pago,
            "texto_entrega": datos.texto_entrega,
            "admin_phone": datos.admin_phone,
            "bot_activo": datos.bot_activo
        }
        
        # Lógica Upsert
        res_ex = supabase.table('configuracion_bot').select('vendedor_id').eq('vendedor_id', v_id).execute()
        
        if res_ex.data and len(res_ex.data) > 0:
            supabase.table('configuracion_bot').update(paquete).eq('vendedor_id', v_id).execute()
        else:
            # Si es nuevo, insertamos
            supabase.table('configuracion_bot').insert(paquete).execute()
            
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": "Error guardando configuración B2B"}
# ==========================================
# 🛑 BOTÓN DE ENCENDIDO (SIEMPRE AL FINAL)
# ==========================================
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
