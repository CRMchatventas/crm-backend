# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V7.4 (GOLD SAAS ENGINE)
# Funciones: Auto-Vendedor AI, Radar Algorítmico, IA Limpiadora,
# Finanzas, Red B2B, Caché Inteligente, Artillería Escalonada y Blindaje Total.
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
from datetime import datetime, timedelta
import difflib

app = FastAPI(title="Motor Central CRM B2B - Engine V7.4 Gold")

# --- 🔑 CREDENCIALES Y CONFIGURACIÓN DEL BOT ---
META_ACCESS_TOKEN = "EAAQeucaUBYoBRIo9TZA0WoZBhQbqNuSKDdfqPeMKPJnASZBUYRuXL4oZACZC80DrmZCi1jrRvWpFsfwM5gr7AluJOBaJuhox5CZA4ZCjG6VrQqAbIyrX8YQFxhgjjyejPKUrrmMZAzvajWDRrCRJ0VZBFwU47ETnG6Xq7qzybeRZASKoRXdSLmS24JLQW0Vfiwqdi7KkgZDZD"
META_PHONE_ID = "975963255609853" # ID de Teléfono
SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"
SCRAPER_API_KEY = "7cc199d2d6234950e92f4fb7cf96cd6e" 

# 🤖 CONFIGURACIÓN DE TU EMPLEADO DIGITAL
ADMIN_PHONE = "524491142598" # 🔴 Celular Admin
LINK_MERCADOPAGO = "https://link.mercadopago.com.mx/fantasygamesags" 

# Conexión a Nube B2B
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 📦 MODELOS DE DATOS ---
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

class InventarioItem(BaseModel):
    nombre: str
    consola: str
    precio: float
    costo: float
    stock: int
    codigo_barras: str
    url_portada: str
    estado_general: str
    tiene_caja: bool
    tiene_manual: bool
    es_portada_original: bool
    descripcion_detallada: str

class VentaItem(BaseModel):
    nombre: str
    consola: str
    estado_general: str = ""
    nuevo_stock: int

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
        ("🟢 Artillería Ligera (1 Crédito)", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("🟡 Artillería Media (5 Créditos)", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true"),
        ("🔴 Artillería Pesada (25 Créditos)", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&premium=true&render=true")
    ]
    
    headers_humanos = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9"
    }

    for nombre_nivel, url_scraper in estrategias:
        print(f"🚀 [SCRAPER] Intentando: {nombre_nivel}...")
        try:
            res = requests.get(url_scraper, timeout=45)
            if res.status_code == 200 and "scraperapi" not in res.text.lower():
                if "pricecharting" in res.text.lower() or "price" in res.text.lower():
                    print(f"✔️ [ÉXITO] Escudo roto usando {nombre_nivel}.")
                    return res.text
                else:
                    print(f"⚠️ [FALLO] Página bloqueada por Cloudflare. Subiendo nivel...")
            else:
                print(f"❌ [ERROR] ScraperAPI rechazó la conexión. Subiendo nivel...")
        except Exception as e:
            print(f"🔥 [CRASH] Error en {nombre_nivel}: {e}")

    print("💀 [FATAL] Todos los niveles fallaron. Intentando acceso directo humano...")
    try:
        res = requests.get(url_objetivo, headers=headers_humanos, timeout=15)
        if res.status_code == 200: return res.text
    except: pass
    
    return ""

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
                print(f"🧠 [CACHÉ B2B] Precio recuperado GRATIS de la nube para: {nombre}")
                return {
                    "status": "ok",
                    "mxn": {"loose": round(datos_cache['loose'] * tipo_cambio, 2), "cib": round(datos_cache['cib'] * tipo_cambio, 2), "new": round(datos_cache['new'] * tipo_cambio, 2)},
                    "usd": {"loose": datos_cache['loose'], "cib": datos_cache['cib'], "new": datos_cache['new']},
                    "tipo_cambio": tipo_cambio,
                    "url_pc": datos_cache['url_pc']
                }
    except Exception as e:
        print(f"⚠️ [CACHÉ] Tabla no detectada o error. Pasando a Web: {e}")

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
        return {"status": "error", "detalle": "Error conectando al Radar de Precios", "url_pc": "https://www.pricecharting.com"}
        
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
            print("💾 [CACHÉ B2B] Nuevo precio guardado en Nube con éxito.")
        except Exception as e:
            print(f"⚠️ [CACHÉ B2B] Error guardando en BD: {e}")

    return {
        "status": "ok",
        "mxn": {"loose": round(p_loose * tipo_cambio, 2), "cib": round(p_cib * tipo_cambio, 2), "new": round(p_new * tipo_cambio, 2)},
        "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
        "tipo_cambio": tipo_cambio,
        "url_pc": url_final_pc
    }

# ==========================================
# 📥 MOTOR MULTIMEDIA & WHATSAPP ALARMAS
# ==========================================
def descargar_y_subir_multimedia(media_id: str, mime_type: str, extension_default: str):
    url_info = f"https://graph.facebook.com/v18.0/{media_id}"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}"}
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
                print(f"❌ Error Nube B2B: {e}")
    return None

def disparar_whatsapp_real(telefono_destino: str, texto_mensaje: str):
    url = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        res = requests.post(url, headers=headers, json=payload)
        # 🚨 ALARMA DE DIAGNÓSTICO PARA RENDER
        if res.status_code != 200:
            print(f"🔥 ERROR FATAL META AL ENVIAR A {telefono_destino}: HTTP {res.status_code} - {res.text}")
        else:
            print(f"✅ MENSAJE ENVIADO CORRECTAMENTE A {telefono_destino}")
    except Exception as e: 
        print(f"❌ Error Gateway API: {e}")

# ==========================================
# 🤖 BOT AAA: EL EMPLEADO DIGITAL 24/7
# ==========================================
def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str):
    print(f"🤖 [BOT] Analizando texto de {cliente}: '{texto_entrante}'")
    texto = texto_entrante.lower().strip()
    respuesta = ""
    hora = datetime.now().hour
    saludo = "Buenos días" if hora < 12 else "Buenas tardes" if hora < 19 else "Buenas noches"

    if "me interesa mercado envios" in texto.replace("í", "i").replace("é", "e"):
        respuesta = (
            "¡Perfecto! 📦 El proceso es el siguiente:\n\n"
            "1️⃣ Te creo una publicación en Mercado Libre por $300 pesos. Te mando el link y lo pagas ahí (esto cubre la guía de envío y el seguro).\n"
            "2️⃣ El restante del costo del juego me lo depositas directo a mi cuenta en este link:\n"
            f"{LINK_MERCADOPAGO}\n\n"
            "*(Ejemplo: Si el juego vale $500 y el envío $250, el total es $750. Pagas $300 en ML y me depositas $450).*\n\n"
            "¿Qué juego te interesa comprar?"
        )

    elif any(word in texto for word in ["hola", "buenas", "menu", "menú", "info"]):
        respuesta = f"¡{saludo}! 🎮 Soy el asistente virtual de Fantasy Games.\n\n¿En qué te ayudo?\n*1.* 👾 Ver Catálogo disponible\n*2.* 🚚 Entregas personales (Local)\n*3.* 🙋‍♂️ Hablar con Miguel\n*4.* 📦 Envíos fuera de Aguascalientes\n\n_O dime el nombre del juego que buscas y reviso si hay disponibilidad._"

    elif texto == "1" or "catalogo" in texto or "catálogo" in texto:
        try:
            res = supabase.table('inventario').select('nombre, consola, precio').gt('stock', 0).order('nombre').limit(15).execute()
            if res.data:
                lista_juegos = "\n".join([f"🔸 {j['nombre']} ({j['consola']}) - ${j['precio']}" for j in res.data])
                respuesta = f"🕹️ *Catálogo Fantasy Games:*\n\n{lista_juegos}\n\n_¡Y muchos más! Dime cuál buscas y te reviso._"
            else:
                respuesta = "Ahorita estamos acomodando el inventario físico 📦. ¡Pero dime qué juego buscas y te lo reviso de inmediato!"
        except Exception:
            respuesta = "Estamos actualizando el inventario 📦. ¡Dime qué juego buscas!"

    elif texto == "2" or "entrega local" in texto or "donde entregas" in texto or "ubicacion" in texto:
        respuesta = (
            "🚚 *Entregas en Aguascalientes:*\n\n"
            "• *Efectivo:* Te lo entrego en Paseos de Aguascalientes el dia y hora que gustes.\n"
            "• *Altaria:* Solamente miércoles y viernes por las tardes(previo depósito ó apartado).\n"
            "• 📦 *¿Compra mayor a $1,000?* Te lo entrego a domicilio cualquier día (previo depósito).\n\n"
            f"💳 *APARTADOS:*\nPaga por adelantado aquí y te lo aparto:\n{LINK_MERCADOPAGO}\n\n¿Qué juego buscas?"
        )

    elif texto == "3" or "humano" in texto or "miguel" in texto or "asesor" in texto:
        respuesta = "¡Claro! 👨‍💻 Le estoy mandando una notificación a Miguel. En cuanto se desocupe te contesta por aquí mismo."
        disparar_whatsapp_real(ADMIN_PHONE, f"🚨 *ALERTA CRM:* El prospecto {cliente} ({telefono}) está pidiendo atención humana.")

    elif texto == "4" or "fuera" in texto or "envios" in texto or "envíos" in texto or "foraneo" in texto:
        respuesta = (
            "📦 *Envíos Nacionales:*\nSi eres de fuera de Aguascalientes, puedo enviarte tu pedido por Mercado Envíos. "
            "Es mucho más seguro y te cuesta aproximadamente $250 pesos.\n\n"
            "Si te interesa esta opción, por favor escríbeme exactamente la frase:\n*me interesa mercado envios*"
        )

    elif len(texto) > 3:
        try:
            res = supabase.table('inventario').select('*').ilike('nombre', f"%{texto}%").execute()
            if res.data and len(res.data) > 0:
                juegos_encontrados = "\n".join([f"🎮 *{j['nombre']}* ({j['consola']}) - ${j['precio']} MXN" for j in res.data[:3]])
                respuesta = (
                    f"¡Sí lo tengo en inventario!\n\n{juegos_encontrados}\n\n"
                    f"🔥 *¿Lo quieres asegurar?*\nPágalo por adelantado y te lo aparto ahora mismo para entregártelo:\n{LINK_MERCADOPAGO}\n\n_(Mándame captura de tu pago por aquí)_"
                )
                disparar_whatsapp_real(ADMIN_PHONE, f"💰 *INTENCIÓN DE COMPRA:*\nEl bot le acaba de ofrecer {res.data[0]['nombre']} a {cliente}.")
            else:
                respuesta = "Hmm, parece que por ahora no tengo ese juego exacto en sistema. 😅\nPuedes checar el catálogo mandando un *1*, o pide hablar con Miguel mandando un *3*."
        except Exception:
            pass

    if respuesta:
        datos_guardar = {
            "nombre": cliente,
            "telefono": telefono,
            "origen": "WHATSAPP",
            "mensaje": f"TÚ: [BOT] {respuesta}",
            "columna": columna_actual 
        }
        supabase.table('prospectos').insert(datos_guardar).execute()
        disparar_whatsapp_real(telefono, respuesta)

# ==========================================
# 🌐 RUTAS DE GESTIÓN CRM (COLUMNAS Y CHATS)
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo():
    try:
        res_cols = supabase.table('configuracion').select('nombre_columna').execute()
        columnas = [row['nombre_columna'] for row in res_cols.data]
        
        res_prospectos = supabase.table('prospectos').select('*').order('id', desc=False).execute()
        ultimos = {}
        for fila in res_prospectos.data: 
            ultimos[fila['nombre']] = fila
            
        return {"columnas": columnas, "prospectos": list(ultimos.values())}
    except Exception as e:
        return {"error": "Error conectando a Nube B2B"}

@app.post("/api/crear_columna")
def crear_columna(datos: dict):
    try:
        supabase.table('configuracion').insert({"nombre_columna": datos.get("nombre")}).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": "Error en Servidor Central"}

@app.post("/api/borrar_columna")
def borrar_columna(datos: dict):
    try:
        supabase.table('configuracion').delete().eq("nombre_columna", datos.get("nombre")).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": "Error en Servidor Central"}

@app.post("/api/historial_chat")
def historial_chat(datos: dict):
    res = supabase.table('prospectos').select('mensaje').eq('nombre', datos["nombre"]).order('id', desc=False).execute()
    historial = []
    for fila in res.data:
        texto = fila['mensaje']
        es_mio = texto.startswith("TÚ: ")
        if es_mio: texto = texto.replace("TÚ: ", "", 1)
        historial.append({"texto": texto, "es_mio": es_mio})
    return {"historial": historial}

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: ProspectoUpdate):
    try:
        supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('nombre', datos.nombre).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": "Error de persistencia"}

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: NotaUpdate):
    try:
        supabase.table('prospectos').update({'notas': datos.notas, 'etiquetas': datos.etiquetas}).eq('nombre', datos.nombre).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error"}

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: dict):
    try:
        supabase.table('prospectos').update({'columna': 'Papelera'}).eq('nombre', datos["nombre"]).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error"}

@app.post("/api/borrar_permanente")
def borrar_permanente(datos: dict):
    try:
        supabase.table('prospectos').delete().eq('nombre', datos.get("nombre")).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": "No se pudo eliminar de la Nube"}

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
def inyectar_starter():
    try:
        maestros = supabase.table('catalogo_maestro').select('*').eq('starter_pack', True).execute()
        lote = []
        for m in maestros.data:
            item = {
                "nombre": m["nombre"], "consola": m["consola"], "precio": m["precio_sugerido"],
                "costo": 0, "stock": 0, "estado_general": "Suelto", "codigo_barras": ""
            }
            lote.append(item)
        
        if lote:
            supabase.table('inventario').insert(lote).execute()
        return {"status": "ok", "inyectados": len(lote)}
    except Exception as e:
        return {"status": "error", "detalle": "Error inyectando Starter Pack"}

# ==========================================
# 📦 INVENTARIO & DB (CON GATILLO DE RADAR)
# ==========================================
@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem):
    try:
        nombre_limpio = datos.nombre.strip()
        consola_limpia = datos.consola.strip()
        estado = datos.estado_general.strip()
        
        res = supabase.table('inventario').select('*').ilike('nombre', nombre_limpio).ilike('consola', consola_limpia).ilike('estado_general', estado).execute()
        
        if res.data and len(res.data) > 0:
            supabase.table('inventario').update(datos.dict()).eq('id', res.data[0]['id']).execute()
        else:
            supabase.table('inventario').insert(datos.dict()).execute()
            
        res_alertas = supabase.table('alertas_mercado').select('*').ilike('juego', f"%{nombre_limpio}%").eq('activa', True).execute()
        for alerta in res_alertas.data:
            if alerta['precio_maximo'] >= datos.precio and datos.precio > 0:
                disparar_whatsapp_real(ADMIN_PHONE, f"🎯 *¡RADAR B2B ACTIVADO!* 🎯\nSe acaba de dar de alta en la red:\n🎮 *{datos.nombre}* ({datos.consola})\n💰 Precio: ${datos.precio}\n👤 Alerta de: {alerta['usuario']}")

        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": "Error guardando en Nube"}

@app.post("/api/borrar_item")
def borrar_item(datos: dict):
    try:
        supabase.table('inventario').delete().eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/cargar_inventario")
def cargar_inventario():
    try: 
        res = supabase.table('inventario').select('*').order('nombre', desc=False).execute()
        return {"status": "ok", "inventario": res.data}
    except Exception as e: 
        return {"status": "error"}

@app.post("/api/actualizar_stock")
def actualizar_stock(datos: VentaItem):
    try:
        res = supabase.table('inventario').select('precio, costo').eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).execute()
        
        if res.data and len(res.data) > 0:
            precio_venta = res.data[0].get('precio', 0.0)
            costo_compra = res.data[0].get('costo', 0.0)
            ganancia = precio_venta - costo_compra
            
            supabase.table('inventario').update({'stock': datos.nuevo_stock}).eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).execute()
            
            registro = {"nombre_juego": datos.nombre, "precio_venta": precio_venta, "costo": costo_compra, "ganancia": ganancia}
            supabase.table('registro_ventas').insert(registro).execute()
            
            return {"status": "ok"}
        else:
            return {"status": "error", "detalle": "Variante no encontrada"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).execute()
        if res.data and len(res.data) > 0: 
            return {"status": "ok", "juego": res.data[0]}
        return {"status": "error", "detalle": "Código no registrado"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/metricas")
def obtener_metricas():
    try:
        res_inv = supabase.table('inventario').select('precio, costo, stock').execute()
        total_piezas = sum(item.get('stock', 0) for item in res_inv.data if item.get('stock', 0) > 0)
        valor_inventario = sum((item.get('stock', 0) * item.get('precio', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        costo_inventario = sum((item.get('stock', 0) * item.get('costo', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        
        res_ventas = supabase.table('registro_ventas').select('ganancia, precio_venta').execute()
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
# 🔗 WEBHOOK (RECEPCIÓN META Y PERSISTENCIA)
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
                    enlace = descargar_y_subir_multimedia(msg[tipo]["id"], msg[tipo].get("mime_type", ""), ".bin")
                    texto = f"[{tipo.upper()}] recibida: {enlace}"
                
                res_ex = supabase.table('prospectos').select('columna').eq('nombre', nombre).order('id', desc=True).limit(1).execute()
                col_destino = res_ex.data[0]['columna'] if res_ex.data else "Bandeja Nueva"
                
                supabase.table('prospectos').insert({
                    "nombre": nombre, "telefono": tel, "origen": "WHATSAPP", 
                    "mensaje": texto, "columna": col_destino
                }).execute()
                
                # 🛡️ BLINDAJE FINAL: El bot SIEMPRE responde, EXCEPTO si tú estás hablando con el cliente manualmente
                if tipo == "text" and col_destino != "En Conversacion":
                    procesar_respuesta_bot(nombre, tel, texto, col_destino)
                    
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e: 
        return PlainTextResponse(content="ERROR", status_code=500)

# ==========================================
# 🟢 ENVIAR MENSAJES DESDE GODOT (NUEVA RUTA)
# ==========================================
@app.post("/api/enviar_mensaje")
def api_enviar_mensaje(datos: MensajeSaliente):
    try:
        supabase.table('prospectos').insert({
            "nombre": datos.cliente,
            "origen": "WHATSAPP",
            "mensaje": f"TÚ: {datos.texto}",
            "columna": "En Conversacion" 
        }).execute()
        
        # Al enviar un mensaje manual, el cliente pasa a "En Conversacion" para silenciar al bot
        supabase.table('prospectos').update({'columna': 'En Conversacion'}).eq('nombre', datos.cliente).execute()
        
        res_tel = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).neq('telefono', None).limit(1).execute()
        if res_tel.data:
            telefono_destino = res_tel.data[0]['telefono']
            disparar_whatsapp_real(telefono_destino, datos.texto)
            return {"status": "ok"}
        else:
            return {"status": "error", "detalle": "Cliente sin teléfono registrado"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 🚀 MÓDULOS B2B E IA LIMPIADORA (CSV)
# ==========================================
@app.post("/api/importar_inventario")
def api_importar_inventario(datos: dict):
    lote_juegos = datos.get("inventario", [])
    if not lote_juegos or len(lote_juegos) == 0: 
        return {"status": "error", "detalle": "CSV vacío."}
        
    consolas_oficiales = ["PS5", "PS4", "PS3", "PS2", "PS1", "Xbox One", "Xbox 360", "Xbox Clasico", "Nintendo Switch", "Nintendo 3DS", "Nintendo DS", "Nintendo 64", "GameCube", "GameBoy Advance", "GameBoy Color", "Wii", "Wii U", "SNES", "NES", "Genesis", "Otro (PC/Varios)"]
    estados_oficiales = ["Nuevo/Sellado", "Excelente", "Bueno", "Aceptable", "Pobre", "Suelto"]
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

    lote_limpio = []
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

        juego["nombre"] = nombre_corregido
        juego["precio"] = precio_asignado
        juego["consola"] = limpiar_campo(juego.get("consola", ""), consolas_oficiales)
        juego["estado_general"] = limpiar_campo(juego.get("estado_general", "Suelto"), estados_oficiales)
        lote_limpio.append(juego)

    try:
        supabase.table('inventario').insert(lote_limpio).execute()
        return {"status": "ok", "insertados": len(lote_limpio), "mensaje": "Inventario subido a Nube B2B."}
    except Exception as e: 
        return {"status": "error", "detalle": "Error subiendo CSV a la Nube"}

@app.post("/api/crear_alerta")
def api_crear_alerta(datos: dict):
    try:
        alerta = {
            "usuario": datos.get("usuario_id", "Admin"), 
            "juego": datos.get("nombre_juego", "").lower(), 
            "consola": datos.get("consola", ""), 
            "precio_maximo": datos.get("precio_max", 0.0), 
            "activa": True
        }
        supabase.table('alertas_mercado').insert(alerta).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/mis_alertas")
def api_mis_alertas():
    try: 
        res = supabase.table('alertas_mercado').select('*').execute()
        return {"status": "ok", "alertas": res.data}
    except Exception as e: 
        return {"status": "error"}

@app.post("/api/borrar_alerta")
def api_borrar_alerta(datos: dict):
    try:
        id_borrar = int(datos.get('id', 0))
        supabase.table('alertas_mercado').delete().eq('id', id_borrar).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error"}

@app.post("/api/calificar_vendedor")
def api_calificar(datos: dict):
    try:
        reseña = {
            "vendedor": datos.get("vendedor"), 
            "estrellas": datos.get("estrellas", 5), 
            "comentario": datos.get("comentario", "")
        }
        supabase.table('reputacion').insert(reseña).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error"}

@app.get("/api/todas_reputaciones")
def api_todas_reputaciones():
    try:
        res = supabase.table('reputacion').select('*').execute()
        agrupado = {}
        for r in res.data:
            v = r['vendedor']
            if v not in agrupado: agrupado[v] = []
            agrupado[v].append(r['estrellas'])
            
        resultado = [{"vendedor": k, "promedio": round(sum(v)/len(v), 1), "ventas": len(v)} for k, v in agrupado.items()]
        return {"status": "ok", "reputaciones": resultado}
    except Exception as e: 
        return {"status": "error"}

@app.post("/api/mensaje_masivo")
def api_mensaje_masivo(datos: dict):
    try:
        clientes = supabase.table('prospectos').select('telefono').eq('columna', datos.get("columna")).neq('telefono', None).execute().data
        if not clientes: return {"status": "error", "detalle": "Sin clientes válidos."}
            
        telefonos_unicos = set([c["telefono"] for c in clientes if c.get("telefono")])
        for tel in telefonos_unicos: 
            disparar_whatsapp_real(tel, datos.get("texto"))
            
        return {"status": "ok", "enviados": len(telefonos_unicos)}
    except Exception as e: 
        return {"status": "error"}

# ==========================================
# 🛑 BOTÓN DE ENCENDIDO (SIEMPRE AL FINAL)
# ==========================================
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
