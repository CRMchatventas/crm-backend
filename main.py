# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V7.0 (GOLD FULL ENGINE)
# Funciones: Auto-Vendedor AI, ScraperAPI + Fallback, IA Limpiadora,
# Finanzas, Web Bridge, Vistas B2B y Notificaciones Push al Admin.
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
from datetime import datetime
import difflib # 🛡️ LIBRERÍA DE IA LIMPIADORA

app = FastAPI(title="CRM Fantasy Games - Engine V7.0 Gold")

# --- 🔑 CREDENCIALES Y CONFIGURACIÓN DEL BOT ---
META_ACCESS_TOKEN = "EAAQeucaUBYoBRIo9TZA0WoZBhQbqNuSKDdfqPeMKPJnASZBUYRuXL4oZACZC80DrmZCi1jrRvWpFsfwM5gr7AluJOBaJuhox5CZA4ZCjG6VrQqAbIyrX8YQFxhgjjyejPKUrrmMZAzvajWDRrCRJ0VZBFwU47ETnG6Xq7qzybeRZASKoRXdSLmS24JLQW0Vfiwqdi7KkgZDZD"
META_PHONE_ID = "975963255609853"
SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"
SCRAPER_API_KEY = "7cc199d2d6234950e92f4fb7cf96cd6e" 

# 🤖 CONFIGURACIÓN DE TU EMPLEADO DIGITAL
ADMIN_PHONE = "524491142598" # 🔴 CAMBIA ESTO POR TU CELULAR (Para notificaciones de ventas)
LINK_MERCADOPAGO = "https://link.mercadopago.com.mx/tu_link_aqui" # 🔴 PON TU LINK AQUÍ
LINK_CATALOGO = "https://tu-link-al-catalogo.com" # 🔴 PON TU LINK AL CATÁLOGO

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
# 📈 MOTOR DE PRECIOS PRO (MÉTODO FRANCOTIRADOR V6.2)
# ==========================================
@app.get("/api/consultar_precio")
def api_consultar_precio(nombre: str, consola: str = ""):
    tipo_cambio = obtener_dolar_hoy()
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
    url_proxy = f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_search)}&premium=true"
    
    try:
        headers_humanos = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }
        res = requests.get(url_proxy, timeout=45)
        
        if "scraperapi" in res.text.lower() or res.status_code != 200:
            res = requests.get(url_search, headers=headers_humanos, timeout=15)
            
        soup = BeautifulSoup(res.text, 'html.parser')
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
            res = requests.get(f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(link_juego)}&premium=true", timeout=45)
            if "scraperapi" in res.text.lower() or res.status_code != 200:
                res = requests.get(link_juego, headers=headers_humanos, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')

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

        return {
            "status": "ok",
            "mxn": {"loose": round(p_loose * tipo_cambio, 2), "cib": round(p_cib * tipo_cambio, 2), "new": round(p_new * tipo_cambio, 2)},
            "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
            "tipo_cambio": tipo_cambio,
            "url_pc": link_juego if link_juego else url_search
        }
    except Exception as e:
        return {"status": "error", "detalle": str(e), "url_pc": "https://www.pricecharting.com"}

# ==========================================
# 📥 MOTOR MULTIMEDIA & WHATSAPP
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
                print(f"❌ Error Supabase: {e}")
    return None

def disparar_whatsapp_real(telefono_destino: str, texto_mensaje: str):
    url = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: 
        requests.post(url, headers=headers, json=payload)
    except Exception as e: 
        print(f"❌ Error Meta API: {e}")

# ==========================================
# 🤖 BOT AAA: EL EMPLEADO DIGITAL 24/7
# ==========================================
def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str):
    texto = texto_entrante.lower().strip()
    respuesta = ""
    hora = datetime.now().hour
    saludo = "Buenos días" if hora < 12 else "Buenas tardes" if hora < 19 else "Buenas noches"

    # 1. SALUDO Y MENÚ PRINCIPAL
    if any(word in texto for word in ["hola", "buenas", "menu", "menú", "info"]):
        respuesta = f"¡{saludo}! 🎮 Soy el asistente virtual de Fantasy Games.\n\n¿En qué te ayudo?\n*1.* 👾 Ver Catálogo completo\n*2.* 🚚 Métodos de entrega y pago\n*3.* 🙋‍♂️ Hablar con Miguel\n\n_O dime el nombre del juego que buscas y reviso si hay disponibilidad._"
    
    # 2. CATÁLOGO
    elif texto == "1" or "catalogo" in texto or "catálogo" in texto:
        respuesta = f"🕹️ *Catálogo Fantasy Games:*\nCheca todos los títulos que tenemos disponibles aquí:\n{LINK_CATALOGO}\n\nSi te interesa alguno, solo escríbeme el nombre."
    
    # 3. ENTREGAS Y PAGOS (REGLAS DE MIGUEL)
    elif texto == "2" or "pago" in texto or "entrega" in texto or "donde entregas" in texto or "ubicacion" in texto:
        respuesta = (
            "🚚 *Entregas y Métodos de Pago:*\n\n"
            "• Si vas a pagar en *efectivo*, te lo entrego en Paseos de Aguascalientes (cerca de mi casa) el día y a la hora que tú quieras.\n"
            "• Entregas a *domicilio* o en *Altaria*, solamente los días miércoles y viernes por las tardes.\n"
            "• 📦 *¿Compra mayor a $1,000?* Te lo entrego a domicilio cualquier día (previo depósito bancario).\n\n"
            "💳 *Para APARTAR un juego seguro:*\nSi me pagas por adelantado, te lo aparto y ya no se lo ofrezco a nadie más. Puedes pagar con tarjeta o transferencia aquí:\n"
            f"{LINK_MERCADOPAGO}\n\n¿Qué juego vas a querer?"
        )
    
    # 4. HABLAR CON HUMANO
    elif texto == "3" or "humano" in texto or "miguel" in texto or "asesor" in texto:
        respuesta = "¡Claro! 👨‍💻 Le estoy mandando una notificación a Miguel. En cuanto se desocupe te contesta por aquí mismo."
        # Notificación Push al Patrón
        disparar_whatsapp_real(ADMIN_PHONE, f"🚨 *ALERTA CRM:* El prospecto {cliente} ({telefono}) está pidiendo atención humana.")

    # 5. EL AUTO-VENDEDOR (Busca en base de datos si el texto parece el nombre de un juego)
    elif len(texto) > 3:
        try:
            # Busca coincidencias en la base de datos (ilike no es sensible a mayusculas/minusculas)
            res = supabase.table('inventario').select('*').ilike('nombre', f"%{texto}%").execute()
            if res.data and len(res.data) > 0:
                juegos_encontrados = "\n".join([f"🎮 *{j['nombre']}* ({j['consola']}) - ${j['precio']} MXN" for j in res.data[:3]])
                respuesta = (
                    f"¡Sí lo tengo en inventario!\n\n{juegos_encontrados}\n\n"
                    "🔥 *¿Lo quieres asegurar?*\nPágalo por adelantado y te lo aparto ahora mismo para entregártelo:\n"
                    f"{LINK_MERCADOPAGO}\n\n_(Mándame captura de tu pago por aquí)_"
                )
                # Notifica a Miguel de una posible venta
                disparar_whatsapp_real(ADMIN_PHONE, f"💰 *INTENCIÓN DE COMPRA:*\nEl bot le acaba de ofrecer {res.data[0]['nombre']} a {cliente}. ¡Échale un ojo al chat!")
            else:
                respuesta = "Hmm, parece que por ahora no tengo ese juego exacto en sistema, o tal vez se escribe diferente. 😅\n\nPuedes checar el catálogo completo mandando un *1*, o pide hablar con Miguel mandando un *3* para que te confirme."
        except Exception as e:
            print(f"Error en Auto-Seller: {e}")

    # SI HAY RESPUESTA, GUARDAR EN HISTORIAL Y ENVIAR WHATSAPP
    if respuesta:
        datos_guardar = {
            "nombre": cliente, 
            "telefono": telefono, 
            "origen": "WHATSAPP", 
            "mensaje": f"TÚ: [BOT] {respuesta}", 
            "columna": "Bandeja Nueva"
        }
        supabase.table('prospectos').insert(datos_guardar).execute()
        disparar_whatsapp_real(telefono, respuesta)

@app.post("/api/enviar_mensaje")
def enviar_mensaje_whatsapp(datos: MensajeSaliente):
    res = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).neq('telefono', None).order('id', desc=True).limit(1).execute()
    tel = res.data[0]['telefono'] if res.data else None
    
    supabase.table('prospectos').insert({
        "nombre": datos.cliente, 
        "telefono": tel, 
        "origen": "WHATSAPP", 
        "mensaje": f"TÚ: {datos.texto}", 
        "columna": "Respondió"
    }).execute()
    
    if tel: 
        disparar_whatsapp_real(tel, datos.texto)
    return {"status": "enviado"}

# ==========================================
# 🌐 RUTAS DE GESTIÓN CRM
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo():
    res_cols = supabase.table('configuracion').select('nombre_columna').execute()
    columnas = [row['nombre_columna'] for row in res_cols.data]
    
    res_prospectos = supabase.table('prospectos').select('*').order('id', desc=False).execute()
    ultimos = {}
    for fila in res_prospectos.data: 
        ultimos[fila['nombre']] = fila
        
    return {"columnas": columnas, "prospectos": list(ultimos.values())}

@app.post("/api/historial_chat")
def historial_chat(datos: dict):
    res = supabase.table('prospectos').select('mensaje').eq('nombre', datos["nombre"]).order('id', desc=False).execute()
    historial = []
    for fila in res.data:
        texto = fila['mensaje']
        es_mio = texto.startswith("TÚ: ")
        if es_mio: 
            texto = texto.replace("TÚ: ", "", 1)
        historial.append({"texto": texto, "es_mio": es_mio})
    return {"historial": historial}

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: ProspectoUpdate):
    try:
        supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('nombre', datos.nombre).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: NotaUpdate):
    try:
        supabase.table('prospectos').update({'notas': datos.notas, 'etiquetas': datos.etiquetas}).eq('nombre', datos.nombre).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: dict):
    try:
        supabase.table('prospectos').update({'columna': 'Papelera'}).eq('nombre', datos["nombre"]).execute()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 📦 INVENTARIO & DB
# ==========================================
@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem):
    try:
        nombre_limpio = datos.nombre.strip()
        consola_limpia = datos.consola.strip()
        estado = datos.estado_general.strip()
        
        res = supabase.table('inventario').select('*').ilike('nombre', nombre_limpio).ilike('consola', consola_limpia).ilike('estado_general', estado).execute()
        
        if len(res.data) > 0:
            supabase.table('inventario').update(datos.dict()).eq('id', res.data[0]['id']).execute()
        else:
            supabase.table('inventario').insert(datos.dict()).execute()
            
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_item")
def borrar_item(datos: dict):
    try:
        supabase.table('inventario').delete().eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.get("/api/cargar_inventario")
def cargar_inventario():
    try: 
        res = supabase.table('inventario').select('*').order('nombre', desc=False).execute()
        return {"status": "ok", "inventario": res.data}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/actualizar_stock")
def actualizar_stock(datos: VentaItem):
    try:
        res = supabase.table('inventario').select('precio, costo').eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).execute()
        
        if len(res.data) > 0:
            precio_venta = res.data[0].get('precio', 0.0)
            costo_compra = res.data[0].get('costo', 0.0)
            ganancia = precio_venta - costo_compra
            
            supabase.table('inventario').update({'stock': datos.nuevo_stock}).eq('nombre', datos.nombre).eq('consola', datos.consola).eq('estado_general', datos.estado_general).execute()
            
            registro = {
                "nombre_juego": datos.nombre,
                "precio_venta": precio_venta,
                "costo": costo_compra,
                "ganancia": ganancia
            }
            supabase.table('registro_ventas').insert(registro).execute()
            
            return {"status": "ok"}
        else:
            return {"status": "error", "detalle": "Variante no encontrada"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

# 🔍 EL ENDPOINT DEL ESCÁNER (Restaurado y Blindado)
@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).execute()
        if len(res.data) > 0: 
            return {"status": "ok", "juego": res.data[0]}
        return {"status": "error", "detalle": "Código no registrado"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

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
            "status": "ok", 
            "piezas": total_piezas, 
            "valor": valor_inventario,
            "costo_inv": costo_inventario, 
            "ganancia_potencial": valor_inventario - costo_inventario,
            "ventas_totales": ventas_totales, 
            "ganancia_real": ganancia_real
        }
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 🔗 WEBHOOK (RECEPCIÓN META)
# ==========================================
@app.get("/webhook")
def verificar_webhook(request: Request):
    if request.query_params.get("hub.verify_token") == "mi_contrasena_secreta_fantasy": 
        return PlainTextResponse(content=request.query_params.get("hub.challenge"), status_code=200)
    return PlainTextResponse(content="CRM Fantasy Activo.", status_code=200)

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
                
                if tel.startswith("521"): 
                    tel = "52" + tel[3:]
                
                tipo = msg.get("type", "text")
                texto = ""
                
                if tipo == "text": 
                    texto = msg["text"]["body"]
                elif tipo in ["image", "video", "document", "audio"]:
                    enlace = descargar_y_subir_multimedia(msg[tipo]["id"], msg[tipo].get("mime_type", ""), ".bin")
                    texto = f"[{tipo.upper()}] recibida: {enlace}"
                
                res = supabase.table('prospectos').select('columna').eq('nombre', nombre).order('id', desc=True).limit(1).execute()
                col = res.data[0]['columna'] if res.data else "Bandeja Nueva"
                
                supabase.table('prospectos').insert({
                    "nombre": nombre, 
                    "telefono": tel, 
                    "origen": "WHATSAPP", 
                    "mensaje": texto, 
                    "columna": col
                }).execute()
                
                # 🛡️ BOT SIEMPRE ACTIVO PARA COMANDOS CLAVE
                es_comando = tipo == "text" and any(cmd in texto.lower() for cmd in ["hola", "buenas", "menu", "menú", "info"])
                if (col in ["Bandeja Nueva", "Primer Contacto"] and tipo == "text") or es_comando:
                    procesar_respuesta_bot(nombre, tel, texto)
                    
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e: 
        return PlainTextResponse(content="ERROR", status_code=500)

# ==========================================
# 🚀 MÓDULOS B2B E IA LIMPIADORA (FUZZY MATCH)
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

    def limpiar_campo(texto_usuario, lista_oficial):
        texto_upper = str(texto_usuario).strip().upper()
        if texto_upper in diccionario_sinonimos: 
            return diccionario_sinonimos[texto_upper]
        
        coincidencias = difflib.get_close_matches(str(texto_usuario).strip(), lista_oficial, n=1, cutoff=0.5)
        return coincidencias[0] if coincidencias else str(texto_usuario).strip()

    lote_limpio = []
    for juego in lote_juegos:
        juego["consola"] = limpiar_campo(juego.get("consola", ""), consolas_oficiales)
        juego["estado_general"] = limpiar_campo(juego.get("estado_general", "Suelto"), estados_oficiales)
        lote_limpio.append(juego)

    try:
        supabase.table('inventario').insert(lote_limpio).execute()
        return {"status": "ok", "insertados": len(lote_limpio), "mensaje": f"Se subieron y limpiaron {len(lote_limpio)} artículos."}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/crear_alerta")
def api_crear_alerta(datos: dict):
    try:
        alerta = {
            "usuario": datos.get("usuario_id", "Miguel"), 
            "juego": datos.get("nombre_juego", "").lower(), 
            "consola": datos.get("consola", ""), 
            "precio_maximo": datos.get("precio_max", 0.0), 
            "activa": True
        }
        supabase.table('alertas_mercado').insert(alerta).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.get("/api/mis_alertas")
def api_mis_alertas():
    try: 
        res = supabase.table('alertas_mercado').select('*').execute()
        return {"status": "ok", "alertas": res.data}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_alerta")
def api_borrar_alerta(datos: dict):
    try:
        supabase.table('alertas_mercado').delete().eq('id', datos.get('id')).execute()
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

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
        return {"status": "error", "detalle": str(e)}

@app.get("/api/todas_reputaciones")
def api_todas_reputaciones():
    try:
        res = supabase.table('reputacion').select('*').execute()
        agrupado = {}
        for r in res.data:
            v = r['vendedor']
            if v not in agrupado: 
                agrupado[v] = []
            agrupado[v].append(r['estrellas'])
            
        resultado = [{"vendedor": k, "promedio": round(sum(v)/len(v), 1), "ventas": len(v)} for k, v in agrupado.items()]
        return {"status": "ok", "reputaciones": resultado}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/mensaje_masivo")
def api_mensaje_masivo(datos: dict):
    try:
        clientes = supabase.table('prospectos').select('telefono').eq('columna', datos.get("columna")).neq('telefono', None).execute().data
        if not clientes: 
            return {"status": "error", "detalle": "Sin clientes válidos."}
            
        telefonos_unicos = set([c["telefono"] for c in clientes if c.get("telefono")])
        for tel in telefonos_unicos: 
            disparar_whatsapp_real(tel, datos.get("texto"))
            
        return {"status": "ok", "enviados": len(telefonos_unicos)}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 🛑 BOTÓN DE ENCENDIDO (SIEMPRE AL FINAL)
# ==========================================
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
