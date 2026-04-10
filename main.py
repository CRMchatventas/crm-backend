# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V5.9 (GOLD FULL ENGINE)
# Funciones: WhatsApp Full, Multimedia Supabase, Bot, 
# Inventario, Scraper Dólar Real & Borrado Quirúrgico.
# ==========================================
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn
import requests
import mimetypes
from bs4 import BeautifulSoup
from supabase import create_client, Client
from datetime import datetime

app = FastAPI(title="CRM Fantasy Games - Engine V5.9 Gold")

# --- 🔑 CREDENCIALES (Configuración Maestra) ---
META_ACCESS_TOKEN = "EAAQeucaUBYoBRIo9TZA0WoZBhQbqNuSKDdfqPeMKPJnASZBUYRuXL4oZACZC80DrmZCi1jrRvWpFsfwM5gr7AluJOBaJuhox5CZA4ZCjG6VrQqAbIyrX8YQFxhgjjyejPKUrrmMZAzvajWDRrCRJ0VZBFwU47ETnG6Xq7qzybeRZASKoRXdSLmS24JLQW0Vfiwqdi7KkgZDZD"
META_PHONE_ID = "975963255609853"
SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"

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
    stock: int
    codigo_barras: str
    url_portada: str
    estado_general: str
    tiene_caja: bool
    tiene_manual: bool
    es_portada_original: bool
    descripcion_detallada: str

# ==========================================
# 💵 MOTOR DE DIVISAS (TIPO DE CAMBIO REAL)
# ==========================================
def obtener_dolar_hoy():
    """Consulta el valor real del dólar en México."""
    try:
        url = "https://www.google.com/search?q=precio+dolar+mexico+hoy"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, "html.parser")
        valor_raw = soup.find("div", class_="BNeawe iBp4i AP7Wnd").text
        valor = float(valor_raw.split()[0].replace(",", ""))
        print(f"💹 [MONEDA] Dólar actualizado: ${valor} MXN")
        return valor
    except:
        print("⚠️ [MONEDA] Falló scraping. Usando respaldo: 18.00")
        return 18.00

# ==========================================
# 📈 MOTOR DE PRECIOS PRO (PRICECHARTING)
# ==========================================
@app.get("/api/consultar_precio")
def api_consultar_precio(nombre: str, consola: str = ""):
    tipo_cambio = obtener_dolar_hoy()
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    query = f"{nombre} {consola_web}".replace(" ", "+")
    url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print(f"🔍 [SCRAPER] Consultando: {nombre} ({consola_web})")
    
    try:
        res = requests.get(url_search, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # Si hay lista de resultados, entramos al primero
        tabla = soup.select_one("#products_table")
        if tabla:
            link = tabla.select_one("td.title a")
            if link:
                res = requests.get("https://www.pricecharting.com" + link['href'], headers=headers)
                soup = BeautifulSoup(res.text, 'html.parser')

        def extraer(selector):
            nodo = soup.select_one(selector)
            return float(nodo.text.strip().replace("$", "").replace(",", "")) if nodo else 0.0

        p_loose = extraer("#used_price")
        p_cib = extraer("#cib_price")
        p_new = extraer("#new_price")
        
        return {
            "mxn": {
                "loose": round(p_loose * tipo_cambio, 2),
                "cib": round(p_cib * tipo_cambio, 2),
                "new": round(p_new * tipo_cambio, 2)
            },
            "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
            "tipo_cambio": tipo_cambio
        }
    except Exception as e:
        return {"error": str(e)}

# ==========================================
# 📥 MOTOR DE EXTRACCIÓN MULTIMEDIA & WHATSAPP
# ==========================================
def descargar_y_subir_multimedia(media_id: str, mime_type: str, extension_default: str):
    print(f"[Descarga] Solicitando archivo a Meta... ID: {media_id}")
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
                public_url = supabase.storage.from_("multimedia").get_public_url(file_path)
                print(f"[NUBE] ✔️ Multimedia guardada: {public_url}")
                return public_url
            except Exception as e:
                print(f"[ERROR NUBE] Falló subida: {e}")
    return None

def disparar_whatsapp_real(telefono_destino: str, texto_mensaje: str):
    url = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try:
        requests.post(url, headers=headers, json=payload)
    except Exception as e:
        print(f"[API META] ❌ Error de red: {e}")

# ==========================================
# 🤖 CEREBRO VIRTUAL (BOT)
# ==========================================
def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str):
    texto = texto_entrante.lower().strip()
    respuesta = ""
    if "hola" in texto or "info" in texto or "precio" in texto:
        respuesta = "¡Hola! 🎮 Bienvenido a Fantasy Games.\n\nElige una opción:\n*1.* 👾 Ver Catálogo\n*2.* 📍 Ubicación\n*3.* 🙋‍♂️ Hablar con un asesor"
    elif texto == "1": respuesta = "🕹️ *Zelda TotK:* $850\n*Mario Kart 8:* $700"
    elif texto == "2": respuesta = "📍 Estamos en el Centro de Aguascalientes."
    elif texto == "3": respuesta = "¡Claro! 👨‍💻 Enseguida te atiende un humano."

    if respuesta:
        datos_guardar = {"nombre": cliente, "telefono": telefono, "origen": "WHATSAPP", "mensaje": f"TÚ: [BOT] {respuesta}", "columna": "Bandeja Nueva"}
        supabase.table('prospectos').insert(datos_guardar).execute()
        disparar_whatsapp_real(telefono, respuesta)

# ==========================================
# 🌐 RUTAS DE API (GODOT -> PYTHON)
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo():
    res_cols = supabase.table('configuracion').select('nombre_columna').execute()
    columnas = [row['nombre_columna'] for row in res_cols.data]
    res_prospectos = supabase.table('prospectos').select('*').order('id', desc=False).execute()
    ultimos = {}
    for fila in res_prospectos.data: ultimos[fila['nombre']] = fila
    return {"columnas": columnas, "prospectos": list(ultimos.values())}

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

@app.post("/api/enviar_mensaje")
def enviar_mensaje_whatsapp(datos: MensajeSaliente):
    res = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).neq('telefono', None).order('id', desc=True).limit(1).execute()
    tel = res.data[0]['telefono'] if res.data else None
    supabase.table('prospectos').insert({"nombre": datos.cliente, "telefono": tel, "origen": "WHATSAPP", "mensaje": f"TÚ: {datos.texto}", "columna": "Respondió"}).execute()
    if tel: disparar_whatsapp_real(tel, datos.texto)
    return {"status": "enviado"}

@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem):
    try:
        supabase.table('inventario').insert(datos.dict()).execute()
        print(f"📦 [INVENTARIO] ✔️ Guardado: {datos.nombre}")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_item")
def borrar_item(datos: dict):
    try:
        supabase.table('inventario').delete().eq('nombre', datos["nombre"]).eq('consola', datos["consola"]).execute()
        print(f"🗑️ [DB] Item borrado exitosamente.")
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

# ==========================================
# 🔗 WEBHOOK DE META (CON INGESTA MULTIMEDIA)
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
                if tel.startswith("521"): tel = "52" + tel[3:]
                
                tipo = msg.get("type", "text")
                texto = ""
                if tipo == "text":
                    texto = msg["text"]["body"]
                elif tipo in ["image", "video", "document", "audio"]:
                    enlace = descargar_y_subir_multimedia(msg[tipo]["id"], msg[tipo].get("mime_type", ""), ".bin")
                    texto = f"[{tipo.upper()}] recibida: {enlace}"
                
                res = supabase.table('prospectos').select('columna').eq('nombre', nombre).order('id', desc=True).limit(1).execute()
                col = res.data[0]['columna'] if res.data else "Bandeja Nueva"
                supabase.table('prospectos').insert({"nombre": nombre, "telefono": tel, "origen": "WHATSAPP", "mensaje": texto, "columna": col}).execute()
                if col in ["Bandeja Nueva", "Primer Contacto"] and tipo == "text":
                    procesar_respuesta_bot(nombre, tel, texto)
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e:
        print(f"[ERROR WEBHOOK] {e}")
        return PlainTextResponse(content="ERROR", status_code=500)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
