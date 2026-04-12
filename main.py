# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V6.0 (GOLD FULL ENGINE)
# Funciones: WhatsApp Full, Bot, Inventario, ScraperAPI (Anti-Europa), 
# Finanzas Completas (Costos y Ganancias), Web Bridge y Extractor Agresivo.
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

app = FastAPI(title="CRM Fantasy Games - Engine V6.0 Gold")

# --- 🔑 CREDENCIALES ---
META_ACCESS_TOKEN = "EAAQeucaUBYoBRIo9TZA0WoZBhQbqNuSKDdfqPeMKPJnASZBUYRuXL4oZACZC80DrmZCi1jrRvWpFsfwM5gr7AluJOBaJuhox5CZA4ZCjG6VrQqAbIyrX8YQFxhgjjyejPKUrrmMZAzvajWDRrCRJ0VZBFwU47ETnG6Xq7qzybeRZASKoRXdSLmS24JLQW0Vfiwqdi7KkgZDZD"
META_PHONE_ID = "975963255609853"
SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"

# 🔴 PON AQUÍ TU LLAVE GRATUITA DE SCRAPERAPI 🔴
SCRAPER_API_KEY = "7cc199d2d6234950e92f4fb7cf96cd6e" 

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 📦 MODELOS DE DATOS ---
class ProspectoUpdate(BaseModel): 
    nombre: str; nueva_columna: str
    
class NotaUpdate(BaseModel): 
    nombre: str; notas: str; etiquetas: str
    
class MensajeSaliente(BaseModel): 
    cliente: str; texto: str

# 🛒 MODELO DE INVENTARIO (Incluye COSTO)
class InventarioItem(BaseModel):
    nombre: str; consola: str; precio: float; costo: float; stock: int
    codigo_barras: str; url_portada: str; estado_general: str
    tiene_caja: bool; tiene_manual: bool; es_portada_original: bool; descripcion_detallada: str

# 🛒 MODELO PARA RECIBIR VENTAS
class VentaItem(BaseModel):
    nombre: str
    consola: str
    nuevo_stock: int

# ==========================================
# 💵 MOTOR DE DIVISAS
# ==========================================
def obtener_dolar_hoy():
    print("\n--- 💹 [MONEDA] CONSULTANDO TIPO DE CAMBIO ---")
    try:
        res = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=5)
        datos = res.json()
        valor = float(datos.get("rates", {}).get("MXN", 18.00))
        print(f"✔️ [MONEDA] Precio detectado: ${valor} MXN")
        return valor
    except Exception as e:
        print(f"⚠️ [MONEDA] Error API: {e}. Usando respaldo: 18.00")
        return 18.00


# ==========================================
# 📈 MOTOR DE PRECIOS PRO (MÉTODO FRANCOTIRADOR V6.2 BLINDADO)
# ==========================================
@app.get("/api/consultar_precio")
def api_consultar_precio(nombre: str, consola: str = ""):
    tipo_cambio = obtener_dolar_hoy()
    
    # 1. 🛡️ TRADUCTOR ESTRICTO: Evita que "PS2" coincida con "ps2-secret-codes"
    slugs_pc = {
        "PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3",
        "PS2": "playstation-2", "PS1": "playstation",
        "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox",
        "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", 
        "Nintendo DS": "nintendo-ds", "Nintendo 64": "nintendo-64",
        "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance", 
        "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", 
        "SNES": "super-nintendo", "NES": "nes", "Genesis": "sega-genesis"
    }
    
    # Consola limpia para la búsqueda en la barra de URL
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    query = f"{nombre} {consola_web}".replace(" ", "+")
    url_search = f"https://www.pricecharting.com/search-products?q={query}&type=videogames"
    
    url_proxy = f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_search)}&premium=true"
    
    print(f"\n--- 🔍 [SCRAPER PRO] CONSULTANDO: {nombre} ({consola}) ---")
    
    try:
        res = requests.get(url_proxy, timeout=45)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        titulo = soup.title.text.strip() if soup.title else "SIN TÍTULO"
        print(f"📄 [RADAR] Página detectada: {titulo}")
        
        link_juego = None
        
        # Identificamos la etiqueta exacta que PriceCharting usa para esa consola
        slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
        etiqueta_busqueda = f"/game/{slug_esperado}/"
        
        # 2. 🛡️ BLINDAJE ANTI-BASURA: Palabras que el francotirador debe ignorar siempre
        palabras_prohibidas = ['strategy-guide', 'magazine', 'comic', 'lot', 'bundle', 'box-only', 'manual-only', 'empty-box']
        
        # MÉTODO FRANCOTIRADOR (Ataque Quirúrgico)
        for a in soup.find_all('a', href=True):
            href = a['href'].lower()
            if '/game/' in href:
                # Si el link contiene basura, lo ignoramos y pasamos al siguiente
                if any(basura in href for basura in palabras_prohibidas):
                    continue
                
                # Buscamos coincidencia PERFECTA con el pasillo de la consola
                if etiqueta_busqueda in href:
                    link_juego = a['href'] if a['href'].startswith("http") else "https://www.pricecharting.com" + a['href']
                    break
        
        # 3. RESPALDO (Por si el juego es raro y no cuadró la etiqueta exacta, pero sigue filtrando basura)
        if not link_juego:
            for a in soup.find_all('a', href=True):
                href = a['href'].lower()
                if '/game/' in href:
                    if any(basura in href for basura in palabras_prohibidas):
                        continue
                    link_juego = a['href'] if a['href'].startswith("http") else "https://www.pricecharting.com" + a['href']
                    break

        if link_juego:
            print(f"🔗 [FRANCOTIRADOR] Link oficial limpio: {link_juego}")
            res = requests.get(f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(link_juego)}&premium=true", timeout=45)
            soup = BeautifulSoup(res.text, 'html.parser')
        else:
            print("⚠️ [FRANCOTIRADOR] No se encontraron links exactos. Extrayendo de la lista general.")

        # 4. EXTRACCIÓN SÚPER AGRESIVA (Con filtro Anti-Europa)
        def extraer_numero_puro(id_css):
            nodo = soup.find(id=id_css)
            if nodo:
                # 🛠️ BLINDAJE: Convertimos comas europeas a puntos americanos para evitar crasheos
                texto = nodo.text.replace(',', '.')
                texto_limpio = ''.join(c for c in texto if c.isdigit() or c == '.')
                try:
                    if texto_limpio: return float(texto_limpio)
                except: pass
            return 0.0

        p_loose = extraer_numero_puro("used_price")
        p_cib   = extraer_numero_puro("cib_price")
        p_new   = extraer_numero_puro("new_price")
        
        # 🛡️ BLINDAJE: Si los IDs fallan, buscamos números de respaldo a la fuerza
        if p_loose == 0.0 and p_cib == 0.0 and p_new == 0.0:
            spans = soup.find_all("span", class_="price")
            numeros = []
            for s in spans:
                limpio = ''.join(c for c in s.text.replace(',', '.') if c.isdigit() or c == '.')
                if limpio: numeros.append(float(limpio))
            if len(numeros) >= 3:
                p_loose, p_cib, p_new = numeros[0], numeros[1], numeros[2]
            elif len(numeros) > 0:
                p_loose = numeros[0] # Recuperamos al menos el Loose si la página está rota

        print(f"💰 [SCRAPER] USD -> Loose: {p_loose}, CIB: {p_cib}, New: {p_new}")
        
        # 🔗 BLINDAJE: Retorna la URL dinámica para que el botón de Godot siempre te lleve al sitio correcto
        return {
            "status": "ok",
            "mxn": {"loose": round(p_loose * tipo_cambio, 2), "cib": round(p_cib * tipo_cambio, 2), "new": round(p_new * tipo_cambio, 2)},
            "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
            "tipo_cambio": tipo_cambio,
            "url_pc": link_juego if link_juego else url_search
        }
    except Exception as e:
        print(f"❌ [SCRAPER] Error crítico de API: {e}")
        return {"status": "error", "detalle": str(e), "url_pc": "https://www.pricecharting.com"}

# ==========================================
# 📥 MOTOR MULTIMEDIA & WHATSAPP
# ==========================================
def descargar_y_subir_multimedia(media_id: str, mime_type: str, extension_default: str):
    print(f"\n--- 📥 [MULTIMEDIA] DESCARGANDO ID: {media_id} ---")
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
                print(f"✔️ [MULTIMEDIA] Guardado en: {public_url}")
                return public_url
            except Exception as e: print(f"❌ Error Supabase: {e}")
    return None

def disparar_whatsapp_real(telefono_destino: str, texto_mensaje: str):
    print(f"🚀 [WHATSAPP] Enviando a {telefono_destino}...")
    url = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try: requests.post(url, headers=headers, json=payload)
    except Exception as e: print(f"❌ Error Meta API: {e}")

# ==========================================
# 🤖 BOT & WHATSAPP ENDPOINTS
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

@app.post("/api/enviar_mensaje")
def enviar_mensaje_whatsapp(datos: MensajeSaliente):
    res = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).neq('telefono', None).order('id', desc=True).limit(1).execute()
    tel = res.data[0]['telefono'] if res.data else None
    supabase.table('prospectos').insert({"nombre": datos.cliente, "telefono": tel, "origen": "WHATSAPP", "mensaje": f"TÚ: {datos.texto}", "columna": "Respondió"}).execute()
    if tel: disparar_whatsapp_real(tel, datos.texto)
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

@app.post("/api/actualizar_estado")
def actualizar_estado(datos: ProspectoUpdate):
    supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('nombre', datos.nombre).execute()
    return {"status": "ok"}

@app.post("/api/actualizar_notas")
def actualizar_notas(datos: NotaUpdate):
    supabase.table('prospectos').update({'notas': datos.notas, 'etiquetas': datos.etiquetas}).eq('nombre', datos.nombre).execute()
    return {"status": "ok"}

@app.post("/api/borrar_prospecto")
def borrar_prospecto(datos: dict):
    supabase.table('prospectos').update({'columna': 'Papelera'}).eq('nombre', datos["nombre"]).execute()
    return {"status": "ok"}

# ==========================================
# 📦 INVENTARIO & DB (Incluye Costos y Ganancias)
# ==========================================
@app.post("/api/guardar_inventario")
def guardar_inventario(datos: InventarioItem):
    try:
        nombre_limpio = datos.nombre.strip()
        consola_limpia = datos.consola.strip()
        
        res = supabase.table('inventario').select('*').ilike('nombre', nombre_limpio).ilike('consola', consola_limpia).execute()
        
        if len(res.data) > 0:
            id_real = res.data[0]['id']
            supabase.table('inventario').update(datos.dict()).eq('id', id_real).execute()
            print(f"🔄 [DB] Registro Modificado: {nombre_limpio}")
        else:
            supabase.table('inventario').insert(datos.dict()).execute()
            print(f"✔️ [DB] Nuevo Guardado: {nombre_limpio}")
            
        return {"status": "ok"}
    except Exception as e: 
        return {"status": "error", "detalle": str(e)}

@app.post("/api/borrar_item")
def borrar_item(datos: dict):
    try:
        supabase.table('inventario').delete().eq('nombre', datos.get("nombre", "")).eq('consola', datos.get("consola", "")).execute()
        print(f"🗑️ [DB] Item borrado: {datos.get('nombre', '')}")
        return {"status": "ok"}
    except Exception as e: return {"status": "error", "detalle": str(e)}

@app.get("/api/cargar_inventario")
def cargar_inventario():
    try:
        res = supabase.table('inventario').select('*').order('nombre', desc=False).execute()
        print(f"📦 [DB] Cargando {len(res.data)} juegos del inventario.")
        return {"status": "ok", "inventario": res.data}
    except Exception as e:
        print(f"❌ [DB] Error al cargar inventario: {e}")
        return {"status": "error", "detalle": str(e)}

# 💰 ENDPOINT DE VENTAS: DESCUENTA STOCK Y REGISTRA GANANCIA
@app.post("/api/actualizar_stock")
def actualizar_stock(datos: VentaItem):
    try:
        # 1. Buscamos precio y costo actual del juego
        res = supabase.table('inventario').select('precio, costo').eq('nombre', datos.nombre).eq('consola', datos.consola).execute()
        
        if len(res.data) > 0:
            precio_venta = res.data[0].get('precio', 0.0)
            costo_compra = res.data[0].get('costo', 0.0)
            ganancia = precio_venta - costo_compra
            
            # 2. Actualizamos el stock
            supabase.table('inventario').update({'stock': datos.nuevo_stock}).eq('nombre', datos.nombre).eq('consola', datos.consola).execute()
            
            # 3. Guardamos el ticket en registro_ventas
            registro = {
                "nombre_juego": datos.nombre,
                "precio_venta": precio_venta,
                "costo": costo_compra,
                "ganancia": ganancia
            }
            supabase.table('registro_ventas').insert(registro).execute()
            
            print(f"💰 [VENTA] {datos.nombre} -> Stock restante: {datos.nuevo_stock} | Ganancia: ${ganancia}")
            return {"status": "ok"}
        else:
            return {"status": "error", "detalle": "Juego no encontrado en BD para venta"}
            
    except Exception as e: 
        print(f"❌ [VENTA] Error: {e}")
        return {"status": "error", "detalle": str(e)}

# 🔍 NUEVO ENDPOINT: BUSCAR JUEGO POR CÓDIGO DE BARRAS
@app.get("/api/buscar_por_codigo")
def buscar_por_codigo(codigo: str):
    try:
        res = supabase.table('inventario').select('*').eq('codigo_barras', codigo).execute()
        if len(res.data) > 0:
            return {"status": "ok", "juego": res.data[0]}
        return {"status": "error", "detalle": "Código no registrado"}
    except Exception as e:
        return {"status": "error", "detalle": str(e)}

# 📊 ENDPOINT: MÉTRICAS FINANCIERAS (DASHBOARD CEO)
@app.get("/api/metricas")
def obtener_metricas():
    try:
        # 1. Finanzas del Inventario Actual
        res_inv = supabase.table('inventario').select('precio, costo, stock').execute()
        total_piezas = sum(item.get('stock', 0) for item in res_inv.data if item.get('stock', 0) > 0)
        valor_inventario = sum((item.get('stock', 0) * item.get('precio', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        costo_inventario = sum((item.get('stock', 0) * item.get('costo', 0.0)) for item in res_inv.data if item.get('stock', 0) > 0)
        ganancia_potencial = valor_inventario - costo_inventario
        
        # 2. Histórico de Ventas Reales
        res_ventas = supabase.table('registro_ventas').select('ganancia, precio_venta').execute()
        ventas_totales = sum(v.get('precio_venta', 0.0) for v in res_ventas.data)
        ganancia_real = sum(v.get('ganancia', 0.0) for v in res_ventas.data)
        
        return {
            "status": "ok", 
            "piezas": total_piezas, 
            "valor": valor_inventario,
            "costo_inv": costo_inventario,
            "ganancia_potencial": ganancia_potencial,
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
                msg, contact = valor["messages"][0], valor["contacts"][0]
                nombre, tel = contact["profile"]["name"], msg["from"]
                if tel.startswith("521"): tel = "52" + tel[3:]
                
                tipo, texto = msg.get("type", "text"), ""
                if tipo == "text": texto = msg["text"]["body"]
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
        print(f"❌ [WEBHOOK] Error: {e}")
        return PlainTextResponse(content="ERROR", status_code=500)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
