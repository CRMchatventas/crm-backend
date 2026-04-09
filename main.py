# ==========================================
# 🚀 SISTEMA BACKEND: CRM PRO V4.0 (CLOUD SUPABASE)
# ==========================================
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
import uvicorn
import requests
from supabase import create_client, Client
from datetime import datetime

app = FastAPI(title="CRM Fantasy Games - Engine V4.0 Cloud")

# --- 🔑 TUS LLAVES SECRETAS (PÉGALAS AQUÍ) ---
META_ACCESS_TOKEN = "EAAQeucaUBYoBRIo9TZA0WoZBhQbqNuSKDdfqPeMKPJnASZBUYRuXL4oZACZC80DrmZCi1jrRvWpFsfwM5gr7AluJOBaJuhox5CZA4ZCjG6VrQqAbIyrX8YQFxhgjjyejPKUrrmMZAzvajWDRrCRJ0VZBFwU47ETnG6Xq7qzybeRZASKoRXdSLmS24JLQW0Vfiwqdi7KkgZDZD"
META_PHONE_ID = "975963255609853"

SUPABASE_URL = "https://hugvthovfcuuexaiuiqc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh1Z3Z0aG92ZmN1dWV4YWl1aXFjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NTc2Mjk1MCwiZXhwIjoyMDkxMzM4OTUwfQ.Fzi0v4ZAV0jiXnk18unmFfY8nkub6nwNnsQ3pbe-zz4"

# Inicializar conexión a la bóveda en la nube
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- MODELOS DE DATOS ---
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

# ==========================================
# 📡 CAÑÓN DE DISPARO A META
# ==========================================
def disparar_whatsapp_real(telefono_destino: str, texto_mensaje: str):
    if not META_ACCESS_TOKEN or META_ACCESS_TOKEN == "TU_TOKEN_DE_META_AQUI":
        print("[API META] ⚠️ Faltan tus llaves de Meta.")
        return

    url = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}}
    try:
        respuesta = requests.post(url, headers=headers, json=payload)
        if respuesta.status_code == 200:
            print(f"[API META] 🚀 Mensaje entregado a {telefono_destino}")
        else:
            print(f"[API META] ❌ Error: {respuesta.text}")
    except Exception as e:
        print(f"[API META] ❌ Falla de red: {e}")

# ==========================================
# 🤖 CEREBRO VIRTUAL (ÁRBOL DE DECISIONES)
# ==========================================
def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str):
    texto = texto_entrante.lower().strip()
    respuesta = ""

    if "hola" in texto or "info" in texto or "precio" in texto or "buenas" in texto:
        respuesta = "¡Hola! 🎮 Bienvenido a Fantasy Games. Soy tu asistente virtual.\n\nElige una opción:\n*1.* 👾 Ver Catálogo de Juegos\n*2.* 📍 Ubicación y Horarios\n*3.* 🙋‍♂️ Hablar con un asesor humano"
    elif texto == "1":
        respuesta = "🕹️ *Catálogo Destacado:*\n- Zelda Tears of the Kingdom: $850\n- Mario Kart 8: $700\n- Control Xbox Series: $600\n\n*(Ejemplo)* ¿Buscas algún título en especial?"
    elif texto == "2":
        respuesta = "📍 Estamos ubicados en el centro. ¡Hacemos entregas en puntos medios todos los días!"
    elif texto == "3":
        respuesta = "¡Claro! 👨‍💻 Dame un momento, enseguida un compañero revisará tu mensaje."

    if respuesta:
        datos_guardar = {
            "nombre": cliente, "telefono": telefono, "origen": "WHATSAPP", 
            "mensaje": f"TÚ: [BOT] {respuesta}", "columna": "Bandeja Nueva"
        }
        supabase.table('prospectos').insert(datos_guardar).execute()
        print(f"[🤖 BOT] Auto-respuesta a {cliente}")
        disparar_whatsapp_real(telefono, respuesta)

# ==========================================
# 🌐 RUTAS DE API (GODOT -> PYTHON SUPABASE)
# ==========================================
@app.get("/api/cargar_todo")
def cargar_todo():
    # 1. Cargar Columnas
    res_cols = supabase.table('configuracion').select('nombre_columna').execute()
    columnas = [row['nombre_columna'] for row in res_cols.data]
    
    # 2. Cargar último mensaje de cada prospecto
    res_prospectos = supabase.table('prospectos').select('*').order('id', desc=False).execute()
    ultimos = {}
    for fila in res_prospectos.data:
        nombre = fila['nombre']
        ultimos[nombre] = fila # Sobreescribe hasta dejar el más reciente
        
    prospectos = list(ultimos.values())
    return {"columnas": columnas, "prospectos": prospectos}

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

@app.post("/api/borrar_permanente")
def borrar_permanente(datos: dict):
    supabase.table('prospectos').delete().eq('nombre', datos["nombre"]).execute()
    return {"status": "ok"}

@app.post("/api/crear_columna")
def crear_columna(datos: dict):
    supabase.table('configuracion').insert({'nombre_columna': datos["nombre"]}).execute()
    return {"status": "ok"}

@app.post("/api/borrar_columna")
def borrar_columna(datos: dict):
    supabase.table('configuracion').delete().eq('nombre_columna', datos["nombre"]).execute()
    return {"status": "ok"}

@app.post("/api/enviar_mensaje")
def enviar_mensaje_whatsapp(datos: MensajeSaliente):
    # Buscar el teléfono en la nube
    res = supabase.table('prospectos').select('telefono').eq('nombre', datos.cliente).neq('telefono', None).order('id', desc=True).limit(1).execute()
    telefono_cliente = res.data[0]['telefono'] if res.data else None
    
    # Guardar tu respuesta
    datos_guardar = {
        "nombre": datos.cliente, "telefono": telefono_cliente, "origen": "WHATSAPP", 
        "mensaje": f"TÚ: {datos.texto}", "columna": "Respondió"
    }
    supabase.table('prospectos').insert(datos_guardar).execute()
    
    if telefono_cliente:
        disparar_whatsapp_real(telefono_cliente, datos.texto)
    return {"status": "enviado"}

# ==========================================
# 🔗 WEBHOOK DE META
# ==========================================
@app.get("/webhook")
def verificar_webhook(request: Request):
    VERIFY_TOKEN = "mi_contrasena_secreta_fantasy" 
    modo = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    desafio = request.query_params.get("hub.challenge")

    if modo and token and modo == "subscribe" and token == VERIFY_TOKEN:
        return PlainTextResponse(content=desafio, status_code=200)
    return PlainTextResponse(content="Servidor Activo Cloud.", status_code=200)

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
                telefono = msg["from"] 
                
                # Parche de enrutamiento +52 1
                if telefono.startswith("521") and len(telefono) == 13:
                    telefono = "52" + telefono[3:]
                    
                tipo = msg.get("type", "text")
                texto = msg["text"]["body"] if tipo == "text" else f"[Archivo Multimedia: {tipo}]"
                
                # Obtener columna actual de la nube
                res = supabase.table('prospectos').select('columna').eq('nombre', nombre).order('id', desc=True).limit(1).execute()
                columna_actual = res.data[0]['columna'] if res.data else "Bandeja Nueva"
                if columna_actual == "Respondió": columna_actual = "Bandeja Nueva"
                
                # Guardar mensaje entrante
                datos_guardar = {
                    "nombre": nombre, "telefono": telefono, "origen": "WHATSAPP", 
                    "mensaje": texto, "columna": columna_actual
                }
                supabase.table('prospectos').insert(datos_guardar).execute()
                print(f"[☁️ NUBE] {nombre}: {texto}")
                
                if columna_actual in ["Bandeja Nueva", "Primer Contacto"] and tipo == "text":
                    procesar_respuesta_bot(nombre, telefono, texto)
                    
        return PlainTextResponse(content="EVENT_RECEIVED", status_code=200)
    except Exception as e:
        print(f"[ERROR WEBHOOK] {e}")
        return PlainTextResponse(content="ERROR", status_code=500)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)