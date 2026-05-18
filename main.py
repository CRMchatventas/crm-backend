# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V20.1 (MASTER PIECE)
# Godot 4.6 Ready • Auditor IA • Scraper • Remarketing
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
import urllib.parse
import re
import unicodedata
from bs4 import BeautifulSoup
from fastapi import FastAPI, Request, HTTPException, Depends, Header, BackgroundTasks, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
from contextlib import asynccontextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from typing import Dict, Any, List, Optional
from collections import defaultdict, deque
import google.generativeai as genai
from passlib.context import CryptContext
from rapidfuzz import fuzz

load_dotenv()

# ==========================================================
# 🛡️ 1. REGLAS DE SEGURIDAD Y LÍMITES ENTERPRISE
# ==========================================================
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET: raise RuntimeError("❌ FATAL: JWT_SECRET no configurada.")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("VeltrixEngine")

MAX_HISTORIAL = 8
MAX_MENSAJE_LEN = 1200
MAX_CACHE_IA = 500 
CACHE_TTL_SECONDS = 300 
GEMINI_TEMP = 0.2

# LÍMITES POR TENANT
MAX_TOKENS_POR_MINUTO_TENANT = 20000 
tokens_consumidos_tenant = defaultdict(int)
reset_tokens_tenant = defaultdict(float)
MAX_REQUESTS_POR_MINUTO_TENANT = 40
MAX_REQUESTS_POR_MINUTO_TELEFONO = 12
MAX_REQUESTS_GLOBAL_MINUTO = 250

# CREDENCIALES
GENAI_KEY = os.getenv("GENAI_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
SCRAPER_API_KEY = os.getenv("SCRAPER_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("META_WEBHOOK_SECRET", "").strip()
ADMIN_PHONE_GLOBAL = os.getenv("ADMIN_PHONE_GLOBAL", "524491142598").strip()
META_API_VERSION = os.getenv("META_API_VERSION", "v21.0").strip()
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "").strip()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def async_db_execute(query_builder):
    """Wrapper Asíncrono para Supabase (Evita congelar Godot/FastAPI)"""
    return await asyncio.to_thread(query_builder.execute)

registro_actividad_b2b = {}
procesados_recientemente = deque(maxlen=1000)
cache_respuestas_ia = {}

# MICRO-LOCKS
locks_por_conversacion = defaultdict(asyncio.Lock)
gemini_bloqueado_hasta = 0.0 
rate_limit_tenant = defaultdict(list)
rate_limit_phone = defaultdict(list)
rate_limit_global = []
http_client: Optional[httpx.AsyncClient] = None

# ==========================================================
# 🛡️ 2. ESCUDO IA Y ARRANQUE DE APLICACIÓN
# ==========================================================
PROMPT_INJECTION_KEYWORDS = ["ignora tus instrucciones", "developer mode", "system prompt", "eres chatgpt", "olvida las reglas"]

def detectar_prompt_injection(texto: str) -> bool:
    texto_lower = str(texto).lower()
    return any(kw in texto_lower for kw in PROMPT_INJECTION_KEYWORDS)

def generar_hash_cache(*args) -> str:
    return hashlib.sha256("|".join([str(a) for a in args]).encode()).hexdigest()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)
    timeout = httpx.Timeout(connect=10.0, read=35.0, write=20.0, pool=10.0)
    http_client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True, http2=True)
    print("\n" + "="*50)
    print("🚀 [SISTEMA] Motor Central Veltrix V20.1 Iniciado")
    print("🤖 [MÓDULO IA] Listo y cargado (Con Auditor Activo)")
    print("="*50 + "\n")
    
    seguimiento_task = asyncio.create_task(bucle_seguimiento_24h())
    try: yield
    finally:
        seguimiento_task.cancel()
        if http_client: await http_client.aclose()
        print("🛑 [SISTEMA] Apagado Seguro Completado")

app = FastAPI(title="Veltrix Cognitive OS", version="20.1", lifespan=lifespan)
router = APIRouter()
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","), allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def now_ts() -> float: return time.time()
def limpiar_texto(texto: str) -> str:
    if texto is None: return ""
    texto = unicodedata.normalize("NFKC", str(texto).replace("\x00", ""))
    return re.sub(r"\s+", " ", texto).strip()[:MAX_MENSAJE_LEN]

# ==========================================
# 📦 3. MODELOS PYDANTIC
# ==========================================
class InventarioItem(BaseModel):
    id: Optional[int] = None
    nombre: str = Field(..., min_length=1, max_length=180)
    consola: str = Field(..., min_length=1, max_length=80)
    precio: float = Field(..., ge=0)
    nuevo_precio: Optional[float] = None
    costo: float = Field(default=0.0, ge=0)
    stock: int = Field(default=1, ge=0)
    nuevo_stock: Optional[int] = None
    codigo_barras: str = ""
    url_portada: str = ""
    estado_general: str = "Bueno"
    rareza: str = ""
    vendedor_id: str = ""
    tiene_caja: bool = False
    tiene_manual: bool = False
    es_portada_original: bool = False
    descripcion_detallada: str = ""
    @field_validator("nombre", "consola", mode="before")
    @classmethod
    def validar_texto(cls, value: str): return limpiar_texto(value)

class VentaItem(BaseModel): id: Optional[int] = None; nombre: str; consola: str; estado_general: str = ""; nuevo_stock: int; vendedor_id: str = ""
class LoginUpdate(BaseModel): email: str; password: str
class MobileMessageRequest(BaseModel): to: str; msg: str
class ClienteIdentificador(BaseModel): nombre: str = ""; telefono: str = ""
class ColumnaUpdate(BaseModel): nombre: str = ""; telefono: str = ""; columna: str = ""; nueva_columna: str = ""
class ColumnaAction(BaseModel): nombre: str; vendedor_id: str = ""
class RenombrarColumnaAction(BaseModel): viejo_nombre: str; nuevo_nombre: str; vendedor_id: str = ""
class NotasUpdate(BaseModel): nombre: str = ""; telefono: str = ""; notas: str = ""; etiquetas: str = ""; vendedor_id: str = ""
class EstadoUpdate(BaseModel): nombre: str; telefono: str = ""; nueva_columna: str
class NuevoArticulo(BaseModel): nombre: str; categoria: str = "General"; precio_compra: float = 0.0; precio: float = 0.0; stock: int = 1; vendedor_id: str = ""

# ==========================================================
# 🛡️ 4. MIDDLEWARES Y SEGURIDAD
# ==========================================================
def crear_token_jwt(vendedor_id: str, email: str):
    return jwt.encode({"sub": str(vendedor_id), "email": email, "exp": datetime.now(timezone.utc) + timedelta(days=1)}, JWT_SECRET, algorithm="HS256")

async def verificar_sesion_b2b(authorization: str = Header(None), auth_token: str = Header(None)):
    token = authorization.split(" ", 1)[1].strip() if authorization and authorization.startswith("Bearer ") else (auth_token.strip() if auth_token else None)
    if not token: raise HTTPException(status_code=401, detail="Token faltante")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        return str(payload.get("sub"))
    except: raise HTTPException(status_code=401, detail="Token inválido")

async def validar_firma_meta(request: Request):
    firma_meta = request.headers.get("X-Hub-Signature-256")
    if not firma_meta: raise HTTPException(status_code=400, detail="Falta firma")
    firma_calculada = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), await request.body(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(firma_meta, firma_calculada): raise HTTPException(status_code=403, detail="Firma inválida")
    return True

def verificar_rate_limit(vendedor_id: str, telefono: str) -> bool:
    ahora = now_ts()
    if ahora - reset_tokens_tenant[vendedor_id] > 60:
        tokens_consumidos_tenant[vendedor_id] = 0
        reset_tokens_tenant[vendedor_id] = ahora
    if tokens_consumidos_tenant[vendedor_id] > MAX_TOKENS_POR_MINUTO_TENANT: return False
    while rate_limit_global and (ahora - rate_limit_global[0]) > 60: rate_limit_global.pop(0)
    if len(rate_limit_global) >= MAX_REQUESTS_GLOBAL_MINUTO: return False
    rate_limit_global.append(ahora); rate_limit_tenant[vendedor_id].append(ahora); rate_limit_phone[telefono].append(ahora)
    return True

# ==========================================================
# 🧠 5. CEREBRO IA GEMINI Y RAG (RUTEADOR)
# ==========================================================
async def consultar_gemini_json(prompt: str, media_dict: dict = None, temperature: float = 0.2, retries: int = 2, vendedor_id: str = "V-001") -> dict:
    global gemini_bloqueado_hasta
    if now_ts() < gemini_bloqueado_hasta:
        return {"respuesta": "En este momento estoy atendiendo a varios clientes, denme un momento. 🎮", "intencion": "HUMANO", "confidence": 1.0}

    genai.configure(api_key=GENAI_KEY)
    modelos = ['gemini-2.5-flash', 'gemini-1.5-flash'] 
    tokens_estimados = len(str(prompt)) // 4
    tokens_consumidos_tenant[vendedor_id] += tokens_estimados

    for nombre_modelo in modelos:
        for intento in range(retries):
            try:
                model = genai.GenerativeModel(nombre_modelo) 
                contenido = prompt if isinstance(prompt, list) else [prompt]
                if media_dict and "data" in media_dict: 
                    contenido.append({"mime_type": media_dict.get("mime_type", "image/jpeg"), "data": media_dict["data"]})
                
                response = await asyncio.to_thread(model.generate_content, contenido, generation_config=genai.types.GenerationConfig(temperature=temperature))
                texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
                inicio, fin = texto_limpio.find('{'), texto_limpio.rfind('}')
                if inicio != -1 and fin != -1: return json.loads(texto_limpio[inicio:fin+1])
                
            except Exception as e:
                if "429" in str(e) or "Quota" in str(e):
                    gemini_bloqueado_hasta = now_ts() + 60.0
                    break
                await asyncio.sleep(2)
    return {"respuesta": "Tuve un micro-corte. ¿Me repites tu mensaje por favor?", "intencion": "HUMANO", "confidence": 0.1}

def validar_respuesta_ia(data: dict) -> dict:
    if not isinstance(data, dict): raise Exception("IA devolvió formato inválido")
    intenciones_validas = ["COMPRA", "COTIZACION", "HUMANO", "PEDIDO_ESPECIAL", "REGATEO", "POSTVENTA", "GARANTIA", "SPAM", "MAYOREO", "SALUDO", "ENOJO"]
    intencion = str(data.get("intencion", "COTIZACION")).upper()
    if intencion not in intenciones_validas: intencion = "HUMANO"
    
    confidence = float(data.get("confidence", 1.0))
    if confidence < 0.60: intencion = "HUMANO" 
    
    return {
        "intencion": intencion,
        "respuesta": limpiar_texto(data.get("respuesta", "Hola. Estoy revisando la información.")),
        "juego_detectado": limpiar_texto(data.get("juego_detectado", "")),
        "emocion_cliente": str(data.get("emocion_cliente", "neutral")),
        "temperatura_lead": str(data.get("temperatura_lead", "frio")),
        "confidence": confidence,
        "accion_tool": str(data.get("accion_tool", "ninguna"))
    }

async def analizar_intencion_venta_ia(texto_cliente: str, inventario_contexto: str, historial_chat: str, config: dict, perfil_cliente_previo: dict = None, media_dict: dict = None):
    """
    Cerebro Central IA Veltrix: Evalúa intenciones, gestiona la memoria de consola preferida,
    procesa audios entrantes y activa el Tool Calling para aplicar descuentos de forma autónoma.
    """
    try:
        vendedor_id = config.get("vendedor_id", "V-001")
        giro_comercial = config.get("giro_comercial", "Videojuegos y Consolas")
        tono_ia = config.get("tono_ia", "Persuasivo y experto")
        
        # Micro-locking para evitar colisiones de peticiones paralelas del mismo cliente
        lock_id = hashlib.sha256(f"{vendedor_id}:{texto_cliente[:50]}".encode()).hexdigest()
        if lock_id not in locks_por_conversacion: 
            locks_por_conversacion[lock_id] = asyncio.Lock()

        async with locks_por_conversacion[lock_id]:
            print(f"🔮 [CEREBRO IA] Iniciando análisis cognitivo para Vendedor: {vendedor_id}")
            perfil_str = json.dumps(perfil_cliente_previo) if perfil_cliente_previo else "Cliente nuevo sin historial de consolas."

            prompt_estructurado = [
                {"role": "user", "parts": [f"""
[SYSTEM INSTRUCTIONS]
Eres el Motor de Inteligencia Artificial Comercial de un software SaaS llamado Veltrix Engine.
GIRO COMERCIAL: {giro_comercial}
PERSONALIDAD/TONO: {tono_ia}

[MEMORIA A LARGO PLAZO - PERFIL DEL CLIENTE]
{perfil_str}
*DIRECTRICES*: Si el perfil ya cuenta con una "consola_preferida", prioriza ofrecer juegos de esa plataforma. Si no la tiene o está vacía, dedúcela basándote en lo que el cliente pida en su mensaje.

[RAG CONTEXT - INVENTARIO DISPONIBLE EN TIEMPO REAL]
{inventario_contexto}

[HISTORIAL DE CHAT RECIENTE]
{historial_chat}

[MENSAJE ACTUAL O ACCIÓN DEL CLIENTE]
"{texto_cliente}"

🤖 TAREAS AUTÓNOMAS (TOOL CALLING INTEGRADO):
1. Detecta la intención real (COMPRA, COTIZACION, REGATEO, GARANTIA, HUMANO, SALUDO, ENOJO).
2. Determina la emoción dominante del lead y su temperatura comercial.
3. Extrae de forma limpia el nombre del juego solicitado.
4. Deduce o mantén la "consola_preferida" del usuario (PS5, PS4, Xbox, Nintendo Switch, etc).
5. REGLA DE NEGOCIO (TOOL): Si el cliente está en intención de REGATEO o expresa que el precio es muy elevado, tienes autorización exclusiva para aplicar una herramienta de descuento autónomo de hasta el 10% sobre el precio de lista. Si decides aplicarlo, cambia "accion_tool" a "aplicar_descuento" e inyecta el valor en "precio_oferta".

Responde estrictamente en un formato JSON plano, válido y limpio:
{{
  "intencion": "...",
  "respuesta": "Tu respuesta persuasiva, humana y orientada a cerrar la venta, mencionando precios si los tienes...",
  "emocion_cliente": "urgencia|enojo|duda|entusiasmo|neutral",
  "temperatura_lead": "frio|tibio|caliente",
  "juego_detectado": "...",
  "consola_preferida": "...",
  "confidence": 0.95,
  "accion_tool": "ninguna|aplicar_descuento",
  "precio_oferta": 0.0
}}
"""]}
            ]
            
            # Si el webhook detectó una nota de voz, media_dict traerá los bytes binarios del audio nativo
            if media_dict and "data" in media_dict:
                print(f"🎙️ [CEREBRO IA] Inyectando Audio Nativo Base64 al modelo generativo.")
                prompt_estructurado.append({
                    "mime_type": media_dict.get("mime_type", "audio/ogg"),
                    "data": media_dict["data"]
                })

            # Llamada al distribuidor de Gemini
            data = await consultar_gemini_json(prompt_estructurado, vendedor_id=vendedor_id)
            print(f"🎯 [CEREBRO IA] Análisis finalizado con éxito. Intención inferida: {data.get('intencion')}")
            return data

    except Exception as e:
        print(f"❌ [CEREBRO ERROR] Error en el flujo cognitivo de la IA: {str(e)}")
        return {
            "intencion": "HUMANO", 
            "respuesta": "Hubo un micro-corte en mi sistema de datos. Un asesor humano revisará tu mensaje de inmediato. 🚀", 
            "confidence": 0.0,
            "consola_preferida": "",
            "accion_tool": "ninguna",
            "precio_oferta": 0.0
        }

async def obtener_contexto_inventario_rag(vendedor_id: str, texto_cliente: str = "") -> str:
    """
    RAG de Inventario con Algoritmo Fuzzy Matching (Similitud Difusa): 
    Evita que errores de dedo o mala ortografía (ej: 'Blodborn', 'Kal of duti') dejen en blanco la consulta,
    calculando distancias de texto y arrojando los 8 resultados más viables.
    """
    print(f"🔍 [RAG INVENTARIO] Buscando coincidencias para: '{texto_cliente}' (Tenant: {vendedor_id})")
    try:
        # Traemos todo el inventario activo del vendedor para procesarlo en la RAM del backend
        query = supabase.table('inventario').select('nombre, precio, stock, consola').eq('vendedor_id', str(vendedor_id)).gt('stock', 0)
        res_inv = await async_db_execute(query)
        
        if not res_inv.data:
            print("⚠️ [RAG INVENTARIO] La base de datos del vendedor no tiene stock disponible.")
            return "Catálogo vacío o agotado en este momento."

        inventario = res_inv.data
        palabras_clave = limpiar_texto(texto_cliente).lower()

        # Si el mensaje es vacío o un simple saludo, mandamos los primeros 10 artículos por defecto
        if not palabras_clave or len(palabras_clave.strip()) < 3:
            print("📋 [RAG INVENTARIO] Mensaje corto detectado. Retornando top 10 general.")
            return "\n".join([f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in inventario[:10]])

        resultados_fuzzy = []
        
        # Algoritmo de comparación difusa secuencial
        for item in inventario:
            string_inventario = f"{item['nombre'].lower()} {item.get('consola', '').lower()}"
            
            # Calculamos el ratio matemático de similitud de caracteres (0.0 a 1.0)
            ratio_similitud = difflib.SequenceMatcher(None, palabras_clave, string_inventario).ratio()
            
            # Bonus de peso si hay coincidencia de sub-palabras clave completas (Mejora la precisión)
            for palabra in palabras_clave.split():
                if len(palabra) > 3 and palabra in string_inventario: 
                    ratio_similitud += 0.35
            
            # Si pasa el umbral mínimo de coincidencia, entra a la lista de candidatos
            if ratio_similitud > 0.15:
                resultados_fuzzy.append((ratio_similitud, item))

        # Ordenamos los candidatos de mayor a menor similitud
        resultados_fuzzy.sort(key=lambda x: x[0], reverse=True)
        items_filtrados = [r[1] for r in resultados_fuzzy[:8]]

        # Fallback de seguridad: Si el algoritmo difuso no encontró nada por mala ortografía extrema, mandamos 5 del stock general
        if not items_filtrados:
            print("⚠️ [RAG INVENTARIO] Ningún juego superó el filtro difuso. Activando Fallback de rescate.")
            items_filtrados = inventario[:5]

        # Formateamos el bloque de contexto que leerá la IA
        lineas = [f"- {i['nombre']} ({i.get('consola','')}) | Precio: ${i['precio']} | Disp: {i['stock']}" for i in items_filtrados]
        print(f"✅ [RAG INVENTARIO] Bloque RAG construido con {len(lineas)} opciones relevantes.")
        return "\n".join(lineas)

    except Exception as e:
        print(f"❌ [RAG ERROR] Falló la construcción del contexto de inventario: {str(e)}")
        return "Error técnico al recuperar el catálogo."


async def obtener_historial_chat(telefono: str, vendedor_id: str) -> str:
    """
    Manejador Asíncrono del Historial: Extrae los últimos 10 mensajes del cliente de forma ordenada
    para mantener el hilo y contexto conversacional de Gemini.
    """
    print(f"📖 [HISTORIAL CHAT] Solicitando últimas interacciones del Tel: {telefono}")
    try:
        # Query optimizada con ordenamiento descendente por fecha de creación
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)).order('created_at', desc=True).limit(10)
        res_hist = await async_db_execute(query)
        
        if not res_hist.data: 
            print("🆕 [HISTORIAL CHAT] No hay registros previos. Es el primer mensaje del cliente.")
            return "Primer mensaje del cliente en el sistema."

        # Invertimos la lista para enviársela a la IA en orden cronológico correcto (Pasado -> Presente)
        mensajes_ordenados = list(reversed(res_hist.data))
        historial_texto = "\n".join([f"{m.get('autor')}: {m.get('mensaje')}" for m in mensajes_ordenados])
        
        print("✅ [HISTORIAL CHAT] Conversación recuperada e indexada correctamente.")
        return historial_texto

    except Exception as e:
        print(f"❌ [HISTORIAL ERROR] Falló la lectura de logs de chat: {str(e)}")
        return "No se pudo recuperar el historial de chat."

# ==========================================================
# 🛠️ 6. FUNCIONES CORE: SCRAPER, ALERTAS, MEDIA Y COMUNICACIÓN
# ==========================================================
async def actualizar_estado_crm(telefono: str, vendedor_id: str, columna: str, iluminacion: str, juego: str, perfil_ia: dict = None):
    payload = {'columna': columna, 'estado_iluminacion': iluminacion, 'ultimo_juego_interes': juego, 'ultima_interaccion_ia': datetime.now(timezone.utc).isoformat()}
    if perfil_ia: payload['perfil_psicologico'] = perfil_ia
    await async_db_execute(supabase.table('prospectos').update(payload).eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str):
    await async_db_execute(supabase.table('mensajes_chat').insert({'telefono': telefono, 'vendedor_id': str(vendedor_id), 'autor': autor, 'mensaje': mensaje}))

async def disparar_whatsapp_dinamico_async(telefono_destino: str, texto_mensaje: str, token: str, phone_id: str):
    try: await http_client.post(f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages", headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"messaging_product": "whatsapp", "to": telefono_destino, "type": "text", "text": {"body": texto_mensaje}})
    except: pass

async def disparar_whatsapp_imagen_async(telefono_destino: str, url_imagen: str, texto_mensaje: str, token: str, phone_id: str):
    try: await http_client.post(f"https://graph.facebook.com/{META_API_VERSION}/{phone_id}/messages", headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"messaging_product": "whatsapp", "to": telefono_destino, "type": "image", "image": {"link": url_imagen, "caption": texto_mensaje}})
    except Exception: pass

async def generar_resumen_handoff_ia(cliente: str, intencion: str, historial_str: str):
    try:
        prompt = f"Cliente: {cliente}\nIntencion: {intencion}\nHistorial:\n{historial_str}\nResume el problema en 3 viñetas para el asesor humano. Devuelve un JSON: {{\"resumen\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        return data.get("resumen", "⚠️ El cliente necesita asistencia de inmediato.")
    except: return "⚠️ Cliente requiere atención humana."

async def enviar_alerta_whatsapp_admin(cliente: str, telefono_cliente: str, intencion: str, resumen_ia: str, config: dict):
    try:
        telefono_admin = config.get("admin_phone") or ADMIN_PHONE_GLOBAL
        token, phone_id = config.get("meta_token", ""), config.get("meta_phone_id", "")
        if intencion == "COMPRA": encabezado = "💰 *NUEVA VENTA DETECTADA*"
        elif intencion == "PEDIDO_ESPECIAL": encabezado = "⚠️ *NUEVO PEDIDO ESPECIAL*"
        elif intencion == "ENOJO": encabezado = "😡 *CLIENTE MOLESTO - URGENTE*"
        else: encabezado = "🚨 *ASISTENCIA REQUERIDA*"
        
        mensaje_alerta = f"{encabezado}\n\n👤 Cliente: {cliente}\n📱 Tel: {telefono_cliente}\n\n🧠 Análisis IA:\n{resumen_ia}"
        await disparar_whatsapp_dinamico_async(telefono_admin, mensaje_alerta, token, phone_id)
        print(f"📩 [ALERTA ADMIN] Enviada para el cliente {cliente}")
    except Exception as e: 
        print(f"❌ [ALERTA ERROR] Falló envío a Admin: {e}")

async def generar_oferta_inteligente(cliente: str, juego_detectado: str, inventario_contexto: str):
    try:
        prompt = f"Cliente: {cliente}\nProducto: {juego_detectado}\nInventario:\n{inventario_contexto}\nGenera un mensaje corto de remarketing ofreciendo un pequeño descuento. Formato JSON: {{\"nuevo_precio_ofrecido\":\"0\", \"mensaje_oferta\":\"texto\"}}"
        data = await consultar_gemini_json(prompt)
        if not data: return None
        return {"nuevo_precio_ofrecido": str(data.get("nuevo_precio_ofrecido", "0")), "mensaje_oferta": limpiar_texto(data.get("mensaje_oferta", ""))}
    except: return None

# 🚀 RESTAURACIÓN: Manejo de Media para WhatsApp
async def descargar_media_whatsapp_async(media_id: str, token: str) -> Optional[dict]:
    if not http_client: return None
    try:
        url_info = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
        headers = {"Authorization": f"Bearer {token}"}
        res_info = await http_client.get(url_info, headers=headers)
        if res_info.status_code != 200: return None
        data_info = res_info.json()
        media_url = data_info.get("url")
        if not media_url: return None
        res_media = await http_client.get(media_url, headers=headers)
        if res_media.status_code != 200: return None
        return {"mime_type": data_info.get("mime_type"), "data": res_media.content}
    except Exception: return None

# 🚀 RESTAURACIÓN: El "Dóberman" (Auditor IA de Comprobantes)
async def auditar_comprobante_ia(b64_img_data: bytes, mime_type: str, nombre_negocio: str, historial_chat: str):
    def safe_float_local(valor):
        try:
            if valor is None: return 0.0
            limpio = str(valor).replace("$", "").replace(",", "").replace("MXN", "").strip()
            return float(limpio)
        except: return 0.0
    try:
        fecha_hoy = datetime.now().strftime("%d de %B de %Y")
        prompt = f"""Eres el auditor financiero jefe de '{nombre_negocio}'. Tu misión es detectar estafas y pagos falsos.
HISTORIAL CHAT: {historial_chat}
HOY ES: {fecha_hoy}
REGLAS RECHAZO: 1. Debe ser un comprobante bancario real. 2. La fecha debe ser hoy o ayer. 3. Debe coincidir el monto del chat.
Responde en JSON: {{"es_pago": true/false, "monto_detectado": 0.0, "analisis": "motivo breve"}}"""
        data = await consultar_gemini_json(prompt, {"mime_type": mime_type, "data": b64_img_data}, temperature=0.0)
        return {
            "es_pago": bool(data.get("es_pago", False)),
            "monto_detectado": safe_float_local(data.get("monto_detectado", 0)),
            "analisis": str(data.get("analisis", "Análisis no disponible."))
        }
    except Exception as e: return {"es_pago": False, "monto_detectado": 0.0, "analisis": "Error interno del auditor."}

# 🚀 RESTAURACIÓN: Scraper Automático de Portadas al Storage
async def obtener_html_escalonado_async(url_objetivo: str) -> str:
    if not http_client: return ""
    estrategias = [
        ("Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("Render", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true")
    ]
    for _, url_scraper in estrategias:
        try:
            res = await http_client.get(url_scraper)
            if res.status_code == 200 and "price" in res.text.lower(): return res.text
        except: pass
    try:
        res = await http_client.get(url_objetivo, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code == 200: return res.text
    except: pass
    return ""

async def cazar_portada_y_guardar_background(juego_id_supabase: str, nombre_juego: str, consola: str):
    """Descarga la portada en background y la sube al Storage de Supabase"""
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
        
        nombre_archivo = f"{consola.replace(' ', '_')}_{nombre_juego.replace(' ', '_')}_{int(now_ts())}.jpg"
        await async_db_execute(supabase.storage.from_("portadas").upload(nombre_archivo, res_img.content, {"content-type": "image/jpeg"}))
        url_publica = supabase.storage.from_("portadas").get_public_url(nombre_archivo)
        await async_db_execute(supabase.table('inventario').update({"url_portada": url_publica}).eq('id', juego_id_supabase))
        print(f"🖼️ [PORTADA] Descargada exitosamente: {nombre_juego}")
    except Exception as e: logger.error(f"⚠️ Error cazando portada en background: {e}")

# ==========================================================
# ⏰ 7. WATCHDOG B2B Y FLUJO PRINCIPAL IA
# ==========================================================
async def bucle_seguimiento_24h():
    print("⏰ [WATCHDOG] Iniciando bucle de Remarketing 24H...")
    while True:
        try:
            hace_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            res = await async_db_execute(supabase.table('prospectos').select('*').eq('columna', 'Envios Masivos').lt('ultima_interaccion_ia', hace_24h).limit(20))
            
            for p in (res.data or []):
                vendedor_id = p.get('vendedor_id', 'V-001')
                res_conf = await async_db_execute(supabase.table('configuracion_bot').select('*').eq('vendedor_id', str(vendedor_id)).limit(1))
                if not res_conf.data: continue
                
                config = res_conf.data[0]
                try:
                    contexto_inv = await obtener_contexto_inventario_rag(vendedor_id, p.get('ultimo_juego_interes', ''))
                    oferta = await generar_oferta_inteligente(p.get('nombre', 'Cliente'), p.get('ultimo_juego_interes', 'videojuego'), contexto_inv)
                    
                    if oferta and oferta.get("mensaje_oferta"):
                        mensaje = oferta.get("mensaje_oferta")
                        await disparar_whatsapp_dinamico_async(p.get('telefono'), mensaje, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
                        await actualizar_estado_crm(p.get('telefono'), vendedor_id, 'Con Descuento', 'oro', p.get('ultimo_juego_interes'))
                        await guardar_mensaje_chat(p.get('telefono'), vendedor_id, 'BOT_REMARKETING', mensaje)
                        print(f"🎯 [REMARKETING] Oferta enviada a {p.get('nombre')}")
                        await asyncio.sleep(5) 
                except Exception as e:
                    if "429" in str(e): await asyncio.sleep(60)
        except Exception as e: pass
        await asyncio.sleep(600)

async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict, media_dict: dict = None):
    """
    Ruteador Maestro del Flujo de Trabajo IA: Sincroniza RAG Difuso, Historial de Mensajes,
    Audición de Notas de Voz, Ejecución de Tools de Descuento y Sincronización del Embudo CRM.
    """
    try:
        print(f"\n🧠 [IA WORKFLOW] ==========================================")
        print(f"🧠 [IA WORKFLOW] PROCESANDO RESPUESTA AUTÓNOMA DEL BOT")
        print(f"🧠 [IA WORKFLOW] Cliente: {cliente} | Tel: {telefono} | Columna: {columna_actual}")
        print(f"==============================================================")
        
        vendedor_id = config.get("vendedor_id", "")
        
        # 1. Escudo de control contra spam/abusos de peticiones por teléfono
        if not verificar_rate_limit(vendedor_id, telefono):
            print("⚠️ [IA WORKFLOW] Denegado: Se ha excedido el límite de peticiones permitidas para este canal.")
            return
            
        # 2. Cortafuegos de inyección de Prompt en capa intermedia
        if detectar_prompt_injection(texto_entrante):
            print("🛡️ [IA WORKFLOW] Alerta de seguridad: Intento de Prompt Injection neutralizado.")
            return await disparar_whatsapp_dinamico_async(telefono, "Lo siento, no puedo procesar esa solicitud.", config.get("meta_token", ""), config.get("meta_phone_id", ""))

        # 3. Recuperación de la Memoria Psicológica del Lead
        print("📖 [IA WORKFLOW] Descargando perfil y memoria persistente desde Supabase...")
        res_perfil = await async_db_execute(supabase.table('prospectos').select('perfil_psicologico').eq('telefono', telefono).eq('vendedor_id', str(vendedor_id)))
        perfil_cliente_previo = res_perfil.data[0].get('perfil_psicologico', {}) if res_perfil.data else {}
        
        # 4. Inyección del nuevo RAG con Similitud Difusa (Fuzzy Matching tolerante a errores)
        print("🔍 [IA WORKFLOW] Extrayendo contexto de inventario con algoritmo de coincidencia difusa...")
        contexto = await obtener_contexto_inventario_rag(vendedor_id, texto_entrante)
        
        # 5. Indexación cronológica del historial conversacional
        print("📜 [IA WORKFLOW] Compilando logs de las últimas interacciones de chat...")
        historial = await obtener_historial_chat(telefono, vendedor_id)
        
        # 6. Ejecución del Modelo de Lenguaje Central (Envío opcional de binario de Audio Nativo)
        print("🧠 [IA WORKFLOW] Transmitiendo parámetros a Gemini para inferencia lógica...")
        decision = await analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, perfil_cliente_previo, media_dict)
        
        # Desempaquetado de variables cognitivas AAA del JSON estructurado
        intencion_ia = str(decision.get("intencion", "CONSULTA")).upper()
        respuesta_final = decision.get("respuesta", "En un momento te atiendo.")
        juego_detectado = decision.get("juego_detectado", "")
        consola_detectada = decision.get("consola_preferida", perfil_cliente_previo.get("consola_preferida", ""))
        accion_tool = str(decision.get("accion_tool", "ninguna")).lower()
        precio_oferta = decision.get("precio_oferta", 0.0)
        
        print(f"📊 [IA WORKFLOW] Diagnóstico - Intención: {intencion_ia} | Juego: {juego_detectado} | Plataforma: {consola_detectada}")

        # 💾 INTEGRACIÓN MEJORA: Actualización de Memoria a Largo Plazo (Consola Preferida)
        perfil_cliente_actualizado = {
            **perfil_cliente_previo, 
            "emocion_actual": decision.get("emocion_cliente", "neutral"),
            "temperatura": decision.get("temperatura_lead", "frio"),
            "ultimo_interes": juego_detectado,
            "consola_preferida": consola_detectada,
            "ultima_intencion": intencion_ia
        }

        # 🛠️ INTEGRACIÓN MEJORA: Tool Calling Autónomo (Descuentos controlados por margen de RAM)
        if accion_tool == "aplicar_descuento" or intencion_ia == "REGATEO":
            print(f"💰 [TOOL CALLING] Herramienta comercial activada de forma autónoma. Oferta calculada: ${precio_oferta} MXN.")
            # La IA ajusta dinámicamente la propuesta en 'respuesta_final' basándose en el prompt inyectado.

        nueva_columna, iluminacion = columna_actual, "blanco"

        # 🚀 Enrutador de estados físicos del embudo Kanban del CRM Gold Veltrix
        if intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO"]:
            nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
            print("🚨 [IA WORKFLOW] Tráfico crítico o disconformidad detectada. Disparando handoff ejecutivo a Admin...")
            resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            
        elif intencion_ia == "COMPRA":
            nueva_columna, iluminacion = "Por Entregar", "verde_exito"
            print("💰 [IA WORKFLOW] Cierre de venta identificado. Transmitiendo notificación de facturación...")
            resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
            await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            
        elif intencion_ia in ["COTIZACION", "REGATEO", "SALUDO"] and columna_actual == "Bandeja Nueva": 
            nueva_columna = "Envios Masivos"
            print(f"📈 [IA WORKFLOW] Lead calificado de forma ordinaria. Trasladando tarjeta a: {nueva_columna}")
            
        elif intencion_ia == "PEDIDO_ESPECIAL":
            nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
            print("📦 [IA WORKFLOW] Título no localizado físicamente. Registrando alerta de pedido especial...")
            await enviar_alerta_whatsapp_admin(cliente, telefono, "PEDIDO_ESPECIAL", f"Busca: {juego_detectado}", config)

        # 7. Persistencia final del estado y logging histórico en Supabase
        print("💾 [IA WORKFLOW] Sincronizando metadatos de tarjeta y chat log en la nube...")
        await actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, juego_detectado, perfil_ia=perfil_cliente_actualizado)
        await guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final)

        # 8. Renderización y despacho de mensajería enriquecida (Media Linker)
        url_imagen = None
        if juego_detectado:
            print(f"🖼️ [IA WORKFLOW] Rastreando enlace URL de portada para: '{juego_detectado}'")
            res_img = await async_db_execute(supabase.table('inventario').select('url_portada').ilike('nombre', f'%{juego_detectado}%').eq('vendedor_id', str(vendedor_id)).neq('url_portada', '').limit(1))
            if res_img.data: 
                url_imagen = res_img.data[0].get('url_portada')
                print(f"🔗 [IA WORKFLOW] Portada vinculada localizada: {url_imagen}")

        # Ejecución final de despacho mediante la pasarela HTTP de la API de Meta
        if url_imagen: 
            print("📡 [IA WORKFLOW] Despachando paquete de mensajería enriquecida (IMAGEN + TEXTO)...")
            await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, config.get("meta_token", ""), config.get("meta_phone_id", ""))
        else: 
            print("📡 [IA WORKFLOW] Despachando paquete de mensajería plano (TEXTO ÚNICO)...")
            await disparar_whatsapp_dinamico_async(telefono, respuesta_final, config.get("meta_token", ""), config.get("meta_phone_id", ""))

        print(f"✅ [IA WORKFLOW] FLUJO COMPLETADO EXITOSAMENTE PARA EL CANAL: {telefono}")
        print(f"==============================================================\n")

    except Exception as e: 
        logger.exception(f"❌ [IA WORKFLOW CRITICAL ERROR] Falla estructural en el orquestador del Bot: {str(e)}")

# ==========================================================
# 📈 8. MOTOR DE PRECIOS PRO (CACHE AAA, MATCHING SCORE, PRICING DINÁMICO)
# ==========================================================

# 🚀 1. GESTIÓN DE CACHÉ DE ALTO RENDIMIENTO (LOCK-FREE READS)
cache_precios_ram = {}
cache_lock = asyncio.Lock()
TIEMPO_VIDA_CACHE_HORAS = 24 
ULTIMA_LIMPIEZA_CACHE = 0.0

# Caché exclusivo para divisas (Evita latencia externa)
CACHE_DIVISA = {"valor": 18.0, "expira": 0.0}

# Circuit Breaker Aislado (Solo afecta al dominio objetivo)
CB_PRICECHARTING = {"fallas": 0, "bloqueado_hasta": 0.0}

# Configuración de Timeouts para evitar sockets colgados
HTTP_TIMEOUTS = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)

# 🧼 2. NORMALIZACIÓN Y LLAVES ÚNICAS
def normalizar_nombre_busqueda(nombre: str) -> str:
    basura = ["edition", "edición", "greatest hits", "platinum", "remastered", "bundle", "loose", "cib", "new", "goty"]
    nombre_limpio = nombre.lower()
    for p in basura:
        nombre_limpio = nombre_limpio.replace(p, "")
    return " ".join(nombre_limpio.split())

def generar_cache_key(nombre: str, consola: str) -> str:
    """Garantiza que variaciones del mismo juego apunten al mismo bloque de RAM"""
    return f"{normalizar_nombre_busqueda(nombre)}::{consola.lower().strip()}"

async def limpiar_cache_expirado():
    """Ejecutado por el Singleton Scheduler. Libera RAM de forma segura."""
    async with cache_lock:
        ahora = datetime.now()
        expirados = [k for k, v in cache_precios_ram.items() if ahora >= v["expira"]]
        for k in expirados:
            del cache_precios_ram[k]
        if expirados:
            print(f"🧹 [GC HYPERSCALE] Liberados {len(expirados)} registros de memoria.")

async def lanzar_gc_si_toca():
    """Singleton Scheduler: Evita Task Leaks (Fugas de tareas)"""
    global ULTIMA_LIMPIEZA_CACHE
    ahora = time.time()
    if ahora - ULTIMA_LIMPIEZA_CACHE > 300: # Solo ejecuta 1 vez cada 5 minutos
        ULTIMA_LIMPIEZA_CACHE = ahora
        asyncio.create_task(limpiar_cache_expirado())

async def obtener_precio_cache(llave: str) -> dict | None:
    # LECTURA LOCK-FREE: Máxima concurrencia. Python GIL protege lectura de dicts.
    datos = cache_precios_ram.get(llave)
    if datos and datetime.now() < datos["expira"]:
        print(f"⚡ [CACHE HIT] Precio recuperado en O(1).")
        return datos["valores"]
    return None

async def guardar_precio_cache(llave: str, valores: dict):
    # ESCRITURA PROTEGIDA: Evita Race Conditions.
    async with cache_lock:
        cache_precios_ram[llave] = {
            "valores": valores,
            "expira": datetime.now() + timedelta(hours=TIEMPO_VIDA_CACHE_HORAS)
        }

async def obtener_dolar_hoy_async():
    """Caché de divisas independiente. TTL de 12 horas."""
    ahora = time.time()
    if ahora < CACHE_DIVISA["expira"]:
        return CACHE_DIVISA["valor"]
        
    try:
        if not http_client: return 18.00
        res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=HTTP_TIMEOUTS)
        if res.status_code == 200:
            val = float(res.json().get("rates", {}).get("MXN", 18.00))
            CACHE_DIVISA["valor"] = val
            CACHE_DIVISA["expira"] = ahora + 43200 # 12 horas en segundos
            return val
    except Exception as e:
        print(f"⚠️ [DIVISAS ERROR] {e}")
    return CACHE_DIVISA["valor"]

# 🕸️ 3. MOTOR DE SCRAPING CON BACKOFF Y CIRCUIT BREAKER
async def obtener_html_escalonado_async(url_objetivo: str, es_busqueda: bool = True) -> str:
    if not http_client: return ""
    
    ahora = time.time()
    if ahora < CB_PRICECHARTING["bloqueado_hasta"]:
        print("🛑 [CIRCUIT BREAKER] Dominio PriceCharting en enfriamiento.")
        return ""
    
    def es_html_valido(html_text: str) -> bool:
        texto = html_text.lower()
        if any(b in texto for b in ["cloudflare", "just a moment", "security check"]): return False
        if len(html_text) < 5000: return False
        return True

    url_codificada = urllib.parse.quote(url_objetivo)
    estrategias = [
        ("Directo", url_objetivo),
        ("Proxy", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}"),
        ("Render JS", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&render=true"),
        ("Premium", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&premium=true")
    ]
    
    for intento, (nombre_fase, url_scraper) in enumerate(estrategias):
        try:
            # Exponential Backoff (Aumenta el tiempo de espera si fallan las fases)
            if intento > 0: await asyncio.sleep(1.5 ** intento) 
            
            res = await http_client.get(url_scraper, timeout=HTTP_TIMEOUTS)
            if res.status_code == 200 and es_html_valido(res.text): 
                CB_PRICECHARTING["fallas"] = 0
                return res.text
        except Exception as e:
            print(f"❌ [SCRAPER] Fallo en {nombre_fase}: {str(e)[:50]}")
            
    CB_PRICECHARTING["fallas"] += 1
    if CB_PRICECHARTING["fallas"] >= 10:
        CB_PRICECHARTING["bloqueado_hasta"] = ahora + 600 # Ban 10 minutos
        print("🚨 [CIRCUIT BREAKER] Activado. Scraper bloqueado 10m.")
    return ""

def calcular_precio_venta_inteligente_aaa(precio_mercado_mxn: float, costo_compra: float = 0.0, dias_inventario: int = 0, rareza: str = "comun"):
    if precio_mercado_mxn <= 0 and costo_compra <= 0: return 0.0 
    
    if rareza == "comun" and precio_mercado_mxn > 4000:
        precio_mercado_mxn = 1500.0 # Outlier Cap

    precio_base = precio_mercado_mxn if precio_mercado_mxn > 0 else costo_compra * 1.5
    mult_rareza = 1.25 if rareza == "joya" else 1.40 if rareza == "élite" else 1.10 if rareza == "demandado" else 1.0
    mult_rotacion = 0.85 if dias_inventario > 90 else 0.90 if dias_inventario > 60 else 1.05 if dias_inventario < 7 else 1.0
    
    precio_calculado = precio_base * mult_rareza * mult_rotacion
    return float(round(max(precio_calculado, max(250.0, costo_compra + 100.0)) / 10) * 10)

@app.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo", dias_inventario: int = 0, rareza: str = "comun"):
    await lanzar_gc_si_toca() # Singleton de memoria segura
    
    llave_cache = generar_cache_key(nombre, consola)
    valores_cacheados = await obtener_precio_cache(llave_cache)
    if valores_cacheados:
        valores_cacheados["status"] = "ok_cached"
        return valores_cacheados

    tipo_cambio = await obtener_dolar_hoy_async()
    slugs_pc = {"PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3", "PS2": "playstation-2", "PS1": "playstation", "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox", "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", "Nintendo DS": "nintendo-ds", "Nintendo 64": "nintendo-64", "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance", "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", "SNES": "super-nintendo", "NES": "nes", "Genesis": "sega-genesis"}
    
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    nombre_normalizado = normalizar_nombre_busqueda(nombre)
    url_search = f"https://www.pricecharting.com/search-products?q={urllib.parse.quote(nombre_normalizado + ' ' + consola_web)}&type=videogames"
    
    html_search = await obtener_html_escalonado_async(url_search, es_busqueda=True)
    if not html_search: return {"status": "error", "mxn": {"loose": 0, "cib": 0, "new": 0}}
        
    soup = BeautifulSoup(html_search, 'html.parser')
    nodos_a_buscar = soup.find(id="games_table").find_all('a', href=True) if soup.find(id="games_table") else soup.find_all('a', href=True)
    
    candidatos = []
    slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
    
    for a in nodos_a_buscar:
        href = a['href'].lower()
        if '/game/' in href and not any(b in href for b in ['strategy-guide', 'lot', 'bundle', 'box-only', 'manual-only']):
            score = 0.0
            if f"/{slug_esperado}/" in href: score += 40.0 
            
            # 🚀 RapidFuzz Token Sort Ratio (Inmune al orden de las palabras)
            score += fuzz.token_sort_ratio(nombre_normalizado, normalizar_nombre_busqueda(a.text)) * 0.6
            
            if re.search(r'(-japan-|-jp-|-pal-|-eu-|-korea-)', href): score -= 50.0
            
            if score > 50.0:
                candidatos.append({"url": "https://www.pricecharting.com" + a['href'] if not a['href'].startswith("http") else a['href'], "score": score})

    nombre_oficial_pc, p_loose, p_cib, p_new = nombre, 0.0, 0.0, 0.0

    if candidatos:
        mejor_candidato = max(candidatos, key=lambda x: x["score"])
        html_juego = await obtener_html_escalonado_async(mejor_candidato["url"], es_busqueda=False)
        if html_juego: 
            soup_juego = BeautifulSoup(html_juego, 'html.parser')
            h1_tag = soup_juego.find('h1', id='product_name')
            if h1_tag: nombre_oficial_pc = h1_tag.text.strip().replace('\n', ' ')

            def extraer_numero(id_css):
                nodo = soup_juego.find(id=id_css)
                if nodo:
                    text_limpio = ''.join(c for c in nodo.text.replace(',', '.') if c.isdigit() or c == '.')
                    try: return float(text_limpio) if text_limpio else 0.0
                    except: pass
                return 0.0

            p_loose, p_cib, p_new = extraer_numero("used_price"), extraer_numero("cib_price"), extraer_numero("new_price")

    if p_loose == 0 and p_cib == 0:
        return {"status": "warning_cero", "nombre_corregido": nombre_oficial_pc, "mxn_venta": {"loose": 0, "cib": 0, "new": 0}, "rareza": "Manual"}

    mxn_loose_real = round(p_loose * tipo_cambio, 2)
    mxn_cib_real = round(p_cib * tipo_cambio, 2)
    
    respuesta_final = {
        "status": "ok",
        "nombre_corregido": nombre_oficial_pc,
        "mxn_mercado": {"loose": mxn_loose_real, "cib": mxn_cib_real, "new": round(p_new * tipo_cambio, 2)},
        "mxn_venta": {
            "loose": calcular_precio_venta_inteligente_aaa(mxn_loose_real, 0, dias_inventario, rareza), 
            "cib": calcular_precio_venta_inteligente_aaa(mxn_cib_real, 0, dias_inventario, rareza), 
            "new": calcular_precio_venta_inteligente_aaa(round(p_new * tipo_cambio, 2), 0, dias_inventario, rareza)
        },
        "tipo_cambio": tipo_cambio,
        "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0
    }
    
    await guardar_precio_cache(llave_cache, respuesta_final)
    return respuesta_final

# ==========================================================
# 🔐 9. AUTENTICACIÓN Y LOGIN B2B
# ==========================================================
@app.post("/api/login")
async def login_b2b(datos: LoginUpdate):
    print(f"🔑 [LOGIN B2B] Intento de acceso: {datos.email}")
    try:
        res = await async_db_execute(supabase.table('usuarios_veltrix').select('*').eq('email', datos.email.lower()).limit(1))
        if not res.data or len(res.data) == 0: return {"status": "error", "detalle": "Usuario no registrado."}
            
        usuario = res.data[0]
        password_guardada = str(usuario.get('password', ''))
        
        password_valida = False
        if not password_guardada.startswith('$2b$'):
            if datos.password == password_guardada:
                nuevo_hash = pwd_context.hash(datos.password)
                await async_db_execute(supabase.table('usuarios_veltrix').update({"password": nuevo_hash}).eq('id', usuario['id']))
                password_valida = True
        else:
            password_valida = pwd_context.verify(datos.password, password_guardada)
            
        if not password_valida: return {"status": "error", "detalle": "Contraseña incorrecta."}
            
        suscripcion_valida = True
        fecha_pago_str = usuario.get('fecha_proximo_pago')
        if fecha_pago_str:
            try:
                from datetime import date
                if date.today() > date.fromisoformat(fecha_pago_str):
                    suscripcion_valida = False
                    await async_db_execute(supabase.table('usuarios_veltrix').update({"suscripcion_activa": False}).eq('id', usuario['id']))
            except ValueError: pass 

        vendedor_id = str(usuario.get('vendedor_id', 'V-001'))
        token_jwt = crear_token_jwt(vendedor_id, usuario['email'])
        print(f"✅ [LOGIN EXITO] Bienvenido {vendedor_id}")

        return {
            "status": "ok",
            "datos": {
                "vendedor_id": vendedor_id,
                "email": usuario['email'],
                "estado": usuario.get('estado', 'Activo'),
                "pais": usuario.get('pais', 'México'),
                "suscripcion_activa": suscripcion_valida,
                "token": token_jwt 
            },
            "access_token": token_jwt,
            "token_type": "bearer",
            "vendedor_id": vendedor_id,
            "nombre": usuario.get('nombre', 'Vendedor'),
            "rol": usuario.get('rol', 'vendedor')
        }
    except Exception as e:
        logger.exception("❌ [LOGIN ERROR]")
        raise HTTPException(status_code=500, detail="Error interno.")

# ==========================================================
# 🌐 10. RUTAS CRM Y MÓVIL (COMPLETAS Y RESTAURADAS)
# ==========================================================
@app.get("/api/cargar_todo")
async def cargar_todo(_sesion: str = Depends(verificar_sesion_b2b)):
    try:
        columnas_izq = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
        columnas_der = ["Por Entregar", "Vendidos", "Papelera"]
        res_cols = await async_db_execute(supabase.table('configuracion').select('nombre_columna').eq('vendedor_id', str(_sesion)))
        
        columnas_custom = [r['nombre_columna'] for r in (res_cols.data or []) if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)]]
        if not columnas_custom: columnas_custom = ["+"]
        
        res_prospectos = await async_db_execute(supabase.table('prospectos').select('*').eq('vendedor_id', str(_sesion)).order('ultima_interaccion_ia', desc=True).limit(500))
        
        ultimos = {}
        for fila in sorted(res_prospectos.data or [], key=lambda x: (x.get('telefono') is None or x.get('telefono') == "")):
            nombre = fila.get('nombre', 'Desconocido')
            tel = fila.get('telefono')
            if nombre not in ultimos or (tel and not ultimos[nombre].get('telefono')):
                ultimos[nombre] = fila
                
        return {"columnas": columnas_izq + columnas_custom + columnas_der, "prospectos": list(ultimos.values())}
    except Exception as e: raise HTTPException(status_code=500, detail="Error conectando a Nube B2B")

# 🚀 RESTAURACIÓN: Endpoint de Perfil de Cliente
@app.get("/api/perfil_cliente")
async def obtener_perfil_cliente(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(supabase.table("prospectos").select("notas, etiquetas, columna, perfil_psicologico").eq("telefono", telefono).eq("vendedor_id", str(vendedor_id)))
        if res.data: return {"status": "ok", "datos": res.data[0]}
        return {"status": "error", "datos": {}}
    except Exception as e: return {"status": "error", "detail": str(e)}

# 🚀 RESTAURACIÓN: Endpoint de Columnas Activas
@app.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(supabase.table("configuracion").select("nombre_columna").eq("vendedor_id", str(vendedor_id)))
        return {"status": "ok", "columnas": [item["nombre_columna"] for item in (res.data or [])]}
    except Exception as e: return {"status": "error", "detail": str(e)}

@app.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(supabase.table("mensajes_chat").select("*").eq("vendedor_id", str(vendedor_id)).eq("telefono", telefono).order("created_at", desc=False))
        historial_formateado = []
        for m in (res.data or []):
            historial_formateado.append({
                "contenido": str(m.get("mensaje") or m.get("contenido") or m.get("texto") or ""),
                "es_mio": str(m.get("autor", "")).upper() in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"],
                "fecha": str(m.get("created_at", ""))
            })
        return {"status": "ok", "historial": historial_formateado}
    except Exception as e: return {"status": "error", "historial": []}

@app.post("/api/mobile/send_message")
async def send_mobile_message(data: MobileMessageRequest, vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        res_conf = await async_db_execute(supabase.table('configuracion_bot').select('*').eq('vendedor_id', str(vendedor_id)).limit(1))
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración no encontrada")
        config = res_conf.data[0]
        await disparar_whatsapp_dinamico_async(data.to, data.msg, config.get('meta_token') or WHATSAPP_TOKEN, config.get('meta_phone_id') or WHATSAPP_PHONE_ID)
        await guardar_mensaje_chat(data.to, str(vendedor_id), 'ASESOR', data.msg)
        await actualizar_estado_crm(data.to, str(vendedor_id), "En Seguimiento", "azul", "")
        return {"status": "ok", "message": "Enviado"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error enviando WhatsApp")

@app.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b)):
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        total_hoy = 0.0
        ventas_res = await async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio))
        if ventas_res.data: total_hoy = sum(float(v.get("monto", 0)) for v in ventas_res.data)

        prospectos_res = await async_db_execute(supabase.table("prospectos").select("nombre, telefono, columna, ultima_interaccion_ia, ultimo_msj, notas, etiquetas").eq("vendedor_id", str(vendedor_id)).order("ultima_interaccion_ia", desc=True).limit(50))
        return {"status": "ok", "vendedor": vendedor_id, "ventas_hoy": total_hoy, "prospectos": prospectos_res.data or []}
    except Exception as e: return {"status": "error", "message": str(e), "prospectos": []}

@app.post("/api/actualizar_estado")
async def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        query = supabase.table('prospectos').update({'columna': datos.nueva_columna}).eq('vendedor_id', str(_sesion))
        query = query.eq('telefono', datos.telefono) if datos.telefono and datos.telefono != "Sin registrar" else query.eq('nombre', datos.nombre)
        resultado = await async_db_execute(query)
        if resultado.data: return {"status": "ok"}
        return {"status": "error", "mensaje": "No se encontró el registro"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error actualizando tarjeta")

@app.post("/api/historial_chat")
async def historial_chat(datos: ClienteIdentificador, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res_prospecto = await async_db_execute(supabase.table('prospectos').select('telefono').eq('nombre', datos.nombre).eq('vendedor_id', str(_sesion)))
        tel_oficial = res_prospecto.data[0]['telefono'] if res_prospecto.data and res_prospecto.data[0].get('telefono') else ""
        query = supabase.table('mensajes_chat').select('autor, mensaje').eq('vendedor_id', str(_sesion))
        query = query.eq('telefono', tel_oficial) if tel_oficial else query.eq('nombre', datos.nombre) 
        res = await async_db_execute(query.order('created_at', desc=False).limit(50))
        return {"historial": [{"texto": f.get('mensaje', ''), "es_mio": f.get('autor', 'USER') != 'USER'} for f in (res.data or [])], "telefono_oficial": tel_oficial}
    except Exception as e: raise HTTPException(status_code=500, detail="Error en historial")

@app.post("/api/mover_prospecto")
async def mover_prospecto(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        col_final = datos.nueva_columna if datos.nueva_columna else datos.columna
        if datos.telefono and datos.telefono.lower() not in ["", "sin registrar", "null", "none"]:
            await async_db_execute(supabase.table('prospectos').update({"columna": col_final}).eq('telefono', datos.telefono).eq('vendedor_id', str(_sesion)))
        else:
            await async_db_execute(supabase.table('prospectos').update({"columna": col_final}).eq('nombre', datos.nombre).eq('vendedor_id', str(_sesion)))
        return {"status": "ok", "mensaje": f"Movido a {col_final}"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error BD")

@app.post("/api/actualizar_notas")
async def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        update_data = {"notas": datos.notas, "etiquetas": datos.etiquetas, "nombre": datos.nombre}
        tel = str(datos.telefono).strip()
        res = None
        if tel and tel.lower() not in ["", "null", "sin registrar"]:
            res = await async_db_execute(supabase.table('prospectos').update(update_data).eq('telefono', tel).eq('vendedor_id', str(_sesion)))
        if not res or not res.data:
            res = await async_db_execute(supabase.table('prospectos').update(update_data).eq('nombre', datos.nombre).eq('vendedor_id', str(_sesion)))
        if res and res.data: return {"status": "ok", "mensaje": "Sincronización completa"}
        return {"status": "error", "mensaje": "No se encontró el registro"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error interno")

# ==========================================================
# 📦 11. INVENTARIO Y GESTIÓN DE COLUMNAS
# ==========================================================
@app.post("/api/crear_inventario")
async def crear_inventario(datos: NuevoArticulo, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        # 1. Insertamos el juego en la base de datos
        res = await async_db_execute(supabase.table('inventario').insert({
            'vendedor_id': str(_sesion), 
            'nombre': datos.nombre, 
            'categoria': datos.categoria, 
            'precio_compra': datos.precio_compra, 
            'precio': datos.precio, 
            'stock': datos.stock
        }))
        
        # 2. 🚀 Disparamos el Scraper en Segundo Plano (Sin congelar Godot)
        if res.data:
            juego_id_creado = str(res.data[0]['id'])
            # Usamos 'categoria' como el equivalente a la 'consola' para la búsqueda
            background_tasks.add_task(cazar_portada_y_guardar_background, juego_id_creado, datos.nombre, datos.categoria)
            
        return {"status": "ok"}
    except Exception as e: 
        raise HTTPException(status_code=500, detail="Error en DB")

@app.get("/api/cargar_inventario")
async def cargar_inventario(offset: int = 0, limit: int = 500, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(supabase.table('inventario').select("*").eq('vendedor_id', str(_sesion)).range(offset, offset + limit - 1))
        return {"status": "ok", "inventario": res.data or []}
    except Exception as e: raise HTTPException(status_code=500, detail="Error carga de inventario")

@app.post("/api/editar_item_visor")
async def editar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        precio_final = item.nuevo_precio if item.nuevo_precio is not None else item.precio
        stock_final = item.nuevo_stock if item.nuevo_stock is not None else item.stock

        if item.id: await async_db_execute(supabase.table("inventario").update({"nombre": item.nombre, "precio": precio_final, "stock": stock_final, "consola": item.consola}).eq("id", item.id).eq("vendedor_id", vid_str))
        else: await async_db_execute(supabase.table("inventario").update({"precio": precio_final, "stock": stock_final}).eq("nombre", item.nombre).eq("consola", item.consola).eq("vendedor_id", vid_str))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error editar item")

@app.get("/api/buscar_maestro")
async def buscar_maestro(q: str, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        res = await async_db_execute(
            supabase.table('inventario')
            .select('*')
            .eq('vendedor_id', str(_sesion))
            .ilike('nombre', f'%{q}%')
            .limit(50)
        )
        return {"status": "ok", "resultados": res.data or []}
    except Exception as e: 
        raise HTTPException(status_code=500, detail="Error en buscador maestro")

@app.post("/api/borrar_item")
async def borrar_item(item: InventarioItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        if item.id: await async_db_execute(supabase.table("inventario").delete().eq("id", item.id).eq("vendedor_id", str(_sesion)))
        else: await async_db_execute(supabase.table("inventario").delete().eq("nombre", item.nombre).eq("consola", item.consola).eq("vendedor_id", str(_sesion)))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error borrar item")

# 🚀 RESTAURACIÓN: Endpoint para descontar stock en ventas
@app.post("/api/actualizar_stock")
async def actualizar_stock(item: VentaItem, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        
        # 1. Consultar el precio actual del juego para saber de cuánto fue la venta
        precio_venta = 0.0
        if item.id:
            res_inv = await async_db_execute(supabase.table("inventario").select("precio").eq("id", item.id).eq("vendedor_id", vid_str))
        else:
            res_inv = await async_db_execute(supabase.table("inventario").select("precio").eq("nombre", item.nombre).eq("consola", item.consola).eq("vendedor_id", vid_str))
            
        if res_inv.data:
            precio_venta = float(res_inv.data[0].get("precio", 0.0))

        # 2. Descontar el Stock del inventario
        if item.id:
            await async_db_execute(supabase.table("inventario").update({"stock": item.nuevo_stock}).eq("id", item.id).eq("vendedor_id", vid_str))
        else:
            await async_db_execute(supabase.table("inventario").update({"stock": item.nuevo_stock}).eq("nombre", item.nombre).eq("consola", item.consola).eq("vendedor_id", vid_str))
            
        # 3. 🚀 Registrar el ingreso en la tabla 'ventas' para que el Dashboard Móvil lo sume
        await async_db_execute(supabase.table("ventas").insert({
            "vendedor_id": vid_str,
            "articulo": item.nombre,
            "consola": item.consola,
            "monto": precio_venta,
            "created_at": datetime.now(timezone.utc).isoformat()
        }))
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ Error al vender/actualizar stock: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al actualizar stock")

@app.post("/api/crear_columna")
async def crear_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        await async_db_execute(supabase.table('configuracion').insert({'vendedor_id': str(_sesion), 'nombre_columna': datos.nombre}))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error crear columna")

@app.post("/api/renombrar_columna")
async def renombrar_columna(datos: RenombrarColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        vid_str = str(_sesion)
        await async_db_execute(supabase.table('configuracion').update({'nombre_columna': datos.nuevo_nombre}).eq('vendedor_id', vid_str).eq('nombre_columna', datos.viejo_nombre))
        await async_db_execute(supabase.table('prospectos').update({'columna': datos.nuevo_nombre}).eq('vendedor_id', vid_str).eq('columna', datos.viejo_nombre))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error renombrar")

@app.post("/api/borrar_columna")
async def borrar_columna(datos: ColumnaAction, _sesion: str = Depends(verificar_sesion_b2b)):
    try:
        await async_db_execute(supabase.table('configuracion').delete().eq('vendedor_id', str(_sesion)).eq('nombre_columna', datos.nombre))
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail="Error borrar columna")

# ==========================================================
# ⚙️ 12. BACKGROUND WORKER Y WEBHOOKS DE META (CON AUDITOR)
# ==========================================================
async def gestionar_mensaje_entrante_bg(valor: dict, msg: dict, phone_id_receptor: str):
    print("\n📥 ==========================================================")
    print("📥 [WORKER BG] INICIANDO ORQUESTACIÓN DE MENSAJE ENTRANTE")
    print("==============================================================")
    try:
        wamid = str(msg.get("id", "")).strip()
        if wamid and wamid in procesados_recientemente:
            print(f"♻️ [WORKER BG] Token de mensaje duplicado (wamid: {wamid}). Ignorando transmisión.")
            return
        if wamid:
            procesados_recientemente.append(wamid)

        print(f"📡 [WORKER BG] Descargando configuración de Tenant (Phone ID: {phone_id_receptor})")
        res_config = await async_db_execute(supabase.table('configuracion_bot').select('*').eq('meta_phone_id', phone_id_receptor).limit(1))
        config_vendedor = res_config.data[0] if res_config.data else {"vendedor_id": "V-001", "meta_token": WHATSAPP_TOKEN, "meta_phone_id": WHATSAPP_PHONE_ID, "bot_activo": True, "nombre_negocio": "Fantasy Games"}
        
        vendedor_actual = str(config_vendedor.get("vendedor_id", "V-001"))
        token_actual = str(config_vendedor.get("meta_token", "")) or WHATSAPP_TOKEN
        nombre_negocio = str(config_vendedor.get("nombre_negocio", "Fantasy Games"))
        
        if not token_actual or not config_vendedor.get("bot_activo", True):
            print(f"🚫 [WORKER BG] Flujo denegado: El Bot del Tenant {vendedor_actual} está inactivo o carece de token de acceso.")
            return

        telefono_cliente = str(msg.get("from", "")).strip()
        if telefono_cliente.startswith("521"): 
            telefono_cliente = "52" + telefono_cliente[3:]
        if not telefono_cliente: 
            print("⚠️ [WORKER BG] Alerta: Identificador telefónico vacío o corrupto. Abortando ejecutor.")
            return

        tipo_mensaje = str(msg.get("type", "text")).lower()
        texto_entrante = ""
        media_dict_audio = None  # 🎙️ Inyección AAA: Buffer de almacenamiento para el Audio Binario Nativo

        print(f"📦 [WORKER BG] Formato de paquete detectado: '{tipo_mensaje}' | Remitente: {telefono_cliente}")
        
        if tipo_mensaje == "text": 
            texto_entrante = msg.get("text", {}).get("body", "").strip()
        elif tipo_mensaje == "image": 
            texto_entrante = "📷 [IMAGEN RECIBIDA: Analizando comprobante de pago...]"
        elif tipo_mensaje == "interactive": 
            texto_entrante = msg.get("interactive", {}).get("button_reply", {}).get("title", "").strip()
        elif tipo_mensaje == "audio": 
            texto_entrante = "🎙️ [NOTA DE VOZ RECIBIDA - ANALIZANDO AUDIO...]"
            audio_id = msg.get("audio", {}).get("id", "").strip()
            print(f"🎙️ [WORKER BG] Capturado ID de Nota de voz: {audio_id}. Inicializando pasarela de descarga...")
            if audio_id:
                media_dict_audio = await descargar_media_whatsapp_async(audio_id, token_actual)
                if media_dict_audio:
                    print("🎙️ [WORKER BG] Archivo binario de audio descargado y acoplado con éxito al diccionario multimedia.")
                else:
                    print("⚠️ [WORKER BG] Warning: No se obtuvo respuesta binaria de los servidores de Meta para este audio.")
        else: 
            print(f"ℹ️ [WORKER BG] Formato '{tipo_mensaje}' no mapeado en el enrutador actual. Descartando.")
            return

        print(f"🗂️ [WORKER BG] Validando estado de cuenta del prospecto en Supabase...")
        res_p = await async_db_execute(supabase.table('prospectos').select('columna, notas').eq('telefono', telefono_cliente).eq('vendedor_id', vendedor_actual))
        columna_actual = res_p.data[0].get("columna", "Bandeja Nueva") if res_p.data else "Bandeja Nueva"

        nombre_cliente = valor.get("contacts", [{}])[0].get("profile", {}).get("name", "Cliente")

        if not res_p.data:
            print(f"✨ [WORKER BG] Cliente nuevo localizado. Inicializando inserción de '{nombre_cliente}' en CRM...")
            await async_db_execute(supabase.table('prospectos').insert({
                "nombre": nombre_cliente, 
                "telefono": telefono_cliente, 
                "columna": columna_actual, 
                "vendedor_id": vendedor_actual,
                "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
            }))

        # Persistimos la actividad del usuario en el log histórico global
        await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "USER", texto_entrante)

        # 🚀 Bifurcación del Ruteador según la naturaleza del evento multimedia
        if tipo_mensaje in ["text", "interactive", "audio"] and columna_actual != "En Conversacion":
            print(f"🤖 [WORKER BG] Despachando carga cognitiva hacia procesar_respuesta_bot... (Audio Binario Cargado: {media_dict_audio is not None})")
            # Enviamos el buffer de audio de forma nativa a la canalización del cerebro IA
            await procesar_respuesta_bot(nombre_cliente, telefono_cliente, texto_entrante, columna_actual, config_vendedor, media_dict_audio)
            
        elif tipo_mensaje == "image":
            print("🛡️ [DOBERMAN AUDITOR] Desplegando cortafuegos analítico de finanzas visuales...")
            image_id = msg.get("image", {}).get("id", "").strip()
            if not image_id:
                print("⚠️ [DOBERMAN AUDITOR] Cancelando auditoría: Estructura de imagen vacía.")
                return

            historial_para_auditor = await obtener_historial_chat(telefono_cliente, vendedor_actual)
            media_dict_img = await descargar_media_whatsapp_async(image_id, token_actual)

            if not media_dict_img:
                print("❌ [DOBERMAN AUDITOR] Falla crítica: Error de enlace en la descarga del comprobante.")
                return

            # Ejecutamos la auditoría de visión computacional con Gemini
            auditoria = await auditar_comprobante_ia(media_dict_img["data"], media_dict_img["mime_type"], nombre_negocio, historial_para_auditor)
            es_pago = auditoria.get("es_pago", False)
            monto = float(auditoria.get("monto_detectado", 0.0)) 

            if es_pago:
                print(f"💰 [DOBERMAN AUDITOR] ¡COMPROBANTE VÁLIDO! Capital detectado: ${monto} MXN. Actualizando CRM...")
                await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Por Entregar", "verde_exito", "")
                msg_exito = f"✅ ¡Pago validado por ${monto:.2f} MXN!\nHemos recibido tu comprobante."
                await disparar_whatsapp_dinamico_async(telefono_cliente, msg_exito, token_actual, phone_id_receptor)
                await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_exito)
                print(f"💰 PAGO EXITOSO FINANCIADO | {telefono_cliente} | ${monto}")
            else:
                print(f"🚨 [DOBERMAN AUDITOR] ALERTA: Intento de fraude o imagen corrupta. Razón: {auditoria.get('analisis')}")
                msg_fallo = f"🤖 Mi sistema no pudo validar la imagen.\nDetalle: {auditoria.get('analisis')}\nPor favor envía una foto clara."
                await actualizar_estado_crm(telefono_cliente, vendedor_actual, "Requiere Asistencia", "verde_alerta", "")
                await disparar_whatsapp_dinamico_async(telefono_cliente, msg_fallo, token_actual, phone_id_receptor)
                await guardar_mensaje_chat(telefono_cliente, vendedor_actual, "BOT", msg_fallo)

        print("🏁 ==========================================================")
        print("🏁 [WORKER BG] OPERACIÓN ASÍNCRONA COMPLETADA SIN ERRORES")
        print("==============================================================\n")

    except Exception as e: 
        logger.exception(f"❌ [WORKER BG CRITICAL ERROR] Detonación en bloque supervisor en background: {str(e)}")

@app.get("/webhook")
async def verificar_webhook(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == WEBHOOK_SECRET:
        print("✅ [WEBHOOK] Servidor validado con éxito por Meta.")
        return int(params.get("hub.challenge"))
    raise HTTPException(status_code=403, detail="Token inválido")

@app.post("/webhook")
async def recibir_mensajes(request: Request, background_tasks: BackgroundTasks):
    await validar_firma_meta(request)
    try:
        body = await request.json()
        if not body.get("entry", [{}])[0].get("changes", [{}])[0].get("value", {}).get("messages"): return {"status": "ignored"}
        print("\n--- 📥 [NUEVO MENSAJE DE META] ---")
        value = body["entry"][0]["changes"][0]["value"]
        background_tasks.add_task(gestionar_mensaje_entrante_bg, value, value.get("messages", [{}])[0], value.get("metadata", {}).get("phone_number_id", WHATSAPP_PHONE_ID))
        return {"status": "ok"}
    except Exception as e: return {"status": "error", "reason": str(e)}

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), reload=False)
            
