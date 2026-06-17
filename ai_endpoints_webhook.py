# ==========================================================
# 🚀 MÓDULO: ai_endpoints_webhook.py
# ==========================================================

import os
import asyncio
import hashlib
import orjson
import json
import requests
import urllib.parse
import re
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from cachetools import TTLCache

from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
import config_and_schemas as config
import ai_security_utils
from ai_gemini_core import consultar_gemini_json, analizar_intencion_venta_ia
from ai_whatsapp_media import descargar_media_whatsapp_async, enviar_mensaje_whatsapp_async
from db_rag_scraper import obtener_contexto_inventario_rag

logger = config.logger

# Importaciones dinámicas/diferidas para evitar Loops Cíclicos (Punto 4)
# Solo las cargamos cuando la lógica de la función realmente las dispara.

# ==========================================================
# 🛡️ CONFIGURACIÓN Y LIMITADORES LOCALES
# ==========================================================

# Límites de seguridad extraídos del archivo maestro config
MAX_BACKGROUND_TASKS_RAM = getattr(config, "MAX_BACKGROUND_TASKS_RAM", 150)
WHATSAPP_TOKEN_FALLBACK = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID_FALLBACK = os.getenv("WHATSAPP_PHONE_ID", "").strip()
GENAI_KEY_FALLBACK = os.getenv("GENAI_KEY", "").strip()

# 🛡️ Caches Locales y Semáforos
cache_busquedas_maestro = TTLCache(maxsize=2000, ttl=30)
ventas_procesadas_idempotencia = TTLCache(maxsize=10000, ttl=86400)
PAYLOAD_FLOOD_CACHE = TTLCache(maxsize=5000, ttl=60) # Cortafuegos Anti-Retries Meta
ULTIMO_WARNING_BACKPRESSURE = 0.0

webhook_lock = asyncio.Lock() # 🛡️ Lock para idempotencia real de webhooks

# 🔌 Inicializamos el Router Secundario
router = APIRouter()

# ==========================================================
# 🛠️ HELPERS AUXILIARES (Simulados para mantener integridad)
# ==========================================================
async def lanzar_gc_si_toca():
    # El GC ahora lo maneja fastapi_endpoints.py (task_gc_locks), pero mantenemos la firma
    # por compatibilidad hacia atrás si otro módulo lo llama.
    pass

def generar_cache_key(nombre, consola): 
    return f"{nombre}_{consola}".lower().replace(" ", "_")

async def obtener_precio_cache(key): 
    # Placeholder: En SaaS real, esto iría a Redis
    return None

async def guardar_precio_cache(key, val): 
    pass

async def obtener_dolar_hoy_async(): 
    # Fallback rápido para no ralentizar el scraper
    return 20.0

# ==========================================================
# 🎮 ENDPOINTS B2B (Manejo de Godot / Dashboard Interno)
# ==========================================================

@router.post("/api/solicitar_portada")
async def solicitar_portada(payload: dict, background_tasks: BackgroundTasks):
    # NOTA: En producción real añadir: _sesion: str = Depends(verificar_sesion_b2b)
    juego_id = payload.get("juego_id")
    nombre = payload.get("nombre")
    consola = payload.get("consola")
    
    if not juego_id or not nombre:
        raise HTTPException(status_code=400, detail="Datos incompletos")
        
    # Importación perezosa para evitar Dependencias Circulares
    import db_api_endpoints 
    
    background_tasks.add_task(db_api_endpoints.cazar_portada_y_guardar_background, str(juego_id), nombre, consola)
    
    return {"status": "searching", "message": "Tarea de cacería iniciada"}


@router.get("/api/consultar_precio")
async def api_consultar_precio(nombre: str, consola: str = "", vendedor_id: str = "anonimo", dias_inventario: int = 0, rareza: str = "comun"):
    await lanzar_gc_si_toca() 
    
    # 🛡️ FIX AAA: Protección contra requests con nombres maliciosamente largos
    nombre = config.limpiar_texto(nombre)[:120]
    
    # 🚀 BYPASS SAAS MULTI-GIRO: Si no hay consola o no aplica, evitamos gastar recursos en PriceCharting
    if not consola or consola.lower() in ["n/a", "general", "ninguna", "otro", ""]:
        logger.info(f"🔄 [BYPASS SAAS] El artículo '{nombre}' no es un videojuego. Omitiendo scraper.")
        return {
            "status": "bypass_saas",
            "api_version": "v3",
            "nombre_corregido": nombre,
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "tipo_cambio": await obtener_dolar_hoy_async(),
            "rareza": rareza,
            "url_pc": "",
            "confidence_score": 100.0,
            "atributos_extra": {}
        }
    
    logger.info(f"🏷️ [RADAR ENTERPRISE] Buscando: '{nombre}' ({consola}) | Operador: {vendedor_id}")
    
    llave_cache = generar_cache_key(nombre, consola)
    valores_cacheados = await obtener_precio_cache(llave_cache)
    if valores_cacheados:
        valores_cacheados["status"] = "ok_cached"
        return valores_cacheados

    tipo_cambio = await obtener_dolar_hoy_async()
    slugs_pc = {"PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3", "PS2": "playstation-2", "PS1": "playstation", "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox", "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", "Nintendo DS": "nintendo-ds", "Nintendo 64": "nintendo-64", "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance", "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", "SNES": "super-nintendo", "NES": "nes", "Genesis": "sega-genesis"}
    
    consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
    nombre_normalizado = ai_security_utils.normalizar_nombre_busqueda(nombre)
    
    query = urllib.parse.quote_plus(nombre_normalizado + ' ' + consola_web)
    url_search = f"https://www.pricecharting.com/search-products?q={query}&type=prices"
    
    import ai_auditor_scraper
    html_search = await ai_auditor_scraper.obtener_html_escalonado_async(url_search, es_busqueda=True)
    
    if not html_search: 
        logger.error(f"❌ [RADAR PRECIOS] Falló la búsqueda HTML. Devolviendo contrato de error estruturado.")
        return {
            "status": "error",
            "api_version": "v3",
            "nombre_corregido": nombre,
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "tipo_cambio": tipo_cambio,
            "rareza": rareza,
            "url_pc": url_search,
            "confidence_score": 0.0,
            "atributos_extra": {}
        }
        
    soup = BeautifulSoup(html_search, 'html.parser')
    
    # 🛡️ FIX AAA: Evitar Attribute Error si el HTML de PriceCharting cambia
    tabla_juegos = soup.find(id="games_table")
    nodos_a_buscar = tabla_juegos.find_all('a', href=True) if tabla_juegos else soup.find_all('a', href=True)
    
    candidatos = []
    slug_esperado = slugs_pc.get(consola, consola_web.lower().replace(' ', '-'))
    
    for a in nodos_a_buscar:
        href = a['href'].lower()
        if '/game/' in href and not any(b in href for b in ['strategy-guide', 'lot', 'bundle', 'box-only', 'manual-only']):
            score = 0.0
            if f"/{slug_esperado}/" in href: score += 40.0 
            
            score += fuzz.token_sort_ratio(nombre_normalizado, ai_security_utils.normalizar_nombre_busqueda(a.text)) * 0.6
            if re.search(r'(-japan-|-jp-|-pal-|-eu-|-korea-)', href): score -= 50.0
            
            if score > 35.0:
                url_limpia = a['href'].strip()
                if not url_limpia.startswith("http"): url_limpia = "https://www.pricecharting.com" + url_limpia
                candidatos.append({"url": url_limpia, "score": score})

    # 🛡️ FIX AAA: Validación anti-HTML corrupto / Exceso de links
    if len(candidatos) > 500:
        logger.error("🚨 [RADAR] HTML corrupto o envenenado. Exceso de candidatos.")
        raise Exception("HTML corrupto")

    nombre_oficial_pc, p_loose, p_cib, p_new = nombre, 0.0, 0.0, 0.0
    link_juego = None

    if candidatos:
        mejor_candidato = max(candidatos, key=lambda x: x["score"])
        link_juego = mejor_candidato["url"]
        logger.info(f"🎯 [MATCHING AAA] Score {round(mejor_candidato['score'], 2)}/100 -> {link_juego}")
        
        html_juego = await ai_auditor_scraper.obtener_html_escalonado_async(link_juego, es_busqueda=False)
        if html_juego: 
            soup_juego = BeautifulSoup(html_juego, 'html.parser')
            h1_tag = soup_juego.find('h1', id='product_name')
            if h1_tag: nombre_oficial_pc = h1_tag.text.strip().replace('\n', ' ')

            def extraer_numero(id_css, clase_css=None):
                try:
                    nodo = soup_juego.find(id=id_css)
                    if not nodo and clase_css:
                        nodo = soup_juego.find(class_=clase_css)
                        
                    if not nodo: return 0.0
                    
                    texto_crudo = nodo.get_text(separator=' ', strip=True).replace(',', '')
                    coincidencias = re.findall(r'\d+\.\d+|\d+', texto_crudo)
                    if coincidencias:
                        return float(coincidencias[0])
                except Exception as e:
                    logger.error(f"❌ [EXTRACTOR] Error parseando {id_css}: {e}")
                return 0.0

            p_loose = extraer_numero("used_price", "price_used")
            p_cib = extraer_numero("cib_price", "price_cib")
            p_new = extraer_numero("new_price", "price_new")

            if p_cib == 0.0:
                if p_loose > 0:
                    p_cib = round(p_loose * 1.30, 2)
                    logger.info(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde Loose: ${p_cib} USD")
                elif p_new > 0:
                    p_cib = round(p_new * 0.70, 2)
                    logger.info(f"🧠 [FALLBACK PRICING] Precio CIB deducido desde New: ${p_cib} USD")

    url_final_godot = link_juego if link_juego else url_search

    if p_loose == 0 and p_cib == 0:
        logger.error(f"❌ [RADAR PRECIOS] Contingencia 0$ Absoluta para: '{nombre_oficial_pc}'.")
        respuesta_fallida = {
            "status": "warning_cero", 
            "api_version": "v3",
            "nombre_corregido": nombre_oficial_pc, 
            "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0}, 
            "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
            "rareza": rareza,
            "url_pc": url_final_godot,
            "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0,
            "atributos_extra": {}
        }
        await guardar_precio_cache(llave_cache, respuesta_fallida)
        return respuesta_fallida

    mxn_loose_real = round(p_loose * tipo_cambio, 2)
    mxn_cib_real = round(p_cib * tipo_cambio, 2)
    mxn_new_real = round(p_new * tipo_cambio, 2)
    
    # Asumimos que la lógica de tu negocio usa un multiplier fijo si falta db_api_endpoints
    def calcular_precio_venta_inteligente_aaa(precio, param1, dias, rareza):
        return round(precio * 1.2, 2)
        
    respuesta_final = {
        "status": "ok",
        "api_version": "v3",
        "nombre_corregido": nombre_oficial_pc,
        "mxn": {
            "loose": mxn_loose_real,
            "cib": mxn_cib_real,
            "new": mxn_new_real
        },
        "mxn_mercado": {
            "loose": mxn_loose_real,
            "cib": mxn_cib_real,
            "new": mxn_new_real
        },
        "mxn_venta": {
            "loose": calcular_precio_venta_inteligente_aaa(mxn_loose_real, 0, dias_inventario, rareza), 
            "cib": calcular_precio_venta_inteligente_aaa(mxn_cib_real, 0, dias_inventario, rareza), 
            "new": calcular_precio_venta_inteligente_aaa(mxn_new_real, 0, dias_inventario, rareza)
        },
        "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
        "tipo_cambio": tipo_cambio,
        "rareza": rareza,
        "url_pc": url_final_godot,
        "confidence_score": round(mejor_candidato["score"], 2) if candidatos else 0.0,
        "atributos_extra": {}
    }
    
    await guardar_precio_cache(llave_cache, respuesta_final)
    logger.info(f"✅ [RADAR EXITO] Mercado CIB: ${mxn_cib_real} MXN | URL: {url_final_godot}")
    return respuesta_final


# ==========================================================
# ⚡ WEBHOOK CENTRAL DE META (Lógica Desacoplada)
# ==========================================================
# 🚀 FIX AUDITORÍA: Refactorizamos el método de encolado.
async def procesar_mensaje_meta_pipeline(value: dict, message: dict, phone_id_receptor: str):
    """
    Tubería de datos asíncrona. Esto extrae el perfil del CRM (Auditoría Punto 9)
    y enruta los datos a Gemini y al ChatDB sin bloquear el endpoint HTTP.
    """
    try:
        import db_crm_logic
        import db_chat
        
        telefono_cliente = str(message.get("from", ""))
        tipo_mensaje = str(message.get("type", "text"))
        
        # 1. Recuperar contexto semántico del CRM
        # (Esto asume que db_crm_logic tiene una forma de leer el prospecto)
        from db_core_wrapper import supabase, async_db_execute
        res_crm = await async_db_execute(
            supabase.table('prospectos').select('*').eq('telefono', telefono_cliente).limit(1)
        )
        
        perfil_previo = {}
        if res_crm and res_crm.data:
            lead = res_crm.data[0]
            perfil_previo = lead.get('perfil_psicologico', {})
            # Inyectamos contexto fuerte (Auditoría Punto 10)
            perfil_previo['remarketing_count'] = lead.get('remarketing_count', 0)
            perfil_previo['interes_historico'] = lead.get('ultimo_producto_interes', '')

        # 2. Descargar multimedia si aplica (Visión AI Activa)
        media_dict = None
        texto_limpio = ""
        
        if tipo_mensaje == "text":
            texto_limpio = message.get("text", {}).get("body", "")
        elif tipo_mensaje in ["image", "audio"]:
            id_media = message.get(tipo_mensaje, {}).get("id")
            mime = message.get(tipo_mensaje, {}).get("mime_type")
            # Descargamos los bytes pasando por el validador estricto de URL (Punto 8)
            bytes_media = await descargar_media_whatsapp_async(id_media, WHATSAPP_TOKEN_FALLBACK)
            if bytes_media:
                media_dict = {"data": bytes_media, "mime_type": mime}
                texto_limpio = "[El usuario ha enviado un archivo multimedia]"
                
        if not texto_limpio and not media_dict:
            return # Mensaje sin soporte
            
        # 3. Extraer historial conversacional
        historial = await db_chat.obtener_historial_chat_ia(telefono_cliente, "V-001") # Placeholder Tenant
        
        # 4. Contexto RAG Inventario
        rag_context = await obtener_contexto_inventario_rag("V-001", texto_limpio)
        
        # 5. Invocar al Cerebro IA
        config_tenant = {
            "vendedor_id": "V-001",
            "meta_token": WHATSAPP_TOKEN_FALLBACK,
            "meta_phone_id": phone_id_receptor
        }
        
        respuesta_ia = await analizar_intencion_venta_ia(
            texto_limpio, rag_context, historial, config_tenant, perfil_cliente_previo=perfil_previo, media_dict=media_dict
        )
        
        # 6. Disparar Respuesta a Meta
        if respuesta_ia and respuesta_ia.get("respuesta"):
            exito_meta = await enviar_mensaje_whatsapp_async(
                telefono_cliente, respuesta_ia["respuesta"], WHATSAPP_TOKEN_FALLBACK, phone_id_receptor
            )
            
            # 7. Sincronizar Sistemas (CRM y Logs)
            if exito_meta:
                await db_chat.guardar_mensaje_chat(telefono_cliente, "V-001", "CLIENTE", texto_limpio)
                await db_chat.guardar_mensaje_chat(telefono_cliente, "V-001", "BOT", respuesta_ia["respuesta"])
                
                # Reflejamos las conclusiones de Gemini de vuelta al CRM
                await db_crm_logic.guardar_resultado_ia_en_crm(telefono_cliente, "V-001", respuesta_ia)
                
    except Exception as e:
        logger.exception(f"❌ [PIPELINE IA ERROR] Fallo procesando mensaje: {str(e)}")


# 🚀 FASTAPI ENTRYPOINT
# No se define el router HTTP aquí; este módulo ahora solo provee las funciones
# lógicas que 'fastapi_endpoints.py' invoca de manera limpia y concurrente.

# ==============================================================================
#  13 🤖 MÓDULO IA VELTRIX: GENERADOR DE COPY COMERCIAL AAA
# ==============================================================================
# Pydantic Schema Inline para evitar dependencias
from pydantic import BaseModel
class PeticionCopy(BaseModel):
    juego: str

@router.post("/api/generar_copy_imagen")
async def api_generar_copy_imagen(datos: PeticionCopy):
    logger.info(f"✨ [IA] Generando copy comercial AAA para: {datos.juego}")
    
    # 1. Definimos la URL de bypass con el modelo 2.5 flash
    api_key_gemini = getattr(config, "GENAI_KEY", GENAI_KEY_FALLBACK)
    url_api = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key_gemini}"
    
    prompt_maestro = f"""
    Eres un experto copywriter de videojuegos físicos. Tu objetivo es vender en Marketplace.
    Genera un texto vendedor ultra-persuasivo para este juego: {datos.juego}
    
    Reglas estrictas de Veltrix Engine:
    1. Devuelve ÚNICAMENTE un objeto JSON válido, sin formato Markdown (NO uses ```json).
    2. El JSON debe tener exactamente dos llaves: "titulo_generado" y "estado_generado".
    3. "titulo_generado": Debe ser un título llamativo, en MAYÚSCULAS, MÁXIMO 4 palabras. Ej: "¡GOD OF WAR REMATADO!"
    4. "estado_generado": Debe incluir emojis y texto comercial corto. Sugiere entregas en puntos clave como Altaria, San Pancho o punto a convenir. Ej: "🔥 ENTREGA INMEDIATA | ESTADO 10/10 | ENTREGAS EN ALTARIA"
    """
    
    try:
        # 2. Construcción del payload
        payload = {
            "contents": [{"parts": [{"text": prompt_maestro}]}],
            "generationConfig": {
                "temperature": 0.2, 
                "maxOutputTokens": 1024,
                "responseMimeType": "application/json" # 🚀 FIX: Forzar formato JSON nativo de Gemini
            }
        }
        
        # 3. Llamada HTTP asíncrona directa (Bypass al SDK obsoleto)
        response = await asyncio.to_thread(
            requests.post,
            url_api,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=20.0
        )
        
        if response.status_code != 200:
            raise Exception(f"API Error {response.status_code}: {response.text}")
            
        # 4. Extracción y Limpieza Blindada
        data = response.json()
        texto_ia = data['candidates'][0]['content']['parts'][0]['text'].strip()
        
        # Blindaje contra Markdown
        texto_ia = texto_ia.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
            
        json_ia = json.loads(texto_ia)
        
        logger.info(f"✅ [IA] Copy generado exitosamente para {datos.juego}")
        return json_ia
        
    except Exception as e:
        logger.exception(f"❌ [IA ERROR] Fallo al generar copy: {str(e)}")
        
        # FALLBACK DE SEGURIDAD
        return {
            "titulo_generado": f"¡{datos.juego.upper()}!",
            "estado_generado": "🔥 DISPONIBLE AHORA | EXCELENTE ESTADO | PUNTO A CONVENIR"
        }