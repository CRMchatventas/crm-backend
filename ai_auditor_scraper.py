# ==========================================================
# 🚀 MÓDULO: ai_auditor_scraper.py
# ==========================================================
# 🔧 FIX (sesión actual): regla #2 de fecha ahora considera si es el primer
# contacto del cliente — un comprobante de "ayer" es imposible si el cliente
# apenas está hablando contigo por primera vez hoy. Hallazgo real, confirmado
# contra el monolito original: la regla "HOY o AYER" sola no distinguía esto.
# ==========================================================

import io
import re
import time
import asyncio
import urllib.parse
import httpx
from datetime import datetime
from PIL import Image
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
from config_and_schemas import *
from ai_security_utils import *
from ai_gemini_core import *

# 🛡️ FIX AAA: Proxy en vivo para el HTTP Client
# Esto evita el error donde http_client se queda como "None" al inicializarse antes del lifespan
class HTTPClientProxy:
    def __getattr__(self, name):
        # Referenciamos directamente a la variable global exportada por ai_security_utils
        import ai_security_utils 
        return getattr(ai_security_utils.http_client, name)
    
    def __bool__(self):
        import ai_security_utils
        return bool(ai_security_utils.http_client)

http_client = HTTPClientProxy()
# ==========================================================

async def auditar_comprobante_ia(
    b64_img_data: bytes,
    mime_type: str,
    nombre_negocio: str,
    historial_chat: str
):
    """
    Motor antifraude financiero IA:
    - OCR contextual
    - Verificación temporal
    - Verificación de montos
    - Anti screenshots falsas
    - Análisis financiero semántico
    """

    # ==========================================================
    # 🧠 HELPER FLOAT SEGURO
    # ==========================================================
    def safe_float_local(valor):
        try:
            if valor is None:
                return 0.0

            limpio = (
                str(valor)
                .replace("$", "")
                .replace(",", "")
                .replace("MXN", "")
                .replace("mxn", "")
                .strip()
            )

            return round(float(limpio), 2)

        except Exception:
            return 0.0

    try:
        logger.info("🛡️ [DOBERMAN] Iniciando auditoría financiera IA.")

        # ==========================================================
        # 🛡️ VALIDACIÓN BINARIA PREVIA
        # ==========================================================
        if not b64_img_data:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "Imagen vacía o inválida."
            }

        if len(b64_img_data) > 12_000_000:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "El archivo excede el tamaño permitido."
            }

        # ==========================================================
        # 🛡️ VALIDACIÓN IMAGEN REAL
        # ==========================================================
        try:
            img = Image.open(io.BytesIO(b64_img_data))
            img.verify()
        except Exception:
            return {
                "es_pago": False,
                "monto_detectado": 0.0,
                "analisis": "La imagen parece corrupta o alterada."
            }

        # ==========================================================
        # 🛡️ HISTORIAL CONTROLADO
        # ==========================================================
        historial_chat = limpiar_texto(historial_chat)

        if len(historial_chat) > 2500:
            historial_chat = historial_chat[-2500:]

        fecha_hoy = datetime.now().strftime("%d de %B de %Y")

        # ==========================================================
        # 🧠 PROMPT ANTIFRAUDE
        # ==========================================================
        prompt = f"""
Eres el auditor financiero principal de '{nombre_negocio}'.

Tu trabajo es detectar:
- comprobantes reales
- screenshots falsas
- montos alterados
- comprobantes viejos
- transferencias sospechosas
- imágenes editadas

HISTORIAL CHAT:
{historial_chat}

FECHA ACTUAL:
{fecha_hoy}

REGLAS OBLIGATORIAS:
1. SOLO aceptar comprobantes bancarios o SPEI reales.
2. La fecha debe ser HOY o AYER. EXCEPCIÓN OBLIGATORIA: si el HISTORIAL CHAT está
   vacío o muestra que este es el primer mensaje de este cliente (sin ninguna
   conversación previa), la fecha del comprobante debe ser de HOY exactamente —
   sería imposible que ya hubiera hecho un pago un día antes de escribirte por
   primera vez. Rechaza cualquier comprobante de "ayer" en ese caso.
3. Debe existir monto visible.
4. Debe existir evidencia bancaria coherente.
5. Si detectas edición, baja calidad o datos sospechosos → rechazar.
6. Si tienes dudas → rechazar.

RESPONDE ÚNICAMENTE JSON:

{{
  "es_pago": true,
  "monto_detectado": 999.99,
  "analisis": "Transferencia válida detectada."
}}
"""

        # ==========================================================
        # 🤖 CONSULTA IA
        # ==========================================================
        data = await consultar_gemini_json(
            prompt=prompt,
            media_dict={
                "mime_type": mime_type,
                "data": b64_img_data
            },
            temperature=0.0
        )

        resultado = {
            "es_pago": bool(data.get("es_pago", False)),
            "monto_detectado": safe_float_local(
                data.get("monto_detectado", 0)
            ),
            "analisis": limpiar_texto(
                str(data.get("analisis", "Análisis no disponible."))
            )[:500]
        }

        logger.info(
            f"🧾 [DOBERMAN] Resultado auditoría | "
            f"Pago: {resultado['es_pago']} | "
            f"Monto: ${resultado['monto_detectado']}"
        )

        return resultado

    except Exception as e:
        logger.exception(f"❌ [DOBERMAN ERROR] {str(e)}")

        return {
            "es_pago": False,
            "monto_detectado": 0.0,
            "analisis": "Error interno del sistema antifraude."
        }


async def obtener_html_escalonado_async_portadas(url_objetivo: str) -> str:
    if not http_client: return ""
    
    # 🛡️ FIX AAA: Escudo SSRF
    dominio = urllib.parse.urlparse(url_objetivo).netloc
    if "pricecharting.com" not in dominio:
        logger.warning(f"🚨 [SSRF PREVENT] Intento de acceso a dominio no autorizado: {dominio}")
        return ""
        
    estrategias = [
        ("Ligera", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}"),
        ("Render", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={urllib.parse.quote(url_objetivo)}&render=true")
    ]
    for _, url_scraper in estrategias:
        try:
            res = await http_client.get(url_scraper, timeout=20.0)
            if res.status_code == 200 and "price" in res.text.lower(): return res.text
        except: pass
    try:
        res = await http_client.get(url_objetivo, headers={"User-Agent": "Mozilla/5.0"}, timeout=15.0)
        if res.status_code == 200: return res.text
    except: pass
    return ""


async def obtener_html_escalonado_async(url_objetivo: str, es_busqueda: bool = True) -> str:
    if not http_client: return ""
    
    # 🛡️ FIX AAA: Validación de API Key y Dominio (Anti-SSRF)
    if not SCRAPER_API_KEY: 
        logger.error("🚨 [SCRAPER] Falta SCRAPER_API_KEY en el entorno.")
        return ""
    if "pricecharting.com" not in urllib.parse.urlparse(url_objetivo).netloc:
        print("🚨 [SSRF PREVENT] Dominio no autorizado en scraper.")
        return ""
    
    ahora = time.time()
    if ahora < CB_PRICECHARTING.get("bloqueado_hasta", 0.0):
        print("🛑 [CIRCUIT BREAKER] Dominio PriceCharting en enfriamiento.")
        return ""
    
    def es_html_valido(html_text: str) -> bool:
        texto = html_text.lower()
        if any(b in texto for b in ["cloudflare", "just a moment", "security check", "verify you are human"]): return False
        if len(html_text) < 2000: return False
        
        if es_busqueda and "games_table" not in texto and "search-results" not in texto: 
            return False
        if not es_busqueda and "product_name" not in texto and "price" not in texto: 
            return False
        return True

    url_codificada = urllib.parse.quote(url_objetivo, safe='')
    estrategias = [
        ("Directo", url_objetivo),
        ("Proxy Desktop", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&device_type=desktop"),
        ("Render JS Desktop", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&render=true&device_type=desktop"),
        ("Premium", f"http://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url_codificada}&premium=true&device_type=desktop")
    ]
    
    for intento, (nombre_fase, url_scraper) in enumerate(estrategias):
        try:
            if intento > 0: await asyncio.sleep(1.5 ** intento) 
            timeout_seguro = httpx.Timeout(connect=10.0, read=35.0, write=15.0, pool=10.0)
            
            target_url = url_objetivo if nombre_fase == "Directo" else url_scraper
            res = await http_client.get(target_url, timeout=timeout_seguro)
            
            if res.status_code == 200 and es_html_valido(res.text): 
                CB_PRICECHARTING["fallas"] = 0
                metricas_radar["scraper_ok"] += 1
                return res.text
        except Exception as e:
            print(f"❌ [SCRAPER] Fallo en {nombre_fase}: {str(e)[:50]}")
            
    CB_PRICECHARTING["fallas"] = CB_PRICECHARTING.get("fallas", 0) + 1
    metricas_radar["scraper_fail"] += 1
    if CB_PRICECHARTING["fallas"] >= 10:
        CB_PRICECHARTING["bloqueado_hasta"] = ahora + 600
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
    
    # 🛡️ FIX AAA: Evitar que los juegos basura o muy baratos queden con precios absurdos
    minimo_operativo = max(costo_compra * 1.15, costo_compra + 50)
    return float(round(max(precio_calculado, minimo_operativo) / 10) * 10)

# ==========================================================
# 💰 BÚSQUEDA REAL DE PRECIOS EN PRICECHARTING (recuperado del monolito)
# ==========================================================
# Esta es la pieza que faltaba para que /api/consultar_precio funcionara de
# verdad: buscar el juego en PriceCharting, filtrar resultados que no son el
# juego en sí (guías, lotes, cajas solas, manuales sueltos), desambiguar por
# similitud de nombre + consola, y parsear los tres precios reales de la
# página del producto. Antes de esto, el endpoint no existía en el backend
# modular — solo estaban los primitivos (el descargador de HTML y la
# calculadora de precio sugerido), nunca la lógica que los conecta.
#
# 🔧 Cambios sobre el original al portarlo:
# - try/except envolviendo toda la función, igual que el resto del archivo
#   (el original no tenía ninguno — un fallo de parseo inesperado se iba sin
#   filtrar hasta el cliente).
# - cache_lock y lock_divisa ahora son locks propios de este módulo (en el
#   monolito vivían sueltos en el archivo principal).
cache_lock = asyncio.Lock()
lock_divisa = asyncio.Lock()

def normalizar_nombre_busqueda(nombre: str) -> str:
    basura = ["edition", "edición", "greatest hits", "platinum", "remastered", "bundle", "loose", "cib", "new", "goty"]
    nombre_limpio = nombre.lower()
    for p in basura:
        nombre_limpio = nombre_limpio.replace(p, "")
    return " ".join(nombre_limpio.split())

def generar_cache_key(nombre: str, consola: str) -> str:
    return f"{normalizar_nombre_busqueda(nombre)}::{consola.lower().strip()}"

async def obtener_precio_cache(llave: str):
    datos = cache_precios_ram.get(llave)
    if datos:
        logger.info("⚡ [CACHE HIT] Precio recuperado en O(1).")
        metricas_radar["cache_hits"] += 1
        if "mxn" not in datos["valores"] and "mxn_mercado" in datos["valores"]:
            datos["valores"]["mxn"] = datos["valores"]["mxn_mercado"]
        return datos["valores"]
    metricas_radar["cache_miss"] += 1
    return None

async def guardar_precio_cache(llave: str, valores: dict):
    async with cache_lock:
        cache_precios_ram[llave] = {"valores": valores}

async def obtener_dolar_hoy_async():
    ahora = time.time()
    async with lock_divisa:
        if ahora < CACHE_DIVISA["expira"]:
            return CACHE_DIVISA["valor"]

        try:
            if not http_client: return 18.00
            res = await http_client.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=HTTP_TIMEOUTS)
            if res.status_code == 200:
                val = float(res.json().get("rates", {}).get("MXN", 18.00))
                CACHE_DIVISA["valor"] = val
                CACHE_DIVISA["expira"] = ahora + 43200
                return val
        except Exception as e:
            logger.warning(f"⚠️ [DIVISAS ERROR] {e}")
        return CACHE_DIVISA["valor"]

# Mapa consola -> slug de PriceCharting, igual al del monolito.
SLUGS_PRICECHARTING = {
    "PS5": "playstation-5", "PS4": "playstation-4", "PS3": "playstation-3", "PS2": "playstation-2",
    "PS1": "playstation", "Xbox One": "xbox-one", "Xbox 360": "xbox-360", "Xbox Clasico": "xbox",
    "Nintendo Switch": "nintendo-switch", "Nintendo 3DS": "nintendo-3ds", "Nintendo DS": "nintendo-ds",
    "Nintendo 64": "nintendo-64", "GameCube": "gamecube", "GameBoy Advance": "gameboy-advance",
    "GameBoy Color": "gameboy-color", "Wii": "wii", "Wii U": "wii-u", "SNES": "super-nintendo",
    "NES": "nes", "Genesis": "sega-genesis"
}

def _respuesta_precio_vacia(status: str, nombre: str, tipo_cambio: float, rareza: str, url_pc: str, confidence: float = 0.0) -> dict:
    return {
        "status": status,
        "api_version": "v3",
        "nombre_corregido": nombre,
        "mxn": {"loose": 0.0, "cib": 0.0, "new": 0.0},
        "mxn_mercado": {"loose": 0.0, "cib": 0.0, "new": 0.0},
        "mxn_venta": {"loose": 0.0, "cib": 0.0, "new": 0.0},
        "usd": {"loose": 0.0, "cib": 0.0, "new": 0.0},
        "tipo_cambio": tipo_cambio,
        "rareza": rareza,
        "rareza_sugerida": "",
        "url_pc": url_pc,
        "confidence_score": confidence,
        "atributos_extra": {}
    }

# 🆕 RAREZA AUTOMÁTICA POR PRECIO REAL: para que el vendedor se entere si
# tiene algo de valor que no sabía que tenía, en vez de tener que adivinar la
# rareza ANTES de buscar el precio. Se usa el precio CIB (o Nuevo si no hay
# CIB) recién encontrado en PriceCharting — nunca lo que el vendedor haya
# puesto en el dropdown antes de la búsqueda, ya que ese valor previo pudo
# haber sido un supuesto equivocado (justo el caso que esto quiere evitar).
def determinar_rareza_por_precio(precio_mxn: float) -> str:
    if precio_mxn >= 2500: return "Élite"
    if precio_mxn >= 1000: return "Joya"
    if precio_mxn >= 400: return "Demandado"
    return "Común"

async def consultar_precio_pricecharting(nombre: str, consola: str = "", vendedor_id: str = "anonimo", dias_inventario: int = 0, rareza: str = "comun") -> dict:
    try:
        nombre = limpiar_texto(nombre)[:120]

        # 🚀 BYPASS SAAS MULTI-GIRO: si no hay consola o no aplica (otros giros
        # que no son videojuegos), evitamos gastar recursos en el scraper.
        if not consola or consola.lower() in ["n/a", "general", "ninguna", "otro", ""]:
            logger.info(f"🔄 [BYPASS SAAS] El artículo '{nombre}' no es un videojuego. Omitiendo scraper.")
            return _respuesta_precio_vacia("bypass_saas", nombre, await obtener_dolar_hoy_async(), rareza, "", 100.0)

        logger.info(f"🏷️ [RADAR ENTERPRISE] Buscando: '{nombre}' ({consola}) | Operador: {vendedor_id}")

        llave_cache = generar_cache_key(nombre, consola)
        valores_cacheados = await obtener_precio_cache(llave_cache)
        if valores_cacheados:
            valores_cacheados = dict(valores_cacheados)
            valores_cacheados["status"] = "ok_cached"
            return valores_cacheados

        tipo_cambio = await obtener_dolar_hoy_async()
        consola_web = consola.replace("Xbox Clasico", "Xbox").replace("GameBoy Advance", "GBA").replace("GameBoy Color", "GBC")
        nombre_normalizado = normalizar_nombre_busqueda(nombre)

        query = urllib.parse.quote_plus(nombre_normalizado + ' ' + consola_web)
        url_search = f"https://www.pricecharting.com/search-products?q={query}&type=prices"

        html_search = await obtener_html_escalonado_async(url_search, es_busqueda=True)
        if not html_search:
            logger.warning("⚠️ [RADAR PRECIOS] Falló la búsqueda HTML. Devolviendo contrato de error estructurado.")
            return _respuesta_precio_vacia("error", nombre, tipo_cambio, rareza, url_search, 0.0)

        soup = BeautifulSoup(html_search, 'html.parser')

        # 🛡️ Evitar AttributeError si el HTML de PriceCharting cambia de estructura
        tabla_juegos = soup.find(id="games_table")
        nodos_a_buscar = tabla_juegos.find_all('a', href=True) if tabla_juegos else soup.find_all('a', href=True)

        candidatos = []
        slug_esperado = SLUGS_PRICECHARTING.get(consola, consola_web.lower().replace(' ', '-'))

        for a in nodos_a_buscar:
            href = a['href'].lower()
            # Filtro real: descarta guías de estrategia, lotes, cajas solas y manuales sueltos —
            # esto es lo que evitaba que se confundiera el juego con accesorios o contenido no relacionado.
            if '/game/' in href and not any(b in href for b in ['strategy-guide', 'lot', 'bundle', 'box-only', 'manual-only']):
                score = 0.0
                if f"/{slug_esperado}/" in href: score += 40.0

                score += fuzz.token_sort_ratio(nombre_normalizado, normalizar_nombre_busqueda(a.text)) * 0.6
                if re.search(r'(-japan-|-jp-|-pal-|-eu-|-korea-)', href): score -= 50.0

                if score > 35.0:
                    url_limpia = a['href'].strip()
                    if not url_limpia.startswith("http"): url_limpia = "https://www.pricecharting.com" + url_limpia
                    candidatos.append({"url": url_limpia, "score": score})

        # 🛡️ Validación anti-HTML corrupto / exceso de links
        if len(candidatos) > 500:
            logger.error("🚨 [RADAR] HTML corrupto o envenenado. Exceso de candidatos.")
            raise Exception("HTML corrupto: exceso de candidatos en la búsqueda")

        nombre_oficial_pc, p_loose, p_cib, p_new = nombre, 0.0, 0.0, 0.0
        link_juego = None
        mejor_candidato = None

        if candidatos:
            mejor_candidato = max(candidatos, key=lambda x: x["score"])
            link_juego = mejor_candidato["url"]
            logger.info(f"🎯 [MATCHING AAA] Score {round(mejor_candidato['score'], 2)}/100 -> {link_juego}")

            # 🛡️ obtener_html_escalonado_async valida internamente que el dominio
            # sea pricecharting.com — segunda capa de defensa aunque link_juego
            # ya viene de un href que empieza por ahí.
            html_juego = await obtener_html_escalonado_async(link_juego, es_busqueda=False)
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
                        logger.warning(f"⚠️ [EXTRACTOR] Error parseando {id_css}: {e}")
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
        confianza_actual = round(mejor_candidato["score"], 2) if mejor_candidato else 0.0

        if p_loose == 0 and p_cib == 0:
            logger.warning(f"⚠️ [RADAR PRECIOS] Contingencia 0$ Absoluta para: '{nombre_oficial_pc}'.")
            respuesta_fallida = _respuesta_precio_vacia("warning_cero", nombre_oficial_pc, tipo_cambio, rareza, url_final_godot, confianza_actual)
            await guardar_precio_cache(llave_cache, respuesta_fallida)
            return respuesta_fallida

        mxn_loose_real = round(p_loose * tipo_cambio, 2)
        mxn_cib_real = round(p_cib * tipo_cambio, 2)
        mxn_new_real = round(p_new * tipo_cambio, 2)

        # 🆕 La rareza que de verdad se usa para calcular el precio sugerido de
        # venta es la determinada por el precio real (CIB de preferencia, si
        # no hay CIB entonces Nuevo, si no hay ninguno Suelto) — no la del
        # dropdown que el vendedor pudo haber dejado en "Común" por defecto.
        precio_referencia_rareza = mxn_cib_real if mxn_cib_real > 0 else (mxn_new_real if mxn_new_real > 0 else mxn_loose_real)
        rareza_sugerida = determinar_rareza_por_precio(precio_referencia_rareza)

        respuesta_final = {
            "status": "ok",
            "api_version": "v3",
            "nombre_corregido": nombre_oficial_pc,
            "mxn": {"loose": mxn_loose_real, "cib": mxn_cib_real, "new": mxn_new_real},
            "mxn_mercado": {"loose": mxn_loose_real, "cib": mxn_cib_real, "new": mxn_new_real},
            "mxn_venta": {
                "loose": calcular_precio_venta_inteligente_aaa(mxn_loose_real, 0, dias_inventario, rareza_sugerida.lower()),
                "cib": calcular_precio_venta_inteligente_aaa(mxn_cib_real, 0, dias_inventario, rareza_sugerida.lower()),
                "new": calcular_precio_venta_inteligente_aaa(mxn_new_real, 0, dias_inventario, rareza_sugerida.lower())
            },
            "usd": {"loose": p_loose, "cib": p_cib, "new": p_new},
            "tipo_cambio": tipo_cambio,
            "rareza": rareza,
            "rareza_sugerida": rareza_sugerida,
            "url_pc": url_final_godot,
            "confidence_score": confianza_actual,
            "atributos_extra": {}
        }

        await guardar_precio_cache(llave_cache, respuesta_final)
        logger.info(f"✅ [RADAR EXITO] Mercado CIB: ${mxn_cib_real} MXN | URL: {url_final_godot}")
        return respuesta_final

    except Exception as e:
        logger.exception(f"❌ [RADAR PRECIOS] Fallo crítico consultando precio para '{nombre}': {e}")
        return _respuesta_precio_vacia("error", nombre, 18.0, rareza, "", 0.0)
