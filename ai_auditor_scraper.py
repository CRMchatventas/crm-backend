# ==========================================================
# 🚀 MÓDULO: ai_auditor_scraper.py
# ==========================================================

import io
import time
import asyncio
import urllib.parse
import httpx
from datetime import datetime
from PIL import Image

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
2. La fecha debe ser HOY o AYER.
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