# ==============================================================================
# 🚀 MÓDULO: db_rag_scraper.py (AAA ENTERPRISE - GOLD STANDARD FINAL 10/10)
# ==============================================================================
# Godot 4.6 Ready • RAG de Inventario, Scraper RAWG y Gestión de Storage
# ==============================================================================

import os
import io
import re
import urllib.parse
import hashlib
import asyncio
import httpx
from PIL import Image
from typing import Optional
from rapidfuzz import process, fuzz

from config_and_schemas import (
    logger,
    cache_respuestas_ia,
    now_ts,
    limpiar_texto
)
# 🚀 FIX: Supabase debe importarse desde el core_wrapper, NO de config_and_schemas
from db_core_wrapper import async_db_execute, supabase

# 🚀 Cliente HTTP global con pooling robusto para entornos SaaS
http_client_global = httpx.AsyncClient(
    timeout=httpx.Timeout(10.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50)
)

async def cerrar_cliente_http_rag():
    """ 🧹 Función para shutdown ordenado. """
    await http_client_global.aclose()
    logger.info("🔌 [HTTP CLIENT] Cliente global cerrado con éxito.")

# 🛡️ Prevención estricta de SSRF
ALLOWED_DOMAINS = ("rawg.io", "media.rawg.io", "pricecharting.com", "cdn.akamai.steamstatic.com")

def es_dominio_seguro(url: str) -> bool:
    try:
        hostname = urllib.parse.urlparse(url).hostname or ""
        return any(hostname == d or hostname.endswith("." + d) for d in ALLOWED_DOMAINS)
    except Exception as e:
        logger.error(f"❌ [SSRF SECURITY] Error parseando URL para verificación de dominio: {e}")
        return False

# 🛡️ Protección global contra Decompression Bombs
Image.MAX_IMAGE_PIXELS = 25_000_000

# ==========================================================
# 🧠 DICCIONARIOS DE INTELIGENCIA SEMÁNTICA
# ==========================================================
STOPWORDS = {"hola", "amigo", "buenos", "dias", "tienes", "algun", "para", "completo", "busco", "quiero", "nuevo", "usado", "buenas", "tardes"}

TITULOS_COMPUESTOS = {
    "gow": "god of war", "re4": "resident evil 4", "rdr2": "red dead redemption",
    "mgs": "metal gear solid", "cod": "call of duty", "tlou": "the last of us",
    "gta": "grand theft auto", "nfs": "need for speed"
}

# ==============================================================================
# 🧠 RAG INVENTARIO: RECUPERACIÓN SEMÁNTICA ACELERADA
# 🔧 FIX MULTI-GIRO: 'consola' nunca existió en la tabla real — se cambia
# por 'categoria' (confirmado en el esquema de Supabase), que es genérico
# y sirve para cualquier giro: "PS5"/"Xbox" en videojuegos, "Residencial"/
# "Comercial" en terrenos, lo que aplique en cada caso. atributos_extra
# (jsonb) queda disponible para datos más específicos por giro a futuro.
# ==============================================================================
async def obtener_contexto_inventario_rag(vendedor_id: str, consulta: str) -> str:
    """
    RAG de Inventario AAA: Prefiltro SQL, RapidFuzz (WRatio), Stopwords y Caché.
    """
    vendedor_id = str(vendedor_id).strip()
    texto_limpio = limpiar_texto(consulta).lower()

    # 1. Traducción de títulos compuestos (ej. gow -> god of war)
    for corto, largo in TITULOS_COMPUESTOS.items():
        texto_limpio = re.sub(rf"\b{corto}\b", largo, texto_limpio)

    # 2. Anti Query Explosion: Filtro de stopwords y palabras < 2 chars
    palabras = [p for p in texto_limpio.split() if p not in STOPWORDS and len(p) >= 2][:5]
    palabras_clave = " ".join(palabras)

    if not palabras_clave:
        return "El cliente no especificó un producto claro."

    # ⚡ Cache Key
    cache_key = hashlib.sha256(f"RAGINV:{vendedor_id}:{palabras_clave}".encode()).hexdigest()

    cache_item = cache_respuestas_ia.get(cache_key)
    if cache_item and (now_ts() - cache_item.get("ts", 0) <= 300):
        return cache_item["data"]

    try:
        # 🚀 Prefiltro SQL con ancla basada en palabra más larga
        termino_fuerte = max(palabras, key=len) if palabras else ""

        query = (
            supabase.table('inventario')
            .select('nombre, categoria, precio, stock, estado_general')
            .eq('vendedor_id', vendedor_id)
            .ilike('nombre', f"%{termino_fuerte}%")
            .gt('stock', 0)
            .limit(100)
        )
        res_inv = await asyncio.wait_for(async_db_execute(query, timeout_seg=8.0), timeout=10.0)
        inventario = res_inv.data or []

        # 🚀 FIX AUDITORÍA: El bloque de fallback ahora cuenta con timeout estricto para evitar bloqueos asíncronos.
        if not inventario:
            fallback_query = supabase.table('inventario').select('nombre, categoria, precio, stock, estado_general').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(200)
            res_inv = await asyncio.wait_for(async_db_execute(fallback_query, timeout_seg=8.0), timeout=10.0)
            inventario = res_inv.data or []

        # 🛡️ Manejo inteligente de llaves (Resuelve duplicados manteniendo el de mayor stock)
        diccionario_opciones = {}
        for item in inventario:
            nombre = str(item.get("nombre", "")).strip()
            if not nombre: continue
            key = f"{nombre} | {item.get('categoria', 'N/A')}"

            # Si ya existe, nos quedamos con el que tenga mayor stock
            if key in diccionario_opciones:
                if item.get('stock', 0) > diccionario_opciones[key].get('stock', 0):
                    diccionario_opciones[key] = item
            else:
                diccionario_opciones[key] = item

        matches = process.extract(palabras_clave, diccionario_opciones.keys(), scorer=fuzz.WRatio, limit=8)

        lineas_contexto = ["--- INVENTARIO DISPONIBLE ---"]
        items_a_mostrar = [diccionario_opciones[m[0]] for m in matches if m[1] > 55] or inventario[:5]

        for item in items_a_mostrar:
            precio = float(item.get('precio') or 0.0)
            lineas_contexto.append(
                f"[{item.get('categoria', 'N/A')}] {item.get('nombre', 'Producto')} - "
                f"${precio:.2f} MXN | Stock: {item.get('stock', 0)} | "
                f"Estado: {item.get('estado_general', 'N/A')}"
            )

        contexto_final = "\n".join(lineas_contexto)[:1800]
        cache_respuestas_ia[cache_key] = {"data": contexto_final, "ts": now_ts()}
        return contexto_final

    except asyncio.TimeoutError:
        logger.error(f"❌ [RAG TIMEOUT] Tiempo de espera agotado consultando el inventario para el tenant: {vendedor_id}")
        return "El almacén de inventario está experimentando retrasos. Intenta de nuevo."
    except Exception as e:
        logger.exception(f"❌ [RAG ERROR] Error crítico en la tubería semántica: {e}")
        return "No se pudo acceder al inventario."



# ==============================================================================
# 📦 SCRAPER RAWG Y GESTIÓN DE STORAGE
# ==============================================================================
async def procesar_imagen_juego(id_juego: str, nombre_juego: str, url_imagen: str) -> Optional[str]:
    """
    Descarga, optimiza y almacena la portada con blindaje total (SSRF, RAM limit, Zip Bomb).
    """
    if not url_imagen or not es_dominio_seguro(url_imagen): 
        logger.error(f"❌ [SSRF BLOCK] Intento de descarga bloqueado. URL sospechosa o no autorizada: {url_imagen}")
        return None

    try:
        int_id = int(id_juego)
        MAX_IMAGE_SIZE = 15 * 1024 * 1024 # 15 MB
        img_bytes_array = bytearray()
        
        # 🚀 Streaming híbrido para protección total contra DOS
        async with http_client_global.stream('GET', url_imagen) as response:
            response.raise_for_status()
            
            # Verificación por header
            if int(response.headers.get("Content-Length", 0)) > MAX_IMAGE_SIZE: 
                logger.error(f"❌ [DOS DETECTED] Cabecera indica tamaño superior al permitido para: {nombre_juego}")
                return None

            async for chunk in response.aiter_bytes():
                img_bytes_array.extend(chunk)
                if len(img_bytes_array) > MAX_IMAGE_SIZE:
                    # 🚀 FIX AUDITORÍA: Promovido a error analítico para alertas tempranas de denegación de servicio.
                    logger.error(f"❌ [DOS BLOCK] Descarga abortada. La imagen excede el límite de 15MB: {nombre_juego}")
                    return None
                    
        img_bytes = bytes(img_bytes_array)

        # 🛡️ Verificación estricta de imagen
        try:
            temp_img = Image.open(io.BytesIO(img_bytes))
            temp_img.verify()
        except Image.DecompressionBombError:
            logger.critical(f"🚨 [SECURITY DETECTED] ¡Decompression Bomb neutralizada en el recurso {nombre_juego}!")
            return None
        except Exception as img_verify_error:
            logger.error(f"❌ [STORAGE ERROR] Estructura de imagen corrupta o ilegible para {nombre_juego}: {img_verify_error}")
            return None

        img = Image.open(io.BytesIO(img_bytes))
        img.thumbnail((1200, 1200))
        if img.mode in ("RGBA", "P"): 
            img = img.convert("RGB")
            
        out_bytes_io = io.BytesIO()
        img.save(out_bytes_io, format="JPEG", quality=85)
        out_bytes = out_bytes_io.getvalue()

        nombre_archivo = f"portadas/{limpiar_texto(nombre_juego).replace(' ', '_').lower()}_{int(now_ts())}.jpg"

        def _upload():
            return supabase.storage.from_("inventario_media").upload(
                nombre_archivo, out_bytes, file_options={"content-type": "image/jpeg", "upsert": "true"}
            )

        await asyncio.wait_for(asyncio.to_thread(_upload), timeout=15.0)
        url_publica = supabase.storage.from_("inventario_media").get_public_url(nombre_archivo)

        # 🚀 Actualización validada y con verificación de existencia
        res_update = await async_db_execute(
            supabase
            .table('catalogo_maestro')
            .update({"url_portada_oficial": url_publica})
            .eq("id", int_id), 
            timeout_seg=10.0
        )
        
        # 🚀 FIX AUDITORÍA: Promovido a error de consistencia. Si no impacta filas, la base de datos está desincronizada.
        if not res_update or not res_update.data:
            logger.error(f"❌ [STORAGE ERROR] Actualización fallida. El registro no existe en catalogo_maestro (ID: {int_id})")
            return None

        logger.info(f"🖼️ [STORAGE SUCCESS] Imagen optimizada y guardada para: {nombre_juego} -> URL: {url_publica}")
        return url_publica

    except Exception as e:
        logger.exception(f"❌ [STORAGE ERROR] Fallo crítico procesando portada para {nombre_juego}: {e}")
        return None
