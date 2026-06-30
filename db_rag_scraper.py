# ==============================================================================
# 🚀 MÓDULO: db_rag_scraper.py (AAA ENTERPRISE - GOLD STANDARD FINAL 10/10)
# ==============================================================================
# Godot 4.6 Ready • RAG de Inventario, Scraper RAWG y Gestión de Storage
#
# 🔥 PATCH (sesión actual) — RESERVA TEMPORAL DE ÚLTIMA UNIDAD:
# El bot nunca descuenta inventario real solo (eso lo hace un humano a mano en
# el Visor). Lo único automático es un "hold" de 1 hora cuando detecta intención
# de compra sobre un producto con stock == 1 (ver db_api_endpoints.py). Mientras
# ese hold esté activo, este RAG debe presentar ese producto como agotado para
# CUALQUIER conversación, no solo la que lo activó — si no, el bot podría
# seguir ofreciéndolo a un segundo cliente mientras el primero completa el pago.
# ✅ FIX: el caché de 5 minutos ahora guarda la LISTA cruda de productos, no el
#    texto ya formateado. El filtro de holds se aplica SIEMPRE al final, sobre
#    datos cacheados o frescos por igual — así un hold creado después de que
#    algo quedó en caché sí se refleja de inmediato, en vez de esperar a que
#    el caché expire solo.
# ==============================================================================

import os
import io
import re
import urllib.parse
import hashlib
import asyncio
import httpx
from datetime import datetime, timezone
from PIL import Image
from typing import Optional
from rapidfuzz import fuzz

from config_and_schemas import (
    logger,
    cache_respuestas_ia,
    now_ts,
    limpiar_texto,
    RESERVAS_TEMPORALES_ULTIMA_UNIDAD,
    RAWG_API_KEY
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
STOPWORDS = {
    "hola", "amigo", "buenos", "dias", "tienes", "algun", "para", "completo",
    "busco", "quiero", "nuevo", "usado", "buenas", "tardes",
    # 🛡️ FIX: faltaban los artículos/preposiciones más comunes — sin esto,
    # una pregunta como "tienes el juego de borderland?" le mandaba al
    # comparador de relevancia "el juego de borderland" completo en vez de
    # solo "juego borderland", diluyendo el puntaje de coincidencia con
    # ruido que no aporta nada para encontrar el producto.
    "el", "la", "los", "las", "un", "una", "unos", "unas",
    "de", "del", "en", "y", "o", "que", "con", "por",
    "ese", "esa", "esos", "esas", "este", "esta", "estos", "estas",
}

TITULOS_COMPUESTOS = {
    "gow": "god of war", "re4": "resident evil 4", "rdr2": "red dead redemption",
    "mgs": "metal gear solid", "cod": "call of duty", "tlou": "the last of us",
    "gta": "grand theft auto", "nfs": "need for speed"
}

# ==========================================================
# ⏳ FILTRO DE RESERVAS TEMPORALES (ver patch arriba)
# ==========================================================
# ==========================================================
# 💸 PISO DE DESCUENTO VIGENTE (descuento escalonado por tiempo)
# ==========================================================
def calcular_precio_producto(item: dict, dias_rotacion_max: int = 90) -> dict:
    """
    🆕 MOTOR DE PRECIOS (PRICING ENGINE) — fuente única de verdad para TODO
    el sistema. Reemplaza a _calcular_precio_vigente_con_rampa. Nadie más en
    el sistema (IA, Visor, CSV, Store, Dashboard) debe calcular un precio por
    su cuenta — todos llaman a esta función.

    DISEÑO FINAL (validado con casos reales de negocio):

      valor_protegido  = MAX(costo, mínimo_manual, valor_de_mercado)
      piso_real        = MAX(costo, valor_protegido × % autorizado)
      destino_rotación = MIN(precio_lista, piso_real)
          ↑ nunca mayor a la lista — si no, "rotación" subiría el precio.
      precio_curva     = interpola precio_lista → destino_rotación con una
                          curva ease-in (lento al inicio, rápido al final —
                          así un comerciante real piensa: "casi no bajo al
                          principio, bajo fuerte cerca del final").
      precio_vigente   = MAX(precio_curva, piso_real)
          ↑ ESTO es lo que resuelve el caso real: si el valor de mercado
          SUBE por encima del precio de lista, el precio vigente sube con
          él automáticamente — sin importar si hay una rampa de rotación
          corriendo o no. El piso protege en ambas direcciones, no solo
          hacia abajo.
      piso_regateo     = MAX(precio_vigente × (1 - % regateo), costo)
          ↑ SOLO el costo frena la negociación del bot — el valor de
          mercado define dónde EMPIEZA a cotizar, nunca hasta dónde puede
          ceder regateando. Eso es intencional: el mercado es una
          referencia de cuánto vale, no un límite de cuánta ganancia exigir.

    🛡️ CANDADO DE SEGURIDAD ABSOLUTO — código, no configuración: precio_vigente
    y piso_regateo NUNCA pueden caer por debajo de 'costo', sin importar qué
    tan mal se configure cualquier campo. Si confiamos la venta de un
    producto a un bot, no puede haber forma de perder dinero por un error
    de captura.

    🌐 MULTI-GIRO: esta función es genérica por diseño. 'valor_mercado' solo
    participa si 'usar_precio_mercado_como_destino' está activo Y el
    producto tiene un 'precio_mercado_referencia' guardado — en giros sin
    un equivalente a PriceCharting (todo lo que no sea videojuegos hoy),
    esos campos simplemente nunca se llenan, y la función sigue funcionando
    igual de bien solo con costo + mínimo manual. No hace falta ninguna
    rama especial por giro.
    """
    precio_lista = float(item.get('precio') or 0)
    costo = float(item.get('costo') or 0)

    # ── 1. VALOR PROTEGIDO: lo más alto entre lo que sabemos que vale de verdad ──
    minimo_manual_raw = item.get('precio_minimo_rotacion')
    minimo_manual = float(minimo_manual_raw) if minimo_manual_raw is not None else 0.0

    usar_mercado = bool(item.get('usar_precio_mercado_como_destino', False))
    valor_mercado = 0.0
    if usar_mercado:
        ref_raw = item.get('precio_mercado_referencia')
        valor_mercado = float(ref_raw) if ref_raw is not None else 0.0

    valor_protegido = max(costo, minimo_manual, valor_mercado)

    # ── 2. ¿Autorizó el vendedor vender por debajo del valor protegido? ──
    # 100 (default) = nunca. Es una decisión EXPLÍCITA del vendedor, nunca
    # algo que el sistema decida solo.
    try:
        pct_autorizado = float(item.get('permitir_bajo_valor_protegido_pct', 100) or 100)
    except (TypeError, ValueError):
        pct_autorizado = 100.0
    pct_autorizado = max(0.0, min(100.0, pct_autorizado))
    piso_real = max(costo, valor_protegido * (pct_autorizado / 100.0))  # 🛡️ candado: nunca bajo costo

    # ── 3. DESTINO DE ROTACIÓN: nunca mayor al precio de lista ──
    rotacion_configurada = (minimo_manual > 0) or (usar_mercado and valor_mercado > 0)
    if rotacion_configurada:
        destino_rotacion = min(precio_lista, piso_real)
    else:
        destino_rotacion = precio_lista  # sin rotación configurada, no hay a dónde bajar

    # ── 4. DÍAS TRANSCURRIDOS (desde fecha_inicio_rotacion, NO fecha_alta) ──
    fecha_inicio_raw = item.get('fecha_inicio_rotacion') or item.get('fecha_alta')
    dias_transcurridos = 0
    if fecha_inicio_raw:
        try:
            texto_fecha = str(fecha_inicio_raw).replace('Z', '+00:00')
            dt_inicio = datetime.fromisoformat(texto_fecha)
            if dt_inicio.tzinfo is None:
                dt_inicio = dt_inicio.replace(tzinfo=timezone.utc)
            dias_transcurridos = max(0, (datetime.now(timezone.utc) - dt_inicio).days)
        except Exception as e:
            logger.warning(f"⚠️ [PRICING ENGINE] No se pudo parsear fecha_inicio_rotacion='{fecha_inicio_raw}': {e}")
            dias_transcurridos = 0

    dias_max = max(1, int(dias_rotacion_max or 90))

    # ── 5. CURVA DE ROTACIÓN (ease-in: lento al inicio, rápido al final) ──
    if dias_transcurridos <= 0:
        precio_curva = precio_lista
    elif dias_transcurridos >= dias_max:
        precio_curva = destino_rotacion
    else:
        progreso = (dias_transcurridos / dias_max) ** 2  # cuadrática, no lineal
        precio_curva = precio_lista - (precio_lista - destino_rotacion) * progreso

    # ── 6. PRECIO VIGENTE: protege en AMBAS direcciones ──
    # Nunca cotiza por debajo del piso real (protección de siempre), pero
    # TAMPOCO se queda atorado en un precio de lista viejo si el valor real
    # (mercado) subió por encima de él — sube con él automáticamente, haya
    # o no rotación corriendo.
    precio_vigente = max(precio_curva, piso_real)
    precio_vigente = max(precio_vigente, costo)  # 🛡️ candado final, redundante a propósito

    # ── 7. PISO DE REGATEO: solo el costo frena al bot ──
    try:
        pct_max_desc = float(item.get('descuento_max_porcentaje') or 0.0)
    except (TypeError, ValueError):
        pct_max_desc = 0.0
    piso_regateo = None
    if pct_max_desc > 0:
        piso_regateo = max(precio_vigente * (1 - (pct_max_desc / 100.0)), costo)  # 🛡️ candado

    # ── 8. ESTADO Y MOTIVO — diagnóstico para Visor/Dashboard. La IA NUNCA
    # recibe esto, solo precio_vigente y piso_regateo. ──
    if usar_mercado and valor_mercado > precio_lista > 0:
        estado = "SUBVALUADO"
        motivo = f"El valor de mercado (${valor_mercado:.2f}) es mayor a tu precio de lista (${precio_lista:.2f}) — considera subir el precio de lista."
    elif precio_vigente <= costo + 0.01:
        estado = "COSTO"
        motivo = "El precio vigente está en el límite del costo — no hay margen de regateo adicional."
    elif rotacion_configurada and dias_transcurridos >= dias_max:
        estado = "ROTACION_MIN"
        motivo = f"Llegó al mínimo de rotación tras {dias_transcurridos} día(s) sin venderse."
        if dias_transcurridos >= dias_max + 1:
            # Llegó a su mínimo hace tiempo y nadie lo ha revisado — esto es
            # lo que alimenta una futura sección de "Pendientes de Revisión"
            # en el dashboard, sin necesidad de un cron job aparte.
            estado = "REVISION"
            motivo = f"Lleva {dias_transcurridos} días en su precio mínimo de rotación — vale la pena revisarlo."
    elif rotacion_configurada and dias_transcurridos > 0:
        estado = "ROTACION"
        motivo = f"En rampa de rotación: día {dias_transcurridos}/{dias_max}."
    else:
        estado = "NORMAL"
        motivo = "Precio de lista normal, sin rotación activa."

    return {
        "precio_lista": round(precio_lista, 2),
        "precio_vigente": round(precio_vigente, 2),
        "piso_regateo": round(piso_regateo, 2) if piso_regateo is not None else None,
        "valor_protegido": round(valor_protegido, 2),
        "piso_real": round(piso_real, 2),
        "destino_rotacion": round(destino_rotacion, 2),
        "dias_transcurridos": dias_transcurridos,
        "dias_rotacion_max": dias_max,
        "rotacion_activa": rotacion_configurada,
        "estado": estado,
        "motivo": motivo,
        # 🛡️ Alias por compatibilidad con cualquier llamador que todavía
        # busque el nombre del campo viejo durante la transición.
        "rotacion_activa_legacy": rotacion_configurada,
    }


# 🛡️ Alias de compatibilidad — por si algún punto de llamada todavía no se
# actualizó al nombre nuevo durante el despliegue. Se puede retirar una vez
# confirmado que ya nada lo usa.
_calcular_precio_vigente_con_rampa = calcular_precio_producto


def _filtrar_items_reservados(vendedor_id: str, items: list) -> list:
    """
    Excluye cualquier artículo con un hold de última-unidad activo (llave
    "vendedor_id:id_item" en RESERVAS_TEMPORALES_ULTIMA_UNIDAD). El stock real
    en Supabase sigue diciendo 1; para efectos de lo que el bot puede ofrecer,
    debe verse agotado hasta que el hold expire (1h) o un humano lo descuente
    de verdad en el Visor.
    """
    resultado = []
    for item in items:
        item_id = item.get('id')
        llave = f"{vendedor_id}:{item_id}"
        if item_id is not None and llave in RESERVAS_TEMPORALES_ULTIMA_UNIDAD:
            continue
        resultado.append(item)
    return resultado

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

    # 🆕 MOTOR DE PRECIOS: necesitamos saber el giro (para decidir si el
    # refresco "justo a tiempo" de mercado aplica — solo tiene sentido para
    # videojuegos) y los días de rampa configurados para este negocio.
    # Cacheado 60s (mismo TTLCache que ya usa el RAG) — esto casi nunca
    # cambia, no vale la pena consultarlo en cada mensaje.
    cache_key_config = hashlib.sha256(f"RAGCONFIG:{vendedor_id}".encode()).hexdigest()
    config_cache = cache_respuestas_ia.get(cache_key_config)
    if config_cache:
        giro_vendedor, dias_rotacion_max = config_cache["giro"], config_cache["dias_rotacion_max"]
    else:
        giro_vendedor, dias_rotacion_max = "", 90
        try:
            res_conf = await asyncio.wait_for(
                async_db_execute(supabase.table('configuracion_bot').select('giro, dias_rotacion_max').eq('vendedor_id', vendedor_id).limit(1), timeout_seg=5.0),
                timeout=6.0
            )
            if res_conf.data:
                giro_vendedor = str(res_conf.data[0].get('giro') or '').lower()
                dias_rotacion_max = int(res_conf.data[0].get('dias_rotacion_max') or 90)
        except Exception as e:
            logger.warning(f"⚠️ [PRICING ENGINE] No se pudo leer configuración de {vendedor_id}, usando defaults: {e}")
        cache_respuestas_ia[cache_key_config] = {"giro": giro_vendedor, "dias_rotacion_max": dias_rotacion_max}
    es_giro_videojuegos = "videojueg" in giro_vendedor

    # ⚡ Cache Key
    cache_key = hashlib.sha256(f"RAGINV:{vendedor_id}:{palabras_clave}".encode()).hexdigest()

    items_a_mostrar = None
    cache_item = cache_respuestas_ia.get(cache_key)
    if cache_item and (now_ts() - cache_item.get("ts", 0) <= 300):
        # 🛡️ FIX RESERVA: se guarda la LISTA cruda, no el texto final — el
        # filtro de holds (más abajo) se aplica siempre, incluso en cache hit.
        items_a_mostrar = cache_item["data"]

    if items_a_mostrar is None:
        try:
            # 🚀 Prefiltro SQL con ancla basada en palabra más larga
            termino_fuerte = max(palabras, key=len) if palabras else ""

            query = (
                supabase.table('inventario')
                .select('id, nombre, categoria, genero, precio, costo, stock, estado_general, descripcion_detallada, fecha_alta, fecha_inicio_rotacion, descuento_max_porcentaje, precio_minimo_rotacion, usar_precio_mercado_como_destino, precio_mercado_referencia, precio_mercado_actualizado_en, permitir_bajo_valor_protegido_pct')
                .eq('vendedor_id', vendedor_id)
                .ilike('nombre', f"%{termino_fuerte}%")
                .gt('stock', 0)
                .limit(100)
            )
            res_inv = await asyncio.wait_for(async_db_execute(query, timeout_seg=8.0), timeout=10.0)
            inventario = res_inv.data or []

            # 🆕 BÚSQUEDA POR GÉNERO: si el cliente pregunta por categoría en vez
            # de un título ("qué tienes de terror", "busco algo de fútbol"), el
            # filtro de arriba (por nombre) normalmente no encuentra nada, porque
            # la palabra de género casi nunca aparece en el TÍTULO del juego. Se
            # complementa con una búsqueda directa sobre la columna 'genero' y se
            # combinan los resultados — así "terror" encuentra juegos de terror
            # aunque ninguno se llame literalmente "Terror".
            if termino_fuerte:
                try:
                    query_genero = (
                        supabase.table('inventario')
                        .select('id, nombre, categoria, genero, precio, costo, stock, estado_general, descripcion_detallada, fecha_alta, fecha_inicio_rotacion, descuento_max_porcentaje, precio_minimo_rotacion, usar_precio_mercado_como_destino, precio_mercado_referencia, precio_mercado_actualizado_en, permitir_bajo_valor_protegido_pct')
                        .eq('vendedor_id', vendedor_id)
                        .ilike('genero', f"%{termino_fuerte}%")
                        .gt('stock', 0)
                        .limit(50)
                    )
                    res_genero = await asyncio.wait_for(async_db_execute(query_genero, timeout_seg=8.0), timeout=10.0)
                    ids_ya_presentes = {it.get('id') for it in inventario}
                    for it in (res_genero.data or []):
                        if it.get('id') not in ids_ya_presentes:
                            inventario.append(it)
                except Exception as e:
                    logger.warning(f"⚠️ [RAG GÉNERO] Búsqueda complementaria por género falló, se ignora: {e}")

            # 🚀 FIX AUDITORÍA: El bloque de fallback ahora cuenta con timeout estricto para evitar bloqueos asíncronos.
            if not inventario:
                fallback_query = supabase.table('inventario').select('id, nombre, categoria, genero, precio, costo, stock, estado_general, descripcion_detallada, fecha_alta, fecha_inicio_rotacion, descuento_max_porcentaje, precio_minimo_rotacion, usar_precio_mercado_como_destino, precio_mercado_referencia, precio_mercado_actualizado_en, permitir_bajo_valor_protegido_pct').eq('vendedor_id', vendedor_id).gt('stock', 0).limit(200)
                res_inv = await asyncio.wait_for(async_db_execute(fallback_query, timeout_seg=8.0), timeout=10.0)
                inventario = res_inv.data or []

            # 🛡️ Manejo inteligente de llaves (Resuelve duplicados manteniendo el de mayor stock)
            diccionario_opciones = {}
            for item in inventario:
                nombre = str(item.get("nombre", "")).strip()
                if not nombre: continue
                # 🛡️ FIX: antes la llave era solo "nombre | categoria(consola)" —
                # si el mismo título existía en dos condiciones distintas (ej.
                # Batman PS4 "Completo" a $600 y "Nuevo" a $1900), las dos caían
                # en la MISMA llave y se quedaba solo una (la de mayor stock),
                # descartando la otra en silencio. El bot nunca podía informar
                # los distintos precios por condición porque el RAG ya solo le
                # mandaba uno.
                key = f"{nombre} | {item.get('categoria', 'N/A')} | {item.get('estado_general', 'N/A')}"

                # Si ya existe, nos quedamos con el que tenga mayor stock
                if key in diccionario_opciones:
                    if item.get('stock', 0) > diccionario_opciones[key].get('stock', 0):
                        diccionario_opciones[key] = item
                else:
                    diccionario_opciones[key] = item

            # 🛡️ FIX: antes la comparación de relevancia se hacía contra la llave
            # completa ("Nombre | Categoria | Estado") — el texto de categoría y
            # condición no aporta nada para saber si el producto coincide con lo
            # que el cliente pidió, solo diluye el puntaje. Ahora se compara
            # contra el nombre solo; la llave compuesta se sigue usando para no
            # perder distintas condiciones/precios del mismo título (ver fix de
            # arriba), pero ya no afecta qué tan bien "puntúa" como coincidencia.
            #
            # Se usa fuzz.WRatio() directo en vez de process.extract() con un
            # diccionario — esto evita depender de la forma exacta en que esa
            # función empaqueta sus resultados (varía si se le pasa una lista o
            # un mapeo), que no se pudo verificar en este entorno. Con menos de
            # un puñado de candidatos por mensaje, el costo de hacerlo uno por
            # uno es insignificante.
            candidatos_con_score = []
            for key, item in diccionario_opciones.items():
                score = fuzz.WRatio(palabras_clave, item.get("nombre", ""))
                if score > 55:
                    candidatos_con_score.append((score, item))
            candidatos_con_score.sort(key=lambda x: x[0], reverse=True)
            items_a_mostrar = [item for _, item in candidatos_con_score[:8]] or inventario[:5]

            # 🆕 FIX: log de diagnóstico — antes, si el RAG no encontraba algo que
            # sí estaba en inventario, no había forma de saber por qué (¿no lo
            # encontró el prefiltro SQL? ¿lo encontró pero no pasó el umbral de
            # similitud?) sin adivinar. Ahora queda visible en los logs de Render.
            logger.info(
                f"🔍 [RAG INVENTARIO] consulta='{consulta[:80]}' palabras_clave='{palabras_clave}' "
                f"termino_fuerte='{termino_fuerte}' candidatos_sql={len(inventario)} "
                f"mostrados={len(items_a_mostrar)} nombres={[i.get('nombre') for i in items_a_mostrar][:8]}"
            )

            cache_respuestas_ia[cache_key] = {"data": items_a_mostrar, "ts": now_ts()}

        except asyncio.TimeoutError:
            logger.error(f"❌ [RAG TIMEOUT] Tiempo de espera agotado consultando el inventario para el tenant: {vendedor_id}")
            return "El almacén de inventario está experimentando retrasos. Intenta de nuevo."
        except Exception as e:
            logger.exception(f"❌ [RAG ERROR] Error crítico en la tubería semántica: {e}")
            return "No se pudo acceder al inventario."

    # ⏳ FIX RESERVA DE ÚLTIMA UNIDAD: se aplica SIEMPRE al final, sobre datos
    # cacheados o frescos por igual — un artículo con un hold activo se quita
    # del contexto, exactamente como si no quedara stock.
    items_disponibles = _filtrar_items_reservados(vendedor_id, items_a_mostrar)

    # 🆕 REFRESCO "JUSTO A TIEMPO" DEL VALOR DE MERCADO — en vez de revisar
    # TODO el inventario contra PriceCharting cada cierto tiempo (caro, lento,
    # innecesario para productos que nadie pregunta), solo se refresca el
    # producto exacto que el cliente preguntó, y solo cuando la búsqueda
    # encontró UN solo producto claro (no una lista de "tal vez es uno de
    # estos 8" — ahí no hay un producto específico que refrescar).
    #
    # Protecciones:
    #  - Solo aplica a videojuegos (es lo único con un PriceCharting real).
    #  - Cooldown de 24h por producto (precio_mercado_actualizado_en) — si 5
    #    clientes preguntan por el mismo juego el mismo día, solo el primero
    #    dispara la consulta real.
    #  - Timeout corto (4s): si PriceCharting tarda o falla, el bot sigue con
    #    el último valor guardado en vez de dejar al cliente esperando.
    if es_giro_videojuegos and len(items_disponibles) == 1:
        await _refrescar_valor_mercado_justo_a_tiempo(vendedor_id, items_disponibles[0])

    lineas_contexto = ["--- INVENTARIO DISPONIBLE ---"]
    if not items_disponibles:
        lineas_contexto.append("(Sin productos disponibles que coincidan con la búsqueda — todo lo encontrado está agotado o reservado temporalmente)")
    else:
        for item in items_disponibles:
            calculo_precio = calcular_precio_producto(item, dias_rotacion_max)
            precio = calculo_precio["precio_vigente"]
            genero_txt = str(item.get('genero') or '').strip()
            sufijo_genero = f" | Género: {genero_txt}" if genero_txt else ""
            condicion_txt = str(item.get('descripcion_detallada') or '').strip()
            sufijo_condicion = f" | Condición: {condicion_txt}" if condicion_txt else ""
            # 🆕 FIX: ya no es "Piso de descuento autorizado AHORA" basado en 3
            # fechas fijas — ahora es el resultado del Motor de Precios
            # completo (costo, mínimo manual, mercado, rotación y % de
            # regateo, todos ya combinados en un solo número final). Nunca se
            # le pide a la IA que haga la cuenta — eso ya pasó aquí, en
            # Python, antes de que el texto llegue al prompt.
            sufijo_descuento = ""
            if calculo_precio["piso_regateo"] is not None:
                sufijo_descuento = f" | Piso de regateo autorizado AHORA: ${calculo_precio['piso_regateo']:.2f} MXN"
            elif calculo_precio["rotacion_activa"]:
                sufijo_descuento = " | Este precio ya refleja su valor protegido — no hay regateo adicional autorizado sobre él"
            lineas_contexto.append(
                f"[{item.get('categoria', 'N/A')}] {item.get('nombre', 'Producto')} - "
                f"${precio:.2f} MXN | Stock: {item.get('stock', 0)} | "
                f"Estado: {item.get('estado_general', 'N/A')}{sufijo_condicion}{sufijo_genero}{sufijo_descuento}"
            )

    return "\n".join(lineas_contexto)[:1800]


async def _refrescar_valor_mercado_justo_a_tiempo(vendedor_id: str, item: dict) -> None:
    """
    🆕 Refresca precio_mercado_referencia para UN producto específico, justo
    antes de que el bot responda sobre él — en vez de un cron job revisando
    todo el catálogo (caro e innecesario). Modifica 'item' en el lugar
    (in-place) para que la MISMA respuesta ya use el valor fresco, además de
    guardarlo en la base para las próximas consultas.

    No bloquea la respuesta del bot por mucho tiempo: timeout corto, y
    cualquier fallo se ignora en silencio — el bot sigue con el valor
    guardado anterior (puede estar desactualizado, pero sigue siendo mejor
    que dejar al cliente esperando o romper la conversación).
    """
    item_id = item.get('id')
    if item_id is None:
        return

    # 🛡️ Cooldown de 24h — no tiene caso re-consultar un producto que ya se
    # refrescó hace unas horas.
    actualizado_raw = item.get('precio_mercado_actualizado_en')
    if actualizado_raw:
        try:
            texto_fecha = str(actualizado_raw).replace('Z', '+00:00')
            dt_actualizado = datetime.fromisoformat(texto_fecha)
            if dt_actualizado.tzinfo is None:
                dt_actualizado = dt_actualizado.replace(tzinfo=timezone.utc)
            horas_desde_refresco = (datetime.now(timezone.utc) - dt_actualizado).total_seconds() / 3600.0
            if horas_desde_refresco < 24.0:
                return
        except Exception:
            pass  # fecha rara/corrupta — mejor refrescar de más que quedarse con un dato sospechoso

    try:
        from ai_auditor_scraper import consultar_precio_pricecharting
        nombre = str(item.get('nombre', '')).strip()
        consola = str(item.get('categoria', '')).strip()
        if not nombre:
            return

        resultado = await asyncio.wait_for(
            consultar_precio_pricecharting(nombre, consola, vendedor_id, 0, "comun"),
            timeout=4.0  # 🛡️ corto a propósito — nunca debe sentirse como que el bot "se congeló"
        )
        if not isinstance(resultado, dict):
            return
        mxn = resultado.get("mxn", {})
        if not isinstance(mxn, dict):
            return

        # Elegimos el precio que corresponde a la condición física real del
        # producto — mismo criterio que ya usa PanelVideojuegos al guardar.
        estado_txt = str(item.get('estado_general', '')).lower()
        if "nuevo" in estado_txt or "sellado" in estado_txt:
            nuevo_valor = float(mxn.get("new", 0) or 0)
        elif "completo" in estado_txt or "cib" in estado_txt:
            nuevo_valor = float(mxn.get("cib", 0) or 0)
        elif "sin librito" in estado_txt or "sin caja" in estado_txt:
            nuevo_valor = float(mxn.get("incompleto", mxn.get("incomplete", 0)) or 0)
        else:
            nuevo_valor = float(mxn.get("loose", 0) or 0)
        if nuevo_valor <= 0:
            nuevo_valor = float(mxn.get("cib", 0) or 0)  # respaldo: CIB es el más común si no hubo match de condición

        if nuevo_valor <= 0:
            return  # PriceCharting no tenía nada útil — no sobreescribimos con basura

        ahora_iso = datetime.now(timezone.utc).isoformat()
        # Actualiza la base para las próximas consultas (fire-and-forget real
        # no aplica aquí — si esto falla, no es grave, solo se reintentará en
        # la próxima pregunta de un cliente sobre este mismo producto).
        await asyncio.wait_for(
            async_db_execute(
                supabase.table('inventario').update({"precio_mercado_referencia": nuevo_valor, "precio_mercado_actualizado_en": ahora_iso})
                .eq('id', item_id).eq('vendedor_id', vendedor_id),
                allow_retry=False
            ),
            timeout=5.0
        )
        # 🆕 Y actualiza el diccionario EN MEMORIA — así esta misma respuesta
        # del bot ya usa el valor fresco, sin esperar a la siguiente consulta.
        item['precio_mercado_referencia'] = nuevo_valor
        item['precio_mercado_actualizado_en'] = ahora_iso
        logger.info(f"💰 [PRICING ENGINE] Refresco justo a tiempo: '{item.get('nombre')}' → ${nuevo_valor:.2f} MXN de mercado.")
    except asyncio.TimeoutError:
        logger.warning(f"⏱️ [PRICING ENGINE] Timeout refrescando valor de mercado para '{item.get('nombre', '?')}' — se sigue con el valor guardado.")
    except Exception as e:
        logger.warning(f"⚠️ [PRICING ENGINE] No se pudo refrescar valor de mercado para '{item.get('nombre', '?')}' (no crítico): {e}")



# ==============================================================================
# 📦 SCRAPER RAWG Y GESTIÓN DE STORAGE
# ==============================================================================
async def _descargar_procesar_subir_portada(nombre_juego: str, url_imagen: str) -> Optional[str]:
    """
    Lógica común de descarga/validación/optimización/subida — blindaje total
    (SSRF, RAM limit, Zip Bomb). Solo sube la imagen al bucket 'portadas' y
    devuelve la URL pública; NO toca ninguna tabla — eso lo decide quien
    llama a esta función (catalogo_maestro compartido, o inventario de un
    vendedor específico).
    """
    if not url_imagen or not es_dominio_seguro(url_imagen):
        logger.error(f"❌ [SSRF BLOCK] Intento de descarga bloqueado. URL sospechosa o no autorizada: {url_imagen}")
        return None

    try:
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
            return supabase.storage.from_("portadas").upload(
                nombre_archivo, out_bytes, file_options={"content-type": "image/jpeg", "upsert": "true"}
            )

        await asyncio.wait_for(asyncio.to_thread(_upload), timeout=15.0)
        return supabase.storage.from_("portadas").get_public_url(nombre_archivo)

    except Exception as e:
        logger.exception(f"❌ [STORAGE ERROR] Fallo descargando/procesando portada para {nombre_juego}: {e}")
        return None


async def contribuir_catalogo_maestro_en_segundo_plano(vendedor_id: str, nombre: str, consola: str, genero: str = None, url_portada: str = None) -> None:
    """
    🆕 Cuando un vendedor agrega un producto que el catálogo maestro
    COMPARTIDO todavía no conoce (ej. un juego nuevo), o le pone portada a
    algo que nadie había guardado antes, se contribuye automáticamente —
    así ese conocimiento sobrevive aunque el inventario de ESTE vendedor se
    borre después (ej. con un reset de fábrica), y beneficia a cualquier
    otro vendedor que después tenga el mismo producto.

    Solo aplica a negocios de videojuegos — catalogo_maestro tiene columnas
    específicas de ese giro (consola, rareza, etc.) que no tendrían sentido
    para otro tipo de negocio. Corre en segundo plano (fire-and-forget),
    nunca afecta la velocidad de guardar el producto ni puede hacer fallar
    ese guardado si algo aquí sale mal.
    """
    try:
        nombre = str(nombre or "").strip()[:200]
        consola = str(consola or "").strip()[:100]
        if not nombre or not consola:
            return

        res_giro = await async_db_execute(
            supabase.table('configuracion_bot').select('giro').eq('vendedor_id', vendedor_id).limit(1),
            timeout_seg=5.0
        )
        giro = str((res_giro.data[0].get('giro') if res_giro.data else '') or '').lower()
        if 'videojueg' not in giro:
            return

        genero = str(genero or "").strip()[:100] or None
        url_portada = str(url_portada or "").strip()[:500] or None

        existente = await async_db_execute(
            supabase.table('catalogo_maestro').select('id, url_portada_oficial, genero')
            .eq('nombre', nombre).eq('consola', consola).limit(1),
            timeout_seg=5.0
        )

        if existente.data:
            fila = existente.data[0]
            actualizar = {}
            if url_portada and not fila.get('url_portada_oficial'):
                actualizar['url_portada_oficial'] = url_portada
            if genero and not fila.get('genero'):
                actualizar['genero'] = genero
            if actualizar:
                await async_db_execute(
                    supabase.table('catalogo_maestro').update(actualizar).eq('id', fila['id']),
                    timeout_seg=8.0, allow_retry=False
                )
                logger.info(f"📚 [CATALOGO MAESTRO] Completado '{nombre}' ({consola}) con datos de {vendedor_id}.")
        else:
            payload = {"nombre": nombre, "consola": consola}
            if genero: payload["genero"] = genero
            if url_portada: payload["url_portada_oficial"] = url_portada
            await async_db_execute(
                supabase.table('catalogo_maestro').insert(payload),
                timeout_seg=8.0, allow_retry=False
            )
            logger.info(f"📚 [CATALOGO MAESTRO] Nuevo producto contribuido: '{nombre}' ({consola}) por {vendedor_id}.")

    except Exception as e:
        # 🛡️ No-crítico por diseño: el inventario del vendedor ya se guardó
        # correctamente antes de que esto corra. Un fallo aquí nunca debe
        # verse del otro lado, ni afectar su propio guardado.
        logger.warning(f"⚠️ [CATALOGO MAESTRO] No se pudo contribuir '{nombre}' al catálogo compartido (no crítico): {e}")


async def procesar_imagen_juego(id_juego: str, nombre_juego: str, url_imagen: str) -> Optional[str]:
    """
    Descarga, optimiza y almacena la portada en el catálogo maestro
    COMPARTIDO (no es de un vendedor específico). Se mantiene tal cual
    funcionaba antes — nada de lo de abajo la modifica.
    """
    url_publica = await _descargar_procesar_subir_portada(nombre_juego, url_imagen)
    if not url_publica:
        return None
    try:
        int_id = int(id_juego)
        res_update = await async_db_execute(
            supabase.table('catalogo_maestro').update({"url_portada_oficial": url_publica}).eq("id", int_id),
            timeout_seg=10.0
        )
        if not res_update or not res_update.data:
            logger.error(f"❌ [STORAGE ERROR] Actualización fallida. El registro no existe en catalogo_maestro (ID: {int_id})")
            return None
        logger.info(f"🖼️ [STORAGE SUCCESS] Imagen optimizada y guardada para: {nombre_juego} -> URL: {url_publica}")
        return url_publica
    except Exception as e:
        logger.exception(f"❌ [STORAGE ERROR] Fallo guardando portada en catalogo_maestro para {nombre_juego}: {e}")
        return None


async def completar_portada_inventario(vendedor_id: str, item_id, nombre_juego: str, url_imagen: str) -> Optional[str]:
    """
    🆕 RECONEXIÓN DE LA PIEZA QUE FALTABA: misma descarga/proceso/subida que
    procesar_imagen_juego, pero actualiza el INVENTARIO REAL de un vendedor
    específico (inventario.url_portada) — esto es lo que de verdad usa el
    bot al decidir si manda foto en su respuesta. catalogo_maestro es un
    catálogo de referencia compartido entre todos los vendedores; esto es
    el stock real de UNO de ellos.
    """
    url_publica = await _descargar_procesar_subir_portada(nombre_juego, url_imagen)
    if not url_publica:
        return None
    try:
        res_update = await async_db_execute(
            supabase.table('inventario').update({"url_portada": url_publica}).eq("id", item_id).eq("vendedor_id", vendedor_id),
            timeout_seg=10.0
        )
        if not res_update or not res_update.data:
            logger.error(f"❌ [STORAGE ERROR] No se pudo guardar portada en inventario (id={item_id}, vendedor={vendedor_id})")
            return None
        logger.info(f"🖼️ [STORAGE SUCCESS] Portada guardada en inventario para '{nombre_juego}' (vendedor={vendedor_id}) -> {url_publica}")
        return url_publica
    except Exception as e:
        logger.exception(f"❌ [STORAGE ERROR] Fallo guardando portada en inventario para {nombre_juego}: {e}")
        return None


async def intentar_completar_portada_en_segundo_plano(vendedor_id: str, item_id, nombre_juego: str) -> None:
    """
    🆕 RECONEXIÓN DE LA FUNCIÓN QUE EXISTÍA EN EL MONOLITO: cuando un
    cliente pregunta por algo que todavía no tiene portada, se busca en RAWG
    y se guarda EN SEGUNDO PLANO (asyncio.create_task — fire-and-forget) —
    nunca bloquea ni retrasa la respuesta que el cliente YA recibió. El
    cliente actual no ve la foto en su mensaje; el SIGUIENTE que pregunte
    por lo mismo, sí.
    """
    if not RAWG_API_KEY or not item_id or not nombre_juego:
        return
    try:
        termino = limpiar_texto(nombre_juego)[:150]
        if len(termino) < 2:
            return
        params = {"key": RAWG_API_KEY, "search": termino, "page_size": 5}
        resp = await asyncio.wait_for(http_client_global.get("https://api.rawg.io/api/games", params=params), timeout=10.0)
        if resp.status_code != 200:
            logger.info(f"🖼️ [PORTADA BG] RAWG respondió {resp.status_code} para '{nombre_juego}' — se omite.")
            return
        resultados = resp.json().get("results", [])
        if not resultados:
            logger.info(f"🖼️ [PORTADA BG] RAWG no encontró nada para '{nombre_juego}'.")
            return
        url_imagen = str(resultados[0].get("background_image", "") or "")
        if not url_imagen:
            return
        url_publica = await completar_portada_inventario(vendedor_id, item_id, nombre_juego, url_imagen)
        if url_publica:
            logger.info(f"🖼️ [PORTADA BG] Lista para el próximo cliente: '{nombre_juego}' (vendedor={vendedor_id}).")
    except Exception as e:
        logger.warning(f"⚠️ [PORTADA BG] No se pudo completar portada en segundo plano para '{nombre_juego}': {e}")
