# ==============================================================================
# 🚀 MÓDULO: db_chat.py (AAA ENTERPRISE - GOLD STANDARD)
# ==============================================================================
import asyncio
import re
import hashlib
import bleach

from config_and_schemas import (
    logger, supabase, get_lock,
    cache_respuestas_ia, CHAT_MESSAGE_HASHES,
    now_ts, limpiar_texto
)
# 🔧 FIX: normalizar_telefono ya no existe en config_and_schemas.py (fue
# renombrada internamente a _local_validar_tel, que lanza ValueError en vez
# de devolver ""). Este archivo espera el contrato "devuelve '' si falla",
# así que se importa la versión robusta y compatible de ai_security_utils.
from ai_security_utils import normalizar_telefono
from db_core_wrapper import async_db_execute

async def obtener_historial_chat(telefono: str, vendedor_id: str, limite: int = 12) -> str:
    """
    Recupera historial sanitizado, cacheado y limitado.
    Patrón: Double-checked locking para evitar Stampede.
    """
    limite = max(1, min(limite, 25))
    tel_norm = normalizar_telefono(telefono)
    
    # 🚀 FIX AUDITORÍA: Promovemos a error estructurado un input inválido en el pipeline.
    if not tel_norm:
        logger.error("❌ [HISTORIAL CHAT] Parámetro 'telefono' inválido o nulo. Se cancela recuperación.")
        return "Cliente inválido."
        
    vendedor_id = str(vendedor_id).strip()
    cache_key = hashlib.sha256(f"HIST:{tel_norm}:{vendedor_id}".encode()).hexdigest()

    # 1. Lectura rápida (Fast-Path)
    cache_item = cache_respuestas_ia.get(cache_key)
    if cache_item and (now_ts() - cache_item.get("ts", 0) <= 15):
        return cache_item["data"]

    # 2. Protección Stampede (Lock granular por caché)
    async with await get_lock(f"hist_stampede_lock:{cache_key}"):
        # Doble verificación
        cache_item = cache_respuestas_ia.get(cache_key)
        if cache_item and (now_ts() - cache_item.get("ts", 0) <= 15):
            return cache_item["data"]

        try:
            query = (
                supabase.table('mensajes_chat')
                .select('autor, mensaje')
                .eq('telefono', tel_norm)
                .eq('vendedor_id', vendedor_id)
                .order('created_at', desc=True)
                .limit(limite)
            )
            res_hist = await asyncio.wait_for(async_db_execute(query), timeout=8.0)

            # 🛡️ Caché de estado "Cliente Nuevo"
            if not res_hist.data:
                res_final = "Primer mensaje registrado del cliente."
            else:
                mensajes_ordenados = list(reversed(res_hist.data))
                lineas = []
                for m in mensajes_ordenados:
                    autor = limpiar_texto(str(m.get("autor", "USER"))).upper()[:15]
                    mensaje = limpiar_texto(bleach.clean(str(m.get("mensaje", "")), tags=[], strip=True))
                    
                    # Anti-Prompt Injection
                    mensaje = re.sub(r"(system prompt|developer mode|ignore instructions|eres chatgpt)", "[FILTRADO]", mensaje, flags=re.IGNORECASE)
                    mensaje = mensaje[:350]

                    if mensaje.strip():
                        lineas.append(f"{autor}: {mensaje}")
                
                res_final = "\n".join(lineas) if lineas else "No hay suficiente historial disponible."
                
                if len(res_final) > 2500:
                    res_final = "... [HISTORIAL COMPRIMIDO] ...\n" + res_final[-2500:]

            cache_respuestas_ia[cache_key] = {"data": res_final, "ts": now_ts()}
            return res_final

        except asyncio.TimeoutError:
            logger.error("❌ [HISTORIAL CHAT] Timeout excedido (8s) al consultar Supabase.")
            return "El historial está tardando demasiado en cargar."
        except Exception as e:
            logger.exception(f"❌ [HISTORIAL ERROR] Falla estructural en recuperación: {e}")
            return "No se pudo recuperar el historial."

async def guardar_mensaje_chat(telefono: str, vendedor_id: str, autor: str, mensaje: str, wamid: str = "") -> bool:
    """Persistencia segura con deduplicación atómica."""
    try:
        tel_norm = normalizar_telefono(telefono)
        vendedor_id = str(vendedor_id).strip()
        wamid_sanitizado = str(wamid).strip()[:250]
        
        # 🛡️ Normalización estricta de autor
        autor_norm = str(autor).strip().upper()[:15]

        # 🚀 FIX AUDITORÍA: Promovemos a error la falta de datos estructurales.
        if not tel_norm or not vendedor_id: 
            logger.error(f"❌ [CHAT SAVE] Imposible persistir: faltan credenciales (Tel: {tel_norm}, Vendedor: {vendedor_id})")
            return False

        mensaje_limpio = bleach.clean(limpiar_texto(str(mensaje)), tags=[], strip=True)[:5000]

        # 🛡️ Hash inteligente: Prioriza WAMID para unicidad absoluta
        if wamid_sanitizado:
            msg_hash = hashlib.sha256(wamid_sanitizado.encode()).hexdigest()
        else:
            msg_hash = hashlib.sha256(f"{tel_norm}:{autor_norm}:{mensaje_limpio}".encode()).hexdigest()

        # 🔒 Lock de escritura (Granular)
        async with await get_lock(f"chat_persist:{vendedor_id}:{tel_norm}"):
            if msg_hash in CHAT_MESSAGE_HASHES:
                logger.info(f"♻️ [CHAT DEDUPE] Mensaje duplicado interceptado para el hash {msg_hash[:8]}...")
                return True
            
            # 💾 Persistencia DB (Operación crítica antes de marcar el hash)
            await async_db_execute(
                supabase.table("mensajes_chat").insert({
                    "telefono": tel_norm,
                    "vendedor_id": vendedor_id,
                    "autor": autor_norm,
                    "mensaje": mensaje_limpio,
                    "wamid": wamid_sanitizado
                }),
                timeout_seg=8.0
            )

            CHAT_MESSAGE_HASHES[msg_hash] = True

        # 🧹 Invalidación de Caché (Lock para evitar condiciones de carrera)
        cache_key_hist = hashlib.sha256(f"HIST:{tel_norm}:{vendedor_id}".encode()).hexdigest()
        async with await get_lock(f"hist_stampede_lock:{cache_key_hist}"):
            cache_respuestas_ia.pop(cache_key_hist, None)

        return True
    except Exception as e:
        logger.exception(f"❌ [CHAT SAVE ERROR] Falla estructural en persistencia: {e}")
        return False
