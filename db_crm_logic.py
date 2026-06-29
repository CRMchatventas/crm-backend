# ==============================================================================
# 🚀 MÓDULO: db_crm_logic.py (AAA ENTERPRISE - GOLD STANDARD FINAL v2.8)
# ==============================================================================
# Godot 4.6 Ready • Motor de Estados CRM, Persistencia IA y Watchdog Remarketing
# ==============================================================================

import asyncio
import hashlib
import orjson
import bleach
import re
import os
from datetime import datetime, timedelta, timezone
from cachetools import TTLCache

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE (EXPLÍCITAS)
# ==========================================================
import config_and_schemas as config

from db_core_wrapper import async_db_execute, supabase
from db_rag_scraper import obtener_contexto_inventario_rag
from db_chat import guardar_mensaje_chat

# Rompemos la dependencia circular importando directamente de los núcleos aislados.
from ai_gemini_core import generar_oferta_inteligente
from ai_whatsapp_media import disparar_whatsapp_dinamico_async

logger = config.logger

# --- MÉTRICAS DE OBSERVABILIDAD AVANZADA ---
METRICAS_CRM = {
    "crm_updates_total": 0,
    "crm_updates_failed": 0,
    "watchdog_cycles_total": 0,
    "remarketing_envios_exito": 0,
    "remarketing_envios_fallidos": 0,
    "last_watchdog_run": 0.0
}

# ==============================================================================
# 🧠 ACTUALIZACIÓN CENTRAL CRM
# ==============================================================================
async def actualizar_estado_crm(
    telefono: str,
    vendedor_id: str,
    columna: str,
    iluminacion: str,
    juego: str,
    perfil_ia: dict = None,
    nombre: str = None,
    # 🛡️ FIX REAL (encontrado al diagnosticar la iluminación que se quedaba
    # pegada en PC y en Mobile): esta función nunca actualizaba
    # 'ultimo_msj' — el campo que la PC y Mobile usan para decidir "¿esto
    # ya lo viste, o es genuinamente nuevo?". Como nunca cambiaba, esa
    # comparación siempre comparaba un valor viejo contra sí mismo, así
    # que el aviso de "nuevo mensaje" prácticamente nunca se disparaba de
    # verdad. Es opcional (default "") para no afectar a quien llama a
    # esta función sin tener un mensaje de por medio (ej. mover una
    # tarjeta manualmente desde el tablero).
    mensaje: str = ""
) -> bool:
    """
    ==============================================================================
    🚀 MOTOR CRM AAA ENTERPRISE
    ==============================================================================
    - Protección concurrente mediante Locks centralizados y autolimpiables.
    - Evita Race Conditions garantizando exclusión mutua por conversación.
    - Sanitización de entradas e inyección JSONB controlada.
    - FIX UPSERT: si no existe fila previa (cliente nuevo, o fila borrada
      manualmente), se crea en vez de perderse silenciosamente.
    ==============================================================================
    """
    inicio_telemetria = config.now_ts()
    METRICAS_CRM["crm_updates_total"] += 1

    try:
        # ==========================================================
        # 🛡️ 1. SANITIZACIÓN INPUTS
        # ==========================================================
        telefono = str(telefono).strip()
        vendedor_id = str(vendedor_id).strip()

        columna = config.limpiar_texto(str(columna))[:50]
        iluminacion = config.limpiar_texto(str(iluminacion))[:50]
        
        juego = bleach.clean(str(juego), tags=[], strip=True)
        juego = config.limpiar_texto(juego)[:100]

        if not telefono or not vendedor_id:
            logger.error("❌ [CRM UPDATE] Parámetros incompletos. Se cancela actualización.")
            METRICAS_CRM["crm_updates_failed"] += 1
            return False

        # ==========================================================
        # 🛡️ 2. ANTI WRITE FLOOD (Uso de TTLCache Seguro del Core)
        # ==========================================================
        flood_key = hashlib.sha256(
            f"CRM_FLD:{telefono}:{vendedor_id}:{columna}:{iluminacion}:{juego}".encode()
        ).hexdigest()

        # FIX FASE 2: Acceso Thread-Safe al Caché Global
        async with config.global_cache_lock:
            if flood_key in config.procesados_recientemente:
                logger.info(f"♻️ [CRM SKIP] Update duplicado evitado transaccionalmente para {telefono[:6]}***")
                return True
            config.procesados_recientemente[flood_key] = config.now_ts()

        # ==========================================================
        # 🛡️ 3. VALIDACIÓN PERFIL IA (JSONB)
        # ==========================================================
        perfil_sanitizado = None

        if perfil_ia:
            try:
                if not isinstance(perfil_ia, dict):
                    raise Exception("perfil_ia inválido. Debe ser diccionario.")

                perfil_sanitizado = {}
                for key, value in perfil_ia.items():
                    key_limpia = config.limpiar_texto(bleach.clean(str(key), tags=[], strip=True))[:80]

                    if isinstance(value, str):
                        perfil_sanitizado[key_limpia] = config.limpiar_texto(
                            bleach.clean(value, tags=[], strip=True)
                        )[:500]
                    elif isinstance(value, (int, float, bool)):
                        perfil_sanitizado[key_limpia] = value
                    elif isinstance(value, list):
                        perfil_sanitizado[key_limpia] = [
                            config.limpiar_texto(str(v))[:120] for v in value[:20]
                        ]

                # 🛡️ LIMITADOR JSONB
                perfil_serializado = orjson.dumps(perfil_sanitizado)
                if len(perfil_serializado) > 12000:
                    logger.error("🚨 [CRM PROFILE LIMIT] Perfil IA truncado por exceder límites de seguridad en JSONB.")
                    perfil_sanitizado = {"estado": "perfil_truncado"}

            except Exception as perfil_e:
                logger.exception(f"❌ [CRM PROFILE ERROR] Fallo al sanitizar perfil: {perfil_e}")
                perfil_sanitizado = {"estado": "perfil_error"}

        # ==========================================================
        # 🛡️ 4. PAYLOAD HARDENED
        # ==========================================================
        payload = {
            "fila": columna,
            "estado_iluminacion": iluminacion,
            "ultimo_producto_interes": juego,
            "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
        }
        # 🛡️ Solo se toca 'ultimo_msj' si esta llamada de verdad trae un
        # mensaje — así una acción manual (ej. mover una tarjeta a mano)
        # nunca borra por accidente el último mensaje real de la conversación.
        mensaje_limpio_crm = config.limpiar_texto(bleach.clean(str(mensaje or ""), tags=[], strip=True))[:2000]
        if mensaje_limpio_crm:
            payload["ultimo_msj"] = mensaje_limpio_crm

        if perfil_sanitizado:
            payload["perfil_psicologico"] = perfil_sanitizado

        # ==========================================================
        # 🛡️ 5. LOCK POR CLIENTE AUTOMÁTICO (Conectado al Core)
        # ==========================================================
        lock_key = hashlib.sha256(f"{telefono}:{vendedor_id}".encode()).hexdigest()
        lock_crm = await config.get_lock(f"crm_lock:{lock_key}")
        
        async with lock_crm:
            # ==========================================================
            # 🛡️ 6. UPDATE CONTROLADO (Sin reintentos)
            # ==========================================================
            # FIX FASE 5: allow_retry=False para evitar colisiones si Supabase lanza timeout de Red
            resultado = await async_db_execute(
                supabase.table("prospectos").update(payload).eq("telefono", telefono).eq("vendedor_id", vendedor_id),
                timeout_seg=10.0,
                allow_retry=False
            )

            # ==========================================================
            # 🆕 6B. FIX UPSERT: si el UPDATE no tocó ninguna fila (cliente
            # nuevo de verdad, o alguien borró la fila manualmente desde
            # Supabase), el cliente se perdía para siempre del tablero
            # aunque el bot le sigue respondiendo por WhatsApp. Si no hubo
            # match, insertamos la fila en vez de fallar en silencio.
            # ==========================================================
            if not getattr(resultado, "data", None):
                payload_insert = dict(payload)
                payload_insert["telefono"] = telefono
                payload_insert["vendedor_id"] = vendedor_id
                payload_insert["nombre"] = config.limpiar_texto(
                    bleach.clean(str(nombre or "Cliente Nuevo"), tags=[], strip=True)
                )[:120]

                await async_db_execute(
                    supabase.table("prospectos").insert(payload_insert),
                    timeout_seg=10.0,
                    allow_retry=False
                )
                logger.info(
                    f"🆕 [CRM INSERT] No existía fila previa para Tel={telefono[:6]}*** — "
                    f"prospecto nuevo creado en '{columna}'."
                )

        # ==========================================================
        # 📊 7. TELEMETRÍA
        # ==========================================================
        tiempo_total = config.now_ts() - inicio_telemetria
        logger.info(
            f"💾 [CRM UPDATE SUCCESS] Tel={telefono[:6]}*** | "
            f"Fila={columna} | Tiempo={tiempo_total:.3f}s"
        )
        return True

    except Exception as e:
        logger.exception(f"❌ [CRM UPDATE ERROR FATAL] {str(e)}")
        METRICAS_CRM["crm_updates_failed"] += 1
        return False


# ==============================================================================
# 🧠 GUARDADO DE RESULTADO IA EN CRM
# ==============================================================================
async def guardar_resultado_ia_en_crm(
    telefono: str,
    vendedor_id: str,
    data: dict
) -> bool:
    """
    ==============================================================================
    💾 PERSISTENCIA AVANZADA DE RESULTADOS IA
    ==============================================================================
    - Sanitización profunda de estrategias y copys.
    - Validación semántica estricta de floats.
    ==============================================================================
    """
    try:
        telefono = str(telefono).strip()
        vendedor_id = str(vendedor_id).strip()

        if not telefono or not vendedor_id:
            logger.error("❌ [CRM IA SAVE] Datos inválidos (Falta teléfono o tenant).")
            return False

        # ==========================================================
        # 🛡️ SANITIZACIÓN DEL PAYLOAD DE IA
        # ==========================================================
        payload = {
            "lead_score": int(max(0, min(100, int(data.get("lead_score", 0))))),
            "probabilidad_cierre": float(max(0.0, min(100.0, float(data.get("probabilidad_cierre", 0.0))))),
            "estrategia_venta": config.limpiar_texto(bleach.clean(str(data.get("estrategia_venta", "")), tags=[], strip=True))[:500],
            "requiere_seguimiento": bool(data.get("requiere_seguimiento", False)),
            "sugerir_veltrix": bool(data.get("sugerir_veltrix", False)),
            "tipo_seguimiento": config.limpiar_texto(bleach.clean(str(data.get("tipo_seguimiento", "")), tags=[], strip=True))[:100],
            "cross_selling": config.limpiar_texto(bleach.clean(str(data.get("cross_selling", "")), tags=[], strip=True))[:300],
            "upselling": config.limpiar_texto(bleach.clean(str(data.get("upselling", "")), tags=[], strip=True))[:300],
            "nivel_prioridad": config.limpiar_texto(bleach.clean(str(data.get("nivel_prioridad", "")), tags=[], strip=True))[:50],
            "ultimo_msj": config.limpiar_texto(bleach.clean(str(data.get("respuesta", "")), tags=[], strip=True))[:2000],
            "ultima_interaccion_ia": datetime.now(timezone.utc).isoformat()
        }

        # ==========================================================
        # 🛡️ UPDATE CONTROLADO EN BBDD (Con exclusión mutua)
        # ==========================================================
        lock_key = hashlib.sha256(f"{telefono}:{vendedor_id}".encode()).hexdigest()
        lock_crm = await config.get_lock(f"crm_lock:{lock_key}")
        
        async with lock_crm:
            # FIX FASE 5: allow_retry=False anclado
            await async_db_execute(
                supabase.table("prospectos").update(payload).eq("telefono", telefono).eq("vendedor_id", vendedor_id),
                timeout_seg=10.0,
                allow_retry=False
            )

        logger.info(f"💾 [CRM SYNC SUCCESS] {telefono[:6]}*** | Score={payload.get('lead_score')}")
        return True

    except Exception as e:
        logger.exception(f"❌ [CRM SYNC ERROR FATAL] {str(e)}")
        return False


# ==============================================================================
# ⏰ WATCHDOG: BUCLE DE SEGUIMIENTO Y REMARKETING 24H
# ==============================================================================
async def bucle_seguimiento_24h():
    """
    ==============================================================================
    ⏰ MOTOR AUTÓNOMO DE REMARKETING (WATCHDOG BG TASK)
    ==============================================================================
    Implementación transaccional distribuida. Extrae los leads de la BBDD
    mediante un RPC atómico (claim_remarketing_leads) que evita dobles envíos.
    ==============================================================================
    """
    logger.info("⏰ [WATCHDOG] Iniciando motor de Remarketing autónomo 24H...")
    
    # TTLCache para evitar configuraciones fantasma o fugas de RAM
    cache_config = TTLCache(maxsize=1000, ttl=600) 

    while True:
        METRICAS_CRM["watchdog_cycles_total"] += 1
        METRICAS_CRM["last_watchdog_run"] = config.now_ts()
        try:
            ahora_utc = datetime.now(timezone.utc)
            hace_24h = (ahora_utc - timedelta(hours=24)).isoformat()
            
            # 🛡️ FIX FASE 7 (CRÍTICO): RPC Atómico en lugar de .select()
            # Esta función de Postgres debe hacer un SELECT FOR UPDATE SKIP LOCKED
            # y actualizar el remarketing_count internamente antes de devolver las filas.
            logger.info("🔍 [WATCHDOG] Reclamando leads para remarketing atómicamente...")
            res = await async_db_execute(
                supabase.rpc('claim_remarketing_leads', {
                    'p_horas_inactividad': 24, 
                    'p_limite': 20
                }),
                allow_retry=False
            )
            
            if res.data:
                logger.info(f"🎯 [WATCHDOG] Reclamados {len(res.data)} prospectos exclusivos para este Worker.")
                
                for p in res.data:
                    vendedor_id = str(p.get('vendedor_id', 'V-001'))
                    telefono_lead = p.get('telefono')
                    
                    # 🚀 CACHÉ DE CONFIGURACIÓN DEL TENANT AUTOMÁTICA
                    config_bot = cache_config.get(vendedor_id)
                    if not config_bot:
                        res_conf = await async_db_execute(
                            supabase.table('configuracion_bot').select('*').eq('vendedor_id', vendedor_id).limit(1)
                        )
                        if res_conf.data:
                            config_bot = res_conf.data[0]
                            cache_config[vendedor_id] = config_bot
                    
                    if not config_bot: 
                        continue
                    
                    try:
                        # 🚀 ORQUESTACIÓN DE IA PARA REMARKETING
                        producto_interes = p.get('ultimo_producto_interes') or p.get('ultimo_juego_interes', '')
                        
                        # 1. Obtención de contexto RAG
                        contexto_inv = await obtener_contexto_inventario_rag(vendedor_id, producto_interes)
                        
                        # 2. Generación de oferta
                        oferta = await generar_oferta_inteligente(
                            p.get('nombre', 'Cliente'), 
                            producto_interes if producto_interes else 'nuestros productos', 
                            contexto_inv
                        )
                        
                        if oferta and oferta.get("mensaje_oferta"):
                            mensaje = oferta.get("mensaje_oferta")
                            
                            # 3. Disparo asíncrono a Meta WhatsApp
                            fallback_token = os.getenv("WHATSAPP_TOKEN", "")
                            fallback_phone_id = os.getenv("WHATSAPP_PHONE_ID", "")

                            exito = await disparar_whatsapp_dinamico_async(
                                telefono_lead, 
                                mensaje, 
                                config_bot.get('meta_token', fallback_token), 
                                config_bot.get('meta_phone_id', fallback_phone_id)
                            )
                            
                            if exito:
                                # 4. Actualizaciones seguras y sincronizadas
                                await actualizar_estado_crm(telefono_lead, vendedor_id, 'Con Descuento', 'oro', producto_interes, mensaje=mensaje)
                                await guardar_mensaje_chat(telefono_lead, vendedor_id, 'BOT_REMARKETING', mensaje)
                                
                                METRICAS_CRM["remarketing_envios_exito"] += 1
                                logger.info(f"🎯 [REMARKETING] Oferta exitosa a {p.get('nombre')} ({telefono_lead[:6]}***)")
                                
                                # Rate Limiting pasivo estructural
                                await asyncio.sleep(5) 
                            else:
                                METRICAS_CRM["remarketing_envios_fallidos"] += 1
                                
                    except Exception as e:
                        METRICAS_CRM["remarketing_envios_fallidos"] += 1
                        if "429" in str(e) or "quota" in str(e).lower(): 
                            logger.error("⚠️ [API LIMIT] Pausa de seguridad por límite de IA de Google.")
                            await asyncio.sleep(60)
                            break 
                        else:
                            logger.exception(f"❌ [REMARKETING ERROR] Fallo con lead {telefono_lead[:6]}***: {e}")
                            
        except Exception as e: 
            logger.exception(f"❌ [WATCHDOG FATAL] Error general en ciclo, reiniciando en 10 min: {e}")
            
        logger.info(f"📊 [WATCHDOG HEALTH] Estado actual: {METRICAS_CRM}")
        await asyncio.sleep(600)
