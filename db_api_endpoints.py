# ==============================================================================
# 🚀 MÓDULO: db_api_endpoints.py (AAA ENTERPRISE GOLD STANDARD - COMPLETAMENTE CORREGIDO v2.8)
# ==============================================================================
# Godot 4.6 Ready • Orquestador IA, Auth B2B, CRM Base, Móvil e Inventario
# ==============================================================================

import asyncio, re, time, uuid, bleach, hashlib, jwt
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any
from cachetools import TTLCache

from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks, Header
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

# ==========================================================
# 🔌 IMPORTACIONES ESTRUCTURADAS (SSOT)
# ==========================================================
from config_and_schemas import (
    logger, get_lock, mensajes_procesados_meta, procesados_recientemente,
    JWT_SECRET, DUMMY_HASH, pwd_context, LoginUpdate, LeadAction, EstadoUpdate, 
    BorrarRequest, ColumnaUpdate, NotasUpdate, NuevoArticulo, VentaItem, 
    MobileMessageRequest, sanitizar_nombre_columna, ReordenarColumnasAction
)

from ai_security_utils import verificar_sesion_b2b
from db_core_wrapper import async_db_execute, supabase
from db_chat import guardar_mensaje_chat, obtener_historial_chat
from db_crm_logic import actualizar_estado_crm

# CONSTANTE DE CRM GLOBAL
FILA_PAPELERA = "Papelera"

# 🔧 FIX ESTRUCTURA: bloques fijos compartidos. Antes vivían como listas
# locales duplicadas dentro de cargar_todo; ahora son la única fuente de
# verdad, usada también por /api/reordenar_columnas para validar que estos
# bloques nunca lleguen alterados desde el cliente.
COLUMNAS_FIJAS_IZQ = ["Bandeja Nueva", "Envios Masivos", "Con Descuento", "Requiere Asistencia"]
COLUMNAS_FIJAS_DER = ["Por Entregar", "Vendidos", FILA_PAPELERA]

# ==========================================================
# 🛡️ HELPERS AAA, SCHEMAS Y CACHÉ
# ==========================================================
ventas_idempotencia_lock = asyncio.Lock()

# TTLCaches explícitos para evitar Memory Leaks y Bloqueos Infinitos
ventas_procesadas_idempotencia = TTLCache(maxsize=10000, ttl=86400)      # 24 Horas
LOGIN_RATE_LIMIT = TTLCache(maxsize=100000, ttl=300)                     # 5 Minutos
RATE_LIMIT_MOBILE_OUTBOUND = TTLCache(maxsize=100000, ttl=60)            # 1 Minuto

class CampanaMasivaRequest(BaseModel):
    columna_origen: str
    mensaje: str

def now_ts() -> float: return time.time()
def limpiar_texto(texto: str) -> str: return bleach.clean(str(texto), tags=[], strip=True).strip()

def normalizar_telefono(tel: Any) -> str:
    if not tel: return ""
    limpio = re.sub(r"\D", "", str(tel))
    return limpio if len(limpio) >= 10 else ""

def _telefono_canonico_dedup(tel: Any) -> str:
    """Solo para agrupar/deduplicar prospectos duplicados por formato de
    teléfono. Colapsa variantes mexicanas con/sin el '1' extra que WhatsApp
    inserta tras el código de país 52 (ej. 524491142598 y 5214491142598
    deben tratarse como el mismo cliente). No usar para escribir en BD."""
    limpio = re.sub(r"\D", "", str(tel or ""))
    if len(limpio) < 10:
        return ""
    if limpio.startswith("52") and not limpio.startswith("521") and len(limpio) == 12:
        limpio = "521" + limpio[2:]
    return limpio

def enmascarar_telefono(tel: str) -> str:
    if not tel or len(tel) < 6: return "unknown"
    return f"{tel[:4]}******{tel[-2:]}"

def obtener_trace_id(x_trace_id: Optional[str] = Header(None)) -> str:
    if x_trace_id:
        limpio = re.sub(r'[^a-zA-Z0-9_-]', '', str(x_trace_id))
        if limpio: return limpio[:64]
    return uuid.uuid4().hex[:16]

async def migrar_password_usuario(user_id: str, nuevo_hash: str):
    # FIX FASE 1: allow_retry=False por ser operación de mutación crítica (UPDATE)
    await async_db_execute(
        supabase.table('usuarios_veltrix').update({"password": nuevo_hash}).eq('id', user_id),
        allow_retry=False
    )

router = APIRouter()

# ==========================================================
# 🤖 0. ORQUESTADOR MAESTRO IA (FLATTENED & HARDENED)
# ==========================================================
async def procesar_respuesta_bot(cliente: str, telefono: str, texto_entrante: str, columna_actual: str, config: dict, media_dict: dict = None, id_mensaje_meta: str = None):
    from ai_gemini_core import analizar_intencion_venta_ia, validar_respuesta_ia, generar_resumen_handoff_ia
    from ai_security_utils import detectar_prompt_injection
    from db_rag_scraper import obtener_contexto_inventario_rag
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async, disparar_whatsapp_imagen_async, enviar_alerta_whatsapp_admin

    trace_id = obtener_trace_id()
    inicio_pipeline = now_ts()
    
    cliente = limpiar_texto(str(cliente or "Cliente"))[:120]
    telefono = normalizar_telefono(telefono)
    texto_entrante = limpiar_texto(str(texto_entrante or ""))[:12000]
    vendedor_id = str(config.get("vendedor_id", "")).strip()

    if not telefono or not vendedor_id:
        logger.warning(f"⚠️ [TRACE:{trace_id}] Inputs inválidos. Abortando pipeline multi-tenant.")
        return

    logger.info(f"🧠 [TRACE:{trace_id}] Inicio Pipeline IA | Tenant={vendedor_id} | Tel={enmascarar_telefono(telefono)}")

    try:
        if id_mensaje_meta:
            async with await get_lock(f"meta_{id_mensaje_meta}"):
                if id_mensaje_meta in mensajes_procesados_meta: return
                mensajes_procesados_meta[id_mensaje_meta] = True

        lock_hash = hashlib.sha256(f"{vendedor_id}:{telefono}".encode()).hexdigest()
        
        async with await get_lock(lock_hash):
            if detectar_prompt_injection(texto_entrante):
                logger.warning(f"🚨 [TRACE:{trace_id}] Injection detectada y bloqueada en tiempo real.")
                await disparar_whatsapp_dinamico_async(telefono, "Solicitud bloqueada por políticas de seguridad.", config.get("meta_token", ""), config.get("meta_phone_id", ""))
                return

            spam_hash = hashlib.sha256(f"{telefono}:{texto_entrante.lower()}".encode()).hexdigest()
            if spam_hash in procesados_recientemente: return
            procesados_recientemente[spam_hash] = True

            perfil_cliente_previo = {}
            try:
                res_p = await asyncio.wait_for(
                    async_db_execute(supabase.table('prospectos').select('perfil_psicologico').eq('telefono', telefono).eq('vendedor_id', vendedor_id).limit(1)), 
                    timeout=5.0
                )
                if res_p.data: perfil_cliente_previo = res_p.data[0].get('perfil_psicologico', {}) or {}
            except Exception as e:
                logger.warning(f"⚠️ [TRACE:{trace_id}] Error consultando perfil: {e}")

            contexto_task = asyncio.create_task(obtener_contexto_inventario_rag(vendedor_id, texto_entrante))
            historial_task = asyncio.create_task(obtener_historial_chat(telefono, vendedor_id, limite=10))
            
            resultados = await asyncio.gather(contexto_task, historial_task, return_exceptions=True)
            contexto = resultados[0] if not isinstance(resultados[0], Exception) else ""
            historial = resultados[1] if not isinstance(resultados[1], Exception) else []
            
            # 🔧 FIX BUG ARGUMENTOS: faltaba 'telefono' — sin él, todo se recorría
            # un lugar (telefono recibía el dict de perfil, perfil_cliente_previo
            # recibía media_dict, y el media_dict real nunca llegaba — siempre None).
            # Esto rompía la memoria persistente del cliente Y el análisis de
            # imágenes/audios al mismo tiempo.
            #
            # 🔧 FIX TIMEOUT: 25.0s afuera competía contra el reintento interno de
            # consultar_gemini_json (2 intentos, hasta 26s cada uno ≈ 52s en el peor
            # caso) — el de afuera casi siempre ganaba la carrera y mataba el
            # mecanismo de reintentos antes de que corriera ni un solo intento.
            decision = await asyncio.wait_for(
                analizar_intencion_venta_ia(texto_entrante, contexto, historial, config, telefono, perfil_cliente_previo, media_dict),
                timeout=60.0
            )
            decision = validar_respuesta_ia(decision)
            
            respuesta_final = decision.get("respuesta", "Lo siento, tengo intermitencias en este momento.")
            producto_detectado = decision.get("producto_detectado", "")
            intencion_ia = decision.get("intencion", "HUMANO")
            
            perfil_actualizado = {
                **perfil_cliente_previo, 
                "emocion_actual": decision.get("emocion_cliente"), 
                "temperatura": decision.get("temperatura_lead"), 
                "ultimo_interes": producto_detectado, 
                "ultima_intencion": intencion_ia
            }
            
            nueva_columna, iluminacion = columna_actual, "blanco"
            
            if intencion_ia in ["HUMANO", "POSTVENTA", "GARANTIA", "ENOJO"]:
                nueva_columna, iluminacion = "Requiere Asistencia", "verde_alerta"
                resumen = await generar_resumen_handoff_ia(cliente, intencion_ia, historial)
                await enviar_alerta_whatsapp_admin(cliente, telefono, intencion_ia, resumen, config)
            elif intencion_ia == "COMPRA":
                nueva_columna, iluminacion = "Por Entregar", "verde_exito"
                
            resultados_gather = await asyncio.gather(
                actualizar_estado_crm(telefono, vendedor_id, nueva_columna, iluminacion, producto_detectado, perfil_ia=perfil_actualizado),
                guardar_mensaje_chat(telefono, vendedor_id, 'BOT', respuesta_final),
                return_exceptions=True
            )
            
            for r in resultados_gather:
                if isinstance(r, Exception): logger.error(f"❌ [TRACE:{trace_id}] Tarea asíncrona fallida en CRM/Chat: {r}")

            url_imagen = None
            if producto_detectado:
                try:
                    # FIX FASE 4: Se añade ordenamiento por relevancia y se mitiga el comodín masivo
                    res_juego = await async_db_execute(
                        supabase.table('inventario')
                        .select('url_portada')
                        .ilike('nombre', f'%{producto_detectado}%')
                        .eq('vendedor_id', vendedor_id)
                        .order('stock', desc=True)
                        .limit(1)
                    )
                    if res_juego.data and res_juego.data[0].get('url_portada'):
                        url_imagen = res_juego.data[0]['url_portada']
                except Exception as e:
                    logger.warning(f"⚠️ [TRACE:{trace_id}] Fallo al buscar portada: {e}")

            if url_imagen:
                await disparar_whatsapp_imagen_async(telefono, url_imagen, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))
            else:
                await disparar_whatsapp_dinamico_async(telefono, respuesta_final, config.get("meta_token"), config.get("meta_phone_id"))

            logger.info(f"✅ [TRACE:{trace_id}] Pipeline IA completado en {now_ts() - inicio_pipeline:.2f}s")

    except asyncio.TimeoutError:
        logger.error(f"⏱️ [TRACE:{trace_id}] Timeout global del Orquestador IA (Superó 60s).")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] CRÍTICO: Error en el pipeline IA: {e}")

# ==========================================================
# 📊 1. DASHBOARD STATS (TELEMETRÍA DE NEGOCIO VIA RPC)
# ==========================================================
@router.get("/stats")
async def get_dashboard_stats(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📊 [TRACE:{trace_id}] Solicitando Stats B2B (Vía RPC) para {vendedor_id}")
    try:
        # FIX FASE 1: allow_retry=False para ejecuciones RPC complejas
        res_rpc = await asyncio.wait_for(
            async_db_execute(supabase.rpc('get_tenant_stats', {'p_vendedor_id': vendedor_id}), allow_retry=False), 
            timeout=5.0
        )
        stats_data = res_rpc.data[0] if res_rpc.data else {"total_leads": 0, "leads_nuevos": 0, "pendientes": 0}
        return {"status": "success", "data": stats_data}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Timeout en base de datos al calcular stats.")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo crítico al procesar stats vía RPC: {e}")
        raise HTTPException(status_code=500, detail="Error recuperando stats nativos.")

# ==========================================================
# 📋 2. LISTADO DE LEADS
# ==========================================================
@router.get("/leads")
async def get_leads(fila: Optional[str] = None, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📋 [TRACE:{trace_id}] Solicitando Leads. Fila: {fila}")
    try:
        query = supabase.table("prospectos").select("*").eq("vendedor_id", vendedor_id)
        if fila: query = query.eq("fila", fila)
        res = await asyncio.wait_for(async_db_execute(query.order("ultima_interaccion_ia", desc=True).limit(50)), timeout=8.0)
        return {"status": "success", "data": res.data}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error recuperando leads: {e}")
        raise HTTPException(status_code=500, detail="Error recuperando leads.")

# ==========================================================
# ⚙️ 3. ACCIÓN MANUAL (CRM CONTROL)
# ==========================================================
@router.post("/leads/accion")
async def ejecutar_accion_lead(payload: LeadAction, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Acción Manual '{payload.accion}' sobre Lead {payload.lead_id}")
    try:
        res_check = await asyncio.wait_for(
            async_db_execute(supabase.table("prospectos").select("telefono").eq("id", payload.lead_id).eq("vendedor_id", vendedor_id)),
            timeout=5.0
        )
        if not res_check.data: raise HTTPException(status_code=404, detail="Lead no encontrado.")
        if payload.accion == "mover_fila": 
            await actualizar_estado_crm(telefono=res_check.data[0]['telefono'], vendedor_id=vendedor_id, columna=payload.valor, iluminacion="blanco", juego="")
        return {"status": "success", "msg": "Acción ejecutada."}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error ejecutando acción: {e}")
        raise HTTPException(status_code=500, detail="Error ejecutando acción.")

# ==========================================================
# 🔐 9. AUTENTICACIÓN Y LOGIN B2B (AAA ENTERPRISE)
# ==========================================================
@router.post("/api/login")
async def login_b2b(datos: LoginUpdate, request: Request, background_tasks: BackgroundTasks, trace_id: str = Depends(obtener_trace_id)):
    ip_cliente = request.client.host if request.client else "127.0.0.1"
    email_normalizado = datos.email.lower().strip()
    
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email_normalizado):
        raise HTTPException(status_code=401, detail="Credenciales inválidas.")

    llave_limite = f"{ip_cliente}:{email_normalizado}"
    
    async with await get_lock(f"login_lock_{llave_limite}"):
        intentos_previos = LOGIN_RATE_LIMIT.get(llave_limite, 0)
        if intentos_previos >= 5:
            logger.warning(f"🚨 [TRACE:{trace_id}] IP bloqueada temporalmente: {llave_limite}")
            raise HTTPException(status_code=429, detail="Demasiados intentos. Intenta en 5 min.")

    logger.info(f"🔑 [TRACE:{trace_id}] Autenticando B2B: {email_normalizado} desde {ip_cliente}")
    
    try:
        res = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('*').eq('email', email_normalizado).limit(1)),
            timeout=10.0
        )
        
        usuario_existe = bool(res.data)
        usuario = res.data[0] if usuario_existe else {}
        password_guardada = str(usuario.get('password', DUMMY_HASH))
        
        password_valida, es_legacy = False, False

        if usuario_existe:
            if password_guardada.startswith('$2b$'):
                password_valida = await run_in_threadpool(pwd_context.verify, datos.password, password_guardada)
            else:
                es_legacy = True
                password_valida = (datos.password == password_guardada)
        else:
            await run_in_threadpool(pwd_context.verify, datos.password, DUMMY_HASH)

        if not password_valida:
            async with await get_lock(f"login_lock_{llave_limite}"):
                LOGIN_RATE_LIMIT[llave_limite] = LOGIN_RATE_LIMIT.get(llave_limite, 0) + 1
            raise HTTPException(status_code=401, detail="Credenciales inválidas.")

        if es_legacy:
            logger.warning(f"🚨 [TRACE:{trace_id}] Migrando cuenta legacy: {email_normalizado}")
            nuevo_hash = await run_in_threadpool(pwd_context.hash, datos.password)
            background_tasks.add_task(migrar_password_usuario, usuario['id'], nuevo_hash)

        if str(usuario.get('estado', '')).lower().strip() != 'activo':
            raise HTTPException(status_code=401, detail="Cuenta no activa.")

        if not usuario.get('suscripcion_activa', False):
            raise HTTPException(status_code=402, detail="Suscripción inactiva.")

        async with await get_lock(f"login_lock_{llave_limite}"):
            LOGIN_RATE_LIMIT.pop(llave_limite, None)

        # FIX FASE 4: CRÍTICO MULTI-TENANT. Rompemos el fallback por defecto a 'V-001'.
        vendedor_id = usuario.get('vendedor_id')
        if not vendedor_id or not str(vendedor_id).strip():
            logger.critical(f"🚨 [SECURITY ALERT] El usuario {email_normalizado} carece de vendedor_id. Bloqueando acceso instantáneamente.")
            raise HTTPException(status_code=500, detail="Estructura de aislamiento corrupta. Contacte soporte.")
            
        vendedor_id = str(vendedor_id).strip()
        ahora = datetime.now(timezone.utc)
        
        # FIX FASE 3: Modificado algoritmo a HS512 para total paridad con el validador central de seguridad
        token_jwt = jwt.encode({
            "sub": vendedor_id, "email": usuario['email'], "jti": str(uuid.uuid4()), 
            "iss": "veltrix-engine", "aud": "veltrix-clients",
            "iat": ahora, "nbf": ahora, "exp": ahora + timedelta(hours=8)
        }, JWT_SECRET, algorithm="HS512")
        
        logger.info(f"✅ [TRACE:{trace_id}] Tenant [{vendedor_id}] verificado con éxito. Despachando token HS512 hacia Godot 4.6.")

        return {
            "status": "ok", "access_token": token_jwt, "token_type": "bearer",
            "datos": {
                "vendedor_id": vendedor_id, "email": usuario['email'],
                "nombre": usuario.get('nombre_contacto', 'Vendedor'), "rol": usuario.get('rol', 'vendedor')
            }
        }

    except HTTPException: raise
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Servicio temporalmente no disponible.")
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error CRÍTICO en login: {e}")
        raise HTTPException(status_code=500, detail="Error interno de autenticación.")
    
# ==========================================================
# 🌐 10. RUTAS CRM (CARGA Y ACTUALIZACIÓN B2B)
# ==========================================================
@router.get("/api/cargar_todo")
async def cargar_todo(limit: int = 200, offset: int = 0, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Sincronizando Tablero Kanban (Modo Ligero).")
    try:
        offset_seguro, limit_seguro = max(0, offset), min(limit, 300)
        columnas_izq = COLUMNAS_FIJAS_IZQ
        columnas_der = COLUMNAS_FIJAS_DER
        
        # 🔧 FIX ORDEN: se agrega 'orden' al select y se ordena por él, para
        # que un reordenamiento guardado vía /api/reordenar_columnas se
        # refleje aquí al recargar (antes no existía ningún ORDER BY, el
        # orden visible dependía por accidente del id de inserción).
        res_cols = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion').select('nombre_columna, orden').eq('vendedor_id', str(_sesion)).order('orden')),
            timeout=10.0
        )
        
        columnas_custom = [
            sanitizar_nombre_columna(r['nombre_columna']) for r in (res_cols.data or []) 
            if r['nombre_columna'].upper() not in [c.upper() for c in (columnas_izq + columnas_der)]
        ]
        
        res_prospectos = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').select('id, nombre, telefono, fila, ultima_interaccion_ia, ultimo_msj')
                .eq('vendedor_id', str(_sesion)).order('ultima_interaccion_ia', desc=True)
                .range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=12.0
        )
        
        ultimos = {}
        for registro in (res_prospectos.data or []):
            # 🔧 FIX DUPLICADOS: usamos la clave canónica (colapsa variantes
            # mexicanas con/sin el "1") en vez de normalizar_telefono crudo,
            # para que un mismo cliente con dos formatos de teléfono en BD
            # nunca aparezca como dos prospectos peleándose por la misma
            # tarjeta en Godot. Como la consulta ya viene ordenada por
            # actividad más reciente primero, la primera fila que gane esta
            # clave es siempre la más reciente/activa.
            key_identificador = _telefono_canonico_dedup(registro.get('telefono', '')) or str(registro.get('id', ''))
            
            if key_identificador not in ultimos:
                registro["ultimo_msj"] = bleach.clean(str(registro.get("ultimo_msj") or ""), tags=[], strip=True)
                ultimos[key_identificador] = registro
                
        return {"columnas": columnas_izq + columnas_custom + columnas_der, "prospectos": list(ultimos.values())}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en cargar_todo: {e}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar tarjetas del embudo.")

@router.get("/api/perfil_cliente")
async def obtener_perfil_cliente(telefono: str, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Consultando perfil profundo de {enmascarar_telefono(telefono)}")
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Parámetro telefónico inválido.")
            
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("prospectos").select("id, notas, etiquetas, fila, perfil_psicologico")
                .eq("telefono", tel_norm).eq("vendedor_id", str(vendedor_id)).limit(1)
            ),
            timeout=5.0
        )
        
        return {"status": "ok", "datos": res.data[0] if res.data else {}}
    except HTTPException: raise
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en perfil_cliente: {e}")
        raise HTTPException(status_code=500, detail="Fallo interno en consulta de perfil.")

@router.get("/api/columnas")
async def obtener_columnas(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Obteniendo Layout de Columnas")
    try:
        # 🔧 FIX ORDEN: igual que en cargar_todo, se ordena por 'orden'.
        res = await asyncio.wait_for(
            async_db_execute(supabase.table("configuracion").select("nombre_columna, orden").eq("vendedor_id", str(vendedor_id)).order('orden')),
            timeout=5.0
        )
        return {"status": "ok", "columnas": [sanitizar_nombre_columna(item["nombre_columna"]) for item in (res.data or [])]}
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo recuperando columnas: {e}")
        raise HTTPException(status_code=500, detail="Error al solicitar columnas configuradas.")

@router.post("/api/reordenar_columnas")
async def reordenar_columnas(datos: ReordenarColumnasAction, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    """
    Guarda el nuevo orden de las columnas dinámicas (incluye "+").
    Blindaje no negociable: los bloques fijos de izquierda y derecha deben
    llegar EXACTOS en su posición obligatoria, o se rechaza con 400 sin
    guardar nada. Esta ruta nunca toca la tabla 'prospectos' — solo
    'configuracion.orden' — así que no existe forma de que mueva tarjetas.
    """
    logger.info(f"🎮 [TRACE:{trace_id}] Guardando nuevo orden de columnas para {vendedor_id}")
    try:
        columnas = datos.columnas
        n_izq, n_der = len(COLUMNAS_FIJAS_IZQ), len(COLUMNAS_FIJAS_DER)

        if len(columnas) < n_izq + n_der:
            raise HTTPException(status_code=400, detail="Estructura de columnas inválida: faltan columnas obligatorias.")

        bloque_izq = [c.upper() for c in columnas[:n_izq]]
        if bloque_izq != [c.upper() for c in COLUMNAS_FIJAS_IZQ]:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Intento de alterar bloque fijo izquierdo rechazado. Tenant={vendedor_id}")
            raise HTTPException(status_code=400, detail="Las columnas fijas de la izquierda no están en su posición obligatoria.")

        bloque_der = [c.upper() for c in columnas[-n_der:]]
        if bloque_der != [c.upper() for c in COLUMNAS_FIJAS_DER]:
            logger.warning(f"⚠️ [TRACE:{trace_id}] Intento de alterar bloque fijo derecho rechazado. Tenant={vendedor_id}")
            raise HTTPException(status_code=400, detail="Las columnas fijas de la derecha no están en su posición obligatoria.")

        zona_dinamica = columnas[n_izq:-n_der]  # incluye "+" siempre, más cualquier columna real creada por el usuario

        # Persistimos el orden completo: fijas (siempre igual) + dinámicas (lo que de verdad cambió).
        # FIX FASE 1 (mismo patrón del resto del archivo): allow_retry=False por ser mutación.
        for idx, nombre_col in enumerate(COLUMNAS_FIJAS_IZQ):
            await asyncio.wait_for(
                async_db_execute(
                    supabase.table("configuracion").update({"orden": idx})
                    .eq("vendedor_id", str(vendedor_id)).ilike("nombre_columna", nombre_col),
                    allow_retry=False
                ),
                timeout=8.0
            )
        for idx, nombre_col in enumerate(zona_dinamica):
            nombre_seguro = sanitizar_nombre_columna(nombre_col, permitir_reservadas=True)
            await asyncio.wait_for(
                async_db_execute(
                    supabase.table("configuracion").update({"orden": n_izq + idx})
                    .eq("vendedor_id", str(vendedor_id)).eq("nombre_columna", nombre_seguro),
                    allow_retry=False
                ),
                timeout=8.0
            )
        for idx, nombre_col in enumerate(COLUMNAS_FIJAS_DER):
            await asyncio.wait_for(
                async_db_execute(
                    supabase.table("configuracion").update({"orden": n_izq + len(zona_dinamica) + idx})
                    .eq("vendedor_id", str(vendedor_id)).ilike("nombre_columna", nombre_col),
                    allow_retry=False
                ),
                timeout=8.0
            )

        logger.info(f"✅ [TRACE:{trace_id}] Orden de columnas guardado para {vendedor_id}")
        return {"status": "ok"}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Error guardando orden de columnas: {e}")
        raise HTTPException(status_code=500, detail="Error al guardar el nuevo orden de columnas.")

# ==========================================================
# 📱 11. ENDPOINTS MÓVILES (APP ASESORES Y GODOT)
# ==========================================================
@router.get("/api/mobile/chat_history")
async def get_mobile_chat_history(telefono: str, limit: int = 50, offset: int = 0, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Solicitando historial móvil para Tel={enmascarar_telefono(telefono)}")
    try:
        tel_norm = normalizar_telefono(telefono)
        if not tel_norm: return {"status": "ok", "historial": []}
        
        offset_seguro, limit_seguro = max(0, offset), min(limit, 100)
        
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("mensajes_chat").select("mensaje, autor, created_at")
                .eq("vendedor_id", str(vendedor_id)).eq("telefono", tel_norm)
                .order("created_at", desc=True).range(offset_seguro, offset_seguro + limit_seguro - 1)
            ),
            timeout=8.0
        )
        
        historial_formateado = [
            {
                "contenido": bleach.clean(str(m.get("mensaje") or ""), tags=[], strip=True),
                "es_mio": str(m.get("autor", "")).upper() in ["BOT", "ASESOR", "HUMANO", "SISTEMA", "BOT_REMARKETING", "VENDEDOR"],
                "fecha": str(m.get("created_at", ""))
            } for m in reversed(res.data or [])
        ]
        return {"status": "ok", "historial": historial_formateado}
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Error recuperando chat_history móvil: {e}")
        raise HTTPException(status_code=500, detail="Error al recuperar logs de conversación.")

@router.post("/api/mobile/send_message")
async def send_mobile_message(data: MobileMessageRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async
    
    tel_norm = normalizar_telefono(data.to)
    mensaje_limpio = str(data.msg).strip()
    
    logger.info(f"🎮 [TRACE:{trace_id}] Handoff humano: Despachando mensaje saliente hacia {enmascarar_telefono(tel_norm)}")
    
    if not tel_norm or not mensaje_limpio: raise HTTPException(status_code=400, detail="Datos incompletos.")
    if len(mensaje_limpio) > 4096: raise HTTPException(status_code=413, detail="Mensaje demasiado largo.")
        
    llave_outbound = f"{vendedor_id}:{tel_norm}"
    
    async with await get_lock(f"mobile_outbound_{llave_outbound}"):
        envios_recientes = RATE_LIMIT_MOBILE_OUTBOUND.get(llave_outbound, 0)
        if envios_recientes > 10: 
            logger.warning(f"🚨 [TRACE:{trace_id}] Límite outbound excedido para {llave_outbound}")
            raise HTTPException(status_code=429, detail="Límite masivo excedido. Espera un momento.")
        RATE_LIMIT_MOBILE_OUTBOUND[llave_outbound] = envios_recientes + 1

    try:
        res_conf = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', str(vendedor_id)).limit(1)),
            timeout=5.0
        )
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración de Meta no encontrada en este tenant.")
            
        config = res_conf.data[0]
        
        # FIX FASE 4: TRAZABILIDAD ANTES QUE EFECTO SECUNDARIO.
        # Guardamos en base de datos primero para garantizar auditoría; si la API externa cae o responde lento, el mensaje humano no queda huérfano.
        await guardar_mensaje_chat(tel_norm, str(vendedor_id), 'ASESOR', mensaje_limpio)
        await actualizar_estado_crm(tel_norm, str(vendedor_id), "En Conversacion", "azul", "")
        
        # Efecto secundario (Llamada HTTP externa a la API de Meta)
        await disparar_whatsapp_dinamico_async(tel_norm, mensaje_limpio, config.get('meta_token'), config.get('meta_phone_id'))
        
        return {"status": "ok", "message": "Enviado"}
    except HTTPException: raise
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Error retransmitiendo handoff manual: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al despachar el mensaje.")

@router.get("/api/mobile/dashboard")
async def mobile_dashboard(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Compilando Dashboard Móvil para {vendedor_id}")
    try:
        hoy_inicio = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        
        ventas_res = await asyncio.wait_for(
            async_db_execute(supabase.table("ventas").select("monto").eq("vendedor_id", str(vendedor_id)).gte("created_at", hoy_inicio)),
            timeout=10.0
        )
        total_hoy = sum((float(v.get("monto") or 0.0) for v in (ventas_res.data or [])))

        prospectos_res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("prospectos").select("id, nombre, telefono, fila, ultima_interaccion_ia, ultimo_msj")
                .eq("vendedor_id", str(vendedor_id)).order("ultima_interaccion_ia", desc=True).limit(50)
            ),
            timeout=8.0
        )
        
        prospectos_limpios = [
            {
                "id": p.get("id"),
                "nombre": bleach.clean(p.get("nombre") or "Cliente", tags=[], strip=True),
                "telefono": normalizar_telefono(p.get("telefono", "")),
                "fila": sanitizar_nombre_columna(p.get("fila") or "Bandeja Nueva"),
                "ultima_interaccion_ia": p.get("ultima_interaccion_ia") or "",
                "ultimo_msj": bleach.clean(p.get("ultimo_msj") or "", tags=[], strip=True)
            } for p in (prospectos_res.data or [])
        ]
            
        return {"status": "ok", "vendedor": vendedor_id, "ventas_hoy": total_hoy, "prospectos": prospectos_limpios}
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Error en mobile_dashboard: {e}")
        raise HTTPException(status_code=500, detail="Error interno al compilar dashboard móvil.")

# ==========================================================
# 🚀 12. GESTIÓN DE TARJETAS CRM (MOVIMIENTOS Y BORRADOS)
# ==========================================================
@router.post("/api/actualizar_estado")
async def actualizar_estado(datos: EstadoUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Forzando Estado CRM a: {datos.nueva_fila}")
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Identificador obligatorio.")
            
        col_segura = sanitizar_nombre_columna(datos.nueva_fila, permitir_reservadas=True)
        
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').update({'fila': col_segura})
                .eq('vendedor_id', str(_sesion)).eq('telefono', tel_norm),
                allow_retry=False
            ),
            timeout=8.0
        )
        
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Registro no encontrado.")
    except HTTPException: raise
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo actualizando tarjeta: {e}")
        raise HTTPException(status_code=500, detail="Error transaccional.")

@router.post("/api/borrar_prospecto")
async def borrar_prospecto(datos: BorrarRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Soft Delete ({FILA_PAPELERA}) para ID: '{datos.nombre}'")
    try:
        prospecto_id = datos.nombre.strip()
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        resultado = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').update({'fila': FILA_PAPELERA}) 
                .eq('vendedor_id', str(_sesion)).eq('id', prospecto_id),
                allow_retry=False
            ),
            timeout=8.0
        )
        
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Prospecto no encontrado para archivar.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo al archivar prospecto: {e}")
        raise HTTPException(status_code=500, detail="Error en base de datos al archivar.")

@router.post("/api/borrar_permanente")
async def borrar_permanente(datos: BorrarRequest, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.warning(f"💀 🎮 [TRACE:{trace_id}] Hard Delete ejecutado para ID: '{datos.nombre}'")
    try:
        res_admin = await asyncio.wait_for(
            async_db_execute(supabase.table('usuarios_veltrix').select('rol').eq('vendedor_id', str(_sesion)).limit(1)),
            timeout=5.0
        )
        if not res_admin.data or str(res_admin.data[0].get('rol', '')).lower() != 'admin':
            logger.warning(f"🚨 [TRACE:{trace_id}] Intento de Hard Delete bloqueado. Requiere privilegios de Administrador.")
            raise HTTPException(status_code=403, detail="Operación denegada. Se requieren privilegios de Administrador.")

        prospecto_id = datos.nombre.strip()
        # FIX FASE 1: allow_retry=False por destrucción crítica de datos (DELETE)
        resultado = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').delete().eq('vendedor_id', str(_sesion)).eq('id', prospecto_id),
                allow_retry=False
            ),
            timeout=8.0
        )
        
        if resultado.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="El prospecto no existe.")
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en hard delete: {e}")
        raise HTTPException(status_code=500, detail="Fallo en la base de datos al eliminar.")

@router.post("/api/mover_prospecto")
async def mover_prospecto(datos: ColumnaUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Moviendo prospecto a fila: {datos.nueva_fila}")
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Identificador obligatorio.")
            
        col_final = sanitizar_nombre_columna(datos.nueva_fila if datos.nueva_fila else datos.columna)
        
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').update({"fila": col_final})
                .eq('telefono', tel_norm).eq('vendedor_id', str(_sesion)),
                allow_retry=False
            ),
            timeout=8.0
        )
        return {"status": "ok", "mensaje": f"Movido a {col_final}"}
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Error transaccional en mover_prospecto: {e}")
        raise HTTPException(status_code=500, detail="Error transaccional.")

@router.post("/api/actualizar_notes")
async def actualizar_notas(datos: NotasUpdate, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    try:
        tel_norm = normalizar_telefono(datos.telefono)
        if not tel_norm: raise HTTPException(status_code=400, detail="Número obligatorio.")
            
        update_data = {
            "notas": bleach.clean(datos.notas or "", tags=[], strip=True)[:5000], 
            "etiquetas": bleach.clean(datos.etiquetas or "", tags=[], strip=True)[:5000], 
            "nombre": bleach.clean(datos.nombre or "Cliente", tags=[], strip=True)[:120]
        }
        # FIX FASE 1: allow_retry=False por mutación (UPDATE)
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table('prospectos').update(update_data).eq('telefono', tel_norm).eq('vendedor_id', str(_sesion)),
                allow_retry=False
            ),
            timeout=8.0
        )
        
        if res and res.data: return {"status": "ok"}
        raise HTTPException(status_code=404, detail="Tarjeta no localizada.")
    except HTTPException: raise
    except Exception as e: 
        logger.exception(f"❌ [TRACE:{trace_id}] Error inyectando notas CRM: {e}")
        raise HTTPException(status_code=500, detail="Error al sincronizar apuntes.")

# ==========================================================
# 📦 13. INVENTARIO Y STOCK (TRANSACCIONES ATÓMICAS)
# ==========================================================
@router.post("/api/actualizar_stock")
async def actualizar_stock(item: VentaItem, request: Request, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🎮 [TRACE:{trace_id}] Procesando Venta Atómica de {item.nombre_producto}")
    try:
        vid_str = str(_sesion)
        if not item.id: raise HTTPException(400, "ID requerido.")
        
        idempotency_key = request.headers.get("x-idempotency-key")
        async with ventas_idempotencia_lock:
            if idempotency_key and idempotency_key in ventas_procesadas_idempotencia:
                return ventas_procesadas_idempotencia[idempotency_key]

        res_inv = await asyncio.wait_for(
            async_db_execute(supabase.table("inventario").select("id, nombre, stock, precio").eq("id", item.id).eq("vendedor_id", vid_str).limit(1)),
            timeout=10.0
        )
        
        if not res_inv.data: raise HTTPException(status_code=404, detail="Artículo no localizado.")
        
        db_item = res_inv.data[0]
        stock_actual = int(db_item.get("stock", 0))
        precio_venta = float(db_item.get("precio", 0.0))
        cantidad_descontar = max(1, item.cantidad_vendida) if item.cantidad_vendida is not None else 1
        
        if cantidad_descontar > stock_actual:
            raise HTTPException(status_code=400, detail=f"Stock insuficiente. Solicitado: {cantidad_descontar}, Disponible: {stock_actual}")

        nuevo_stock_seguro = stock_actual - cantidad_descontar

        # Optimistic Locking 
        # FIX FASE 1: allow_retry=False por mutación concurrente crítica
        res_update = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario").update({"stock": nuevo_stock_seguro})
                .eq("id", item.id).eq("stock", stock_actual),
                allow_retry=False
            ),
            timeout=10.0
        )
        
        if not res_update.data: raise HTTPException(status_code=409, detail="Colisión de inventario: El stock cambió en otra transacción paralela.")

        transaccion_id = str(uuid.uuid4())
        try:
            # FIX FASE 1: allow_retry=False por inserción financiera (INSERT)
            await asyncio.wait_for(
                async_db_execute(
                    supabase.table("ventas").insert({
                        "vendedor_id": vid_str, "nombre_producto": db_item.get("nombre", item.nombre_producto),
                        "monto": precio_venta * cantidad_descontar, "cantidad": cantidad_descontar,
                        "stock_anterior": stock_actual, "stock_nuevo": nuevo_stock_seguro,
                        "tx_uuid": transaccion_id, "created_at": datetime.now(timezone.utc).isoformat()
                    }),
                    allow_retry=False
                ),
                timeout=10.0
            )
        except Exception as e:
            logger.critical(f"🚨 [TRACE:{trace_id}] Fallo al insertar historial de venta. Ejecutando ROLLBACK de stock seguro. Error: {e}")
            await asyncio.wait_for(
                async_db_execute(supabase.table("inventario").update({"stock": stock_actual}).eq("id", item.id), allow_retry=False),
                timeout=10.0
            )
            raise HTTPException(status_code=500, detail="Fallo transaccional al registrar la venta. Stock revertido a su estado original.")
        
        respuesta_exitosa = {"status": "ok", "nuevo_stock": nuevo_stock_seguro, "tx_id": transaccion_id}
        
        async with ventas_idempotencia_lock:
            if idempotency_key: ventas_procesadas_idempotencia[idempotency_key] = respuesta_exitosa
            
        return respuesta_exitosa
    
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en transacción de venta: {e}")
        raise HTTPException(status_code=500, detail="Fallo crítico al procesar la venta.")

# ==========================================================
# 📦 13B. INVENTARIO: CARGA, EDICIÓN Y BORRADO (VISOR)
# ==========================================================

class EditarItemVisorRequest(BaseModel):
    id: Any
    nombre: Optional[str] = ""
    consola: Optional[str] = ""
    precio: float = 0.0
    stock: int = 0

class BorrarItemRequest(BaseModel):
    id: Any
    nombre: Optional[str] = ""
    consola: Optional[str] = ""

@router.get("/api/cargar_inventario")
async def cargar_inventario(vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"📦 [TRACE:{trace_id}] Cargando inventario para {vendedor_id}")
    try:
        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario")
                .select("*")
                .eq("vendedor_id", str(vendedor_id))
                .order("nombre")
                .limit(500)
            ),
            timeout=12.0
        )
        items = res.data or []
        # Si algún ítem trae datos extra anidados (ej. de importación CSV), los
        # exponemos también en el nivel superior sin pisar columnas reales.
        for it in items:
            extra = it.get("atributos_extra")
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if k not in it or it.get(k) in (None, ""):
                        it[k] = v
        return {"status": "ok", "inventario": items}
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en cargar_inventario: {e}")
        raise HTTPException(status_code=500, detail="Error interno al recuperar inventario.")

@router.post("/api/editar_item_visor")
async def editar_item_visor(item: EditarItemVisorRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"✏️ [TRACE:{trace_id}] Editando ítem id={item.id} para {vendedor_id}")
    try:
        if not item.id:
            raise HTTPException(status_code=400, detail="ID requerido.")

        campos: dict = {"precio": item.precio, "stock": max(0, item.stock)}
        if item.nombre and item.nombre.strip():
            campos["nombre"] = bleach.clean(item.nombre.strip(), tags=[], strip=True)
        if item.consola and item.consola.strip():
            # En Supabase la columna se llama "categoria", no "consola"
            campos["categoria"] = bleach.clean(item.consola.strip(), tags=[], strip=True)

        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario").update(campos)
                .eq("id", item.id).eq("vendedor_id", str(vendedor_id)),
                allow_retry=False
            ),
            timeout=10.0
        )
        return {"status": "ok", "updated": len(res.data) if res.data else 0}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en editar_item_visor: {e}")
        raise HTTPException(status_code=500, detail="Error interno al editar ítem.")

@router.post("/api/borrar_item")
async def borrar_item(item: BorrarItemRequest, vendedor_id: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    logger.info(f"🗑️ [TRACE:{trace_id}] Borrando ítem id={item.id} para {vendedor_id}")
    try:
        if not item.id:
            raise HTTPException(status_code=400, detail="ID requerido.")

        res = await asyncio.wait_for(
            async_db_execute(
                supabase.table("inventario").delete()
                .eq("id", item.id).eq("vendedor_id", str(vendedor_id)),
                allow_retry=False
            ),
            timeout=10.0
        )
        return {"status": "ok", "deleted": len(res.data) if res.data else 0}
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en borrar_item: {e}")
        raise HTTPException(status_code=500, detail="Error interno al eliminar ítem.")

# ==========================================================
# 📢 14. ENVÍOS MASIVOS (MARKETING AUTOMATION - BATCHING AAA)
# ==========================================================
def procesar_en_lotes(lista, n):
    for i in range(0, len(lista), n): yield lista[i:i + n]

async def background_enviar_campana(prospectos: list, mensaje: str, meta_token: str, meta_phone_id: str, trace_id: str):
    from ai_whatsapp_media import disparar_whatsapp_dinamico_async
    logger.info(f"🚀 [TRACE:{trace_id}] Iniciando envío masivo a {len(prospectos)} prospectos (En Lotes).")
    
    sem = asyncio.Semaphore(10)

    async def enviar_con_semaforo(p):
        async with sem:
            try:
                await disparar_whatsapp_dinamico_async(p['telefono'], mensaje, meta_token, meta_phone_id)
                await asyncio.sleep(0.1) 
            except Exception as e:
                logger.error(f"❌ [TRACE:{trace_id}] Fallo envío a {enmascarar_telefono(p['telefono'])}: {str(e)}")

    for lote in procesar_en_lotes(prospectos, 50):
        tareas = [asyncio.create_task(enviar_con_semaforo(p)) for p in lote]
        await asyncio.gather(*tareas)
        await asyncio.sleep(1.0) 
    
    logger.info(f"✅ [TRACE:{trace_id}] Campaña masiva completada exitosamente.")

@router.post("/api/mensaje_masivo")
async def ejecutar_campana_masiva(datos: CampanaMasivaRequest, background_tasks: BackgroundTasks, _sesion: str = Depends(verificar_sesion_b2b), trace_id: str = Depends(obtener_trace_id)):
    try:
        vendedor_id = str(_sesion)
        res_conf = await asyncio.wait_for(
            async_db_execute(supabase.table('configuracion_bot').select('meta_token, meta_phone_id').eq('vendedor_id', vendedor_id).limit(1)),
            timeout=5.0
        )
        if not res_conf.data: raise HTTPException(status_code=404, detail="Configuración de Meta no encontrada para este tenant.")
            
        config = res_conf.data[0]
        meta_token, meta_phone = config.get('meta_token'), config.get('meta_phone_id')
        
        if not meta_token or not meta_phone: raise HTTPException(status_code=400, detail="Las credenciales de Meta están incompletas.")

        fila_a_buscar = datos.columna_origen
        res_prospectos = await async_db_execute(
            supabase.table('prospectos').select('telefono, nombre').eq('vendedor_id', vendedor_id).eq('fila', fila_a_buscar)
        )
        
        prospectos_data = res_prospectos.data or []
        if not prospectos_data: return {"status": "ok", "msg": "No hay prospectos en la columna especificada."}
        
        background_tasks.add_task(
            background_enviar_campana, prospectos_data, datos.mensaje, meta_token, meta_phone, trace_id
        )
            
        return {"status": "ok", "msg": "Campaña encolada en background", "total_objetivos": len(prospectos_data)}
        
    except HTTPException: raise
    except Exception as e:
        logger.exception(f"❌ [TRACE:{trace_id}] Fallo en encolamiento de ejecución masiva: {e}")
        raise HTTPException(status_code=500, detail="Fallo campaña masiva.")
