# ==========================================================
# 🚀 MÓDULO: media_and_whatsapp.py
# ==========================================================
# 🚀 SISTEMA BACKEND: VELTRIX ENGINE V20.2 (AAA ENTERPRISE)
# Control de Meta y Archivos Locales
# ==========================================================

import os
import httpx
from cachetools import TTLCache

# ==========================================================
# 🔌 IMPORTACIONES NATIVAS VELTRIX ENTERPRISE
# ==========================================================
from config_and_schemas import *
from ai_whatsapp_media import *

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "").strip()

# 🛡️ FIX AAA: Protección Anti Audio Bomb
AUDIO_HASHES_PROCESADOS = TTLCache(maxsize=10000, ttl=1800)

