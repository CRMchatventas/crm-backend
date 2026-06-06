# ==========================================================
# 🔍 AUDITOR AUTOMÁTICO DE ESTRUCTURA VELTRIX
# ==========================================================

import ast
import json
from pathlib import Path

ARCHIVO_ANALIZAR = "main.py"
ARCHIVO_SALIDA = "estructura_veltrix.json"

# ==========================================================
# DETECTOR DE MÓDULO
# ==========================================================

def detectar_modulo(nombre_funcion: str) -> str:

    n = nombre_funcion.lower()

    if any(x in n for x in [
        "inventario",
        "stock",
        "producto",
        "catalogo",
        "precio"
    ]):
        return "inventario"

    if any(x in n for x in [
        "crm",
        "prospecto",
        "cliente",
        "kanban",
        "columna"
    ]):
        return "crm"

    if any(x in n for x in [
        "cita",
        "agenda",
        "calendario"
    ]):
        return "citas"

    if any(x in n for x in [
        "publicacion",
        "marketplace"
    ]):
        return "publicaciones"

    if any(x in n for x in [
        "whatsapp",
        "webhook",
        "mensaje"
    ]):
        return "whatsapp"

    if any(x in n for x in [
        "gemini",
        "ia",
        "rag",
        "copy",
        "prompt"
    ]):
        return "ia"

    if any(x in n for x in [
        "login",
        "auth",
        "sesion",
        "usuario",
        "token"
    ]):
        return "auth"

    return "core"

# ==========================================================
# ANALIZADOR
# ==========================================================

def analizar_archivo():

    codigo = Path(
        ARCHIVO_ANALIZAR
    ).read_text(
        encoding="utf-8",
        errors="ignore"
    )

    tree = ast.parse(codigo)

    resultado = {
        "total_funciones": 0,
        "total_clases": 0,
        "modulos": {}
    }

    # --------------------------------------
    # CLASES
    # --------------------------------------

    for nodo in ast.walk(tree):

        if isinstance(nodo, ast.ClassDef):

            resultado["total_clases"] += 1

            modulo = "modelos"

            if modulo not in resultado["modulos"]:
                resultado["modulos"][modulo] = []

            resultado["modulos"][modulo].append({
                "tipo": "class",
                "nombre": nodo.name,
                "linea": nodo.lineno
            })

    # --------------------------------------
    # FUNCIONES
    # --------------------------------------

    for nodo in ast.walk(tree):

        if isinstance(
            nodo,
            (
                ast.FunctionDef,
                ast.AsyncFunctionDef
            )
        ):

            resultado["total_funciones"] += 1

            modulo = detectar_modulo(
                nodo.name
            )

            if modulo not in resultado["modulos"]:
                resultado["modulos"][modulo] = []

            resultado["modulos"][modulo].append({
                "tipo": (
                    "async"
                    if isinstance(
                        nodo,
                        ast.AsyncFunctionDef
                    )
                    else "sync"
                ),
                "nombre": nodo.name,
                "linea": nodo.lineno
            })

    # Ordenar

    for modulo in resultado["modulos"]:

        resultado["modulos"][modulo].sort(
            key=lambda x: x["linea"]
        )

    with open(
        ARCHIVO_SALIDA,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            resultado,
            f,
            indent=4,
            ensure_ascii=False
        )

    print(
        f"\n✅ Archivo generado: "
        f"{ARCHIVO_SALIDA}"
    )

    print(
        f"\n📊 Funciones: "
        f"{resultado['total_funciones']}"
    )

    print(
        f"📊 Clases: "
        f"{resultado['total_clases']}"
    )

    print("\n📦 MÓDULOS DETECTADOS:\n")

    for modulo in resultado["modulos"]:

        cantidad = len(
            resultado["modulos"][modulo]
        )

        print(
            f"{modulo.upper():15}"
            f" -> {cantidad}"
        )

# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":
    analizar_archivo()
