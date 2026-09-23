# ============================================================
# Workshop Izzi — Data Engineering en Databricks
# Configuración editable. Ajusta estos valores y ejecuta
# %run ./_setup  al inicio de cada notebook.
# ============================================================

# Catálogo de Unity Catalog donde se trabaja
CATALOG = "jgworkspaceclassic_catalog"

# ── Esquema de trabajo ───────────────────────────────────────
# Esquema donde creas tus tablas Delta. Cada participante usa el suyo.
# Ejemplo: "prueba_izzi_juan", "prueba_izzi_equipo_1", etc.
WORK_SCHEMA = "prueba_izzi"

# ── Origen de datos ──────────────────────────────────────────
# Ruta donde viven los JSONL preprocesados de Kentik (1.5M+ registros).
# Generados a partir de los archivos JSON concatenados originales
# (preprocesamiento: arrays JSON → JSONL).
# Spark lee igual de un Volume, S3, ADLS o GCS — solo cambia la ruta.

#DATA_PATH = f"/Volumes/{CATALOG}/prueba_izzi/landing_jsonl"
DATA_PATH = "s3://pocdatabricksizzi/workshop/jsons/"

# ── Datos de evolución de esquema ─────────────────────────────
# Carpeta con JSONL que tienen columnas adicionales (collector_version, latency_ms)
# para el ejercicio de schema evolution del lab 02.

#EVOLUTION_PATH = f"/Volumes/{CATALOG}/prueba_izzi/landing_jsonl_evolucion"
EVOLUTION_PATH = "s3://pocdatabricksizzi/workshop/jsons_otro_esquema/"

