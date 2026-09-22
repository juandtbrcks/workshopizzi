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
# Ruta base donde están los datos del workshop.
# Spark lee igual de un Volume, S3, ADLS o GCS — solo cambia la ruta.
# Ejemplos:
#   Volume UC:  "/Volumes/mi_catalogo/mi_schema/mi_volume"
#   S3:         "s3://mi-bucket-izzi/landing/telemetria"
# Si usas S3 necesitas un External Location en Unity Catalog.
BASE_PATH = "/Volumes/jgworkspaceclassic_catalog/prueba_izzi/workshop"

# Subcarpetas dentro de BASE_PATH
RAW_SUBDIR       = "raw_jsonl"
EVOLUTION_SUBDIR = "raw_jsonl_evolucion"
MULTILINE_SUBDIR = "multiline_sample"


