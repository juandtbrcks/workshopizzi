# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # ⚙️ _setup — Bootstrap de configuración
# MAGIC
# MAGIC Este notebook se ejecuta al inicio de cada lab con `%run ./_setup`.
# MAGIC Lee el archivo **`config.py`** (edítalo para cambiar catálogo/esquema/volumen),
# MAGIC deriva las rutas, crea tu esquema de trabajo y expone estas variables:
# MAGIC
# MAGIC | Variable | Descripción |
# MAGIC |---|---|
# MAGIC | `CATALOG` | Catálogo UC |
# MAGIC | `WORK_SCHEMA` | Esquema donde creas tus tablas (definido en `config.py`) |
# MAGIC | `DATA_PATH` | Carpeta con los JSONL de Kentik (1.5M+ registros) |
# MAGIC | `EVOLUTION_PATH` | Carpeta con JSONL de esquema evolucionado (definida en `config.py`) |
# MAGIC | `CLEAN_COLS` | Lista de 35 columnas limpias (top-level + extraídas de `custom_str`) |
# MAGIC | `read_clean(limit=N)` | Helper: lee JSONL, aplana `custom_str.*`, devuelve DataFrame limpio |

# COMMAND ----------

# DBTITLE 1,Bootstrap config
import os

# --- cargar config.py (junto a este notebook, vía workspace files) ---
def _config_dir():
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return "/Workspace" + os.path.dirname(nb)
    except Exception:
        return os.getcwd()

_dir = _config_dir()
_config_path = os.path.join(_dir, "config.py")
assert os.path.exists(_config_path), (
    f"No se encontró config.py en {_dir}. Asegúrate de que config.py esté junto a los notebooks."
)
exec(open(_config_path).read())  # importa CATALOG, DATA_PATH, etc. al scope local

# --- rutas de datos (definidas en config.py) ---
DATA_PATH       = DATA_PATH.rstrip("/")
EVOLUTION_PATH  = EVOLUTION_PATH.rstrip("/")

# Campos de negocio que viven dentro de custom_str (struct anidado).
# Se extraen como columnas top-level con alias limpio.
_CUSTOM_STR_EXTRACTS = {
    "device_site":       "custom_str.device_site",
    "device_site_market": "custom_str.device_site_market",
    "application":       "custom_str.application",
    "src_as_name":       "custom_str.src_as_name",
    "dst_as_name":       "custom_str.dst_as_name",
    "service_provider":  "custom_str.service_provider",
    "service_type":      "custom_str.service_type",
    "src_connect_type":  "custom_str.src_connect_type",
    "dst_connect_type":  "custom_str.dst_connect_type",
    "sampler_address":   "custom_str.SamplerAddress",
}

# Columnas top-level seguras (sin custom_*)
_TOP_COLS = [
    "timestamp", "protocol", "src_addr", "dst_addr",
    "l4_src_port", "l4_dst_port",
    "in_bytes", "in_pkts", "out_bytes", "out_pkts",
    "src_as", "dst_as",
    "src_geo", "dst_geo", "src_geo_city", "dst_geo_city",
    "src_geo_region", "dst_geo_region",
    "device_name", "device_id",
    "sample_rate", "tcp_flags", "ip_size",
    "eventType", "provider",
]

# Lista combinada de columnas que quedan en el DataFrame limpio
CLEAN_COLS = _TOP_COLS + list(_CUSTOM_STR_EXTRACTS.keys())

def read_clean(path=None, limit=None):
    """Lee JSONL y devuelve DataFrame con columnas limpias (extrae custom_str.*)."""
    from pyspark.sql import functions as F
    df = spark.read.json(path or DATA_PATH)
    selects = [F.col(c) for c in _TOP_COLS]
    for alias, src in _CUSTOM_STR_EXTRACTS.items():
        selects.append(F.col(src).alias(alias))
    df = df.select(*selects)
    return df.limit(limit) if limit else df

# --- esquema de trabajo (definido en config.py por cada participante) ---
_user = spark.sql("SELECT current_user()").collect()[0][0]

# --- crear esquema de trabajo y fijar contexto ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{WORK_SCHEMA}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{WORK_SCHEMA}`")

# --- volumen para checkpoints de streaming (Auto Loader / DLT interactivo) ---
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{WORK_SCHEMA}`.chk")
CHK_PATH = f"/Volumes/{CATALOG}/{WORK_SCHEMA}/chk"

def tbl(name: str) -> str:
    """Nombre calificado de una tabla en tu esquema de trabajo."""
    return f"`{CATALOG}`.`{WORK_SCHEMA}`.`{name}`"

print("=" * 60)
print(f"  Usuario         : {_user}")
print(f"  Catálogo        : {CATALOG}")
print(f"  Esquema trabajo : {WORK_SCHEMA}  (tus tablas se crean aquí)")
print(f"  DATA_PATH       : {DATA_PATH}  (JSONL completos)")
print(f"  EVOLUTION_PATH  : {EVOLUTION_PATH}")
print(f"  CHK_PATH        : {CHK_PATH}  (checkpoints)")
print("=" * 60)