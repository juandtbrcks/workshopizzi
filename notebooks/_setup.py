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
# MAGIC | `BASE_PATH` | Raíz de los datos (Volume, S3, ADLS o GCS) |
# MAGIC | `RAW_PATH` | Carpeta con los JSON crudos (JSON-lines) |
# MAGIC | `EVOLUTION_PATH` | Carpeta con el esquema evolucionado |
# MAGIC | `MULTILINE_PATH` | Carpeta con la muestra multilínea (gotcha) |

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
exec(open(_config_path).read())  # importa CATALOG, BASE_PATH, etc. al scope local

# --- derivar rutas de datos desde BASE_PATH ---
BASE_PATH      = BASE_PATH.rstrip("/")
RAW_PATH       = f"{BASE_PATH}/{RAW_SUBDIR}"
EVOLUTION_PATH = f"{BASE_PATH}/{EVOLUTION_SUBDIR}"
MULTILINE_PATH = f"{BASE_PATH}/{MULTILINE_SUBDIR}"

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
print(f"  Origen datos    : {BASE_PATH}")
print(f"  RAW_PATH        : {RAW_PATH}")
print(f"  EVOLUTION_PATH  : {EVOLUTION_PATH}")
print(f"  MULTILINE_PATH  : {MULTILINE_PATH}")
print(f"  CHK_PATH        : {CHK_PATH}  (checkpoints)")
print("=" * 60)