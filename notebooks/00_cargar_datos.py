# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Cargar datos del workshop a un Volume
# MAGIC
# MAGIC Los datos del workshop viven en la carpeta **`data/`** de este repo. Auto Loader y las
# MAGIC Declarative Pipelines leen desde **almacenamiento de objetos / Volumes**, no desde archivos del
# MAGIC workspace, así que este notebook **copia `data/` a tu Volume** (el `BASE_PATH` de `config.py`).
# MAGIC
# MAGIC Ejecútalo **una sola vez** después de editar `config.py`. Si vas a usar el Volume compartido que
# MAGIC ya tiene los datos, puedes saltarte este paso.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

import os

# --- ubicar la carpeta data/ del repo (hermana de notebooks/) ---
def _repo_data_dir():
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebooks_dir = os.path.dirname(nb)          # .../workshopizzi/notebooks
    repo_root = os.path.dirname(notebooks_dir)   # .../workshopizzi
    return "/Workspace" + repo_root + "/data"

DATA_DIR = _repo_data_dir()
print("Origen (repo):", DATA_DIR)
print("Destino (Volume):", BASE_PATH)

# COMMAND ----------

# --- si BASE_PATH es un Volume UC, crearlo si no existe ---
if BASE_PATH.startswith("/Volumes/"):
    parts = BASE_PATH.strip("/").split("/")   # ['Volumes', catalog, schema, volume]
    if len(parts) >= 4:
        _cat, _sch, _vol = parts[1], parts[2], parts[3]
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{_cat}`.`{_sch}`")
        spark.sql(f"CREATE VOLUME IF NOT EXISTS `{_cat}`.`{_sch}`.`{_vol}`")
        print(f"Volume asegurado: {_cat}.{_sch}.{_vol}")

# COMMAND ----------

# --- copiar cada subcarpeta del repo al Volume ---
for sub in [RAW_SUBDIR, EVOLUTION_SUBDIR, MULTILINE_SUBDIR]:
    src = f"file:{DATA_DIR}/{sub}"
    dst = f"{BASE_PATH}/{sub}"
    print(f"Copiando {sub} ...")
    dbutils.fs.cp(src, dst, recurse=True)

print("\nContenido del Volume:")
display(dbutils.fs.ls(BASE_PATH))

# COMMAND ----------

# --- verificación rápida ---
n = spark.read.json(RAW_PATH).count()
print(f"OK · {n:,} eventos JSON-lines cargados en {RAW_PATH}")
print("Listo para empezar con 01_delta_lab. 🚀")
