# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,README
# MAGIC %md
# MAGIC # Workshop Izzi — Data Engineering en Databricks
# MAGIC
# MAGIC Bienvenidos. Durante 2 sesiones construiremos, de punta a punta, un pipeline de
# MAGIC **network analytics** sobre eventos de red  del JSON crudo a una capa
# MAGIC **oro**
# MAGIC
# MAGIC
# MAGIC ## Datos del workshop
# MAGIC La ruta base de los datos se configura en `config.py` (variable `BASE_PATH`).
# MAGIC Por defecto apunta al Volume compartido, pero acepta cualquier ruta que Spark pueda leer
# MAGIC (S3, ADLS, GCS vía External Location).
# MAGIC
# MAGIC Subcarpetas esperadas dentro de `BASE_PATH`:
# MAGIC - `raw_jsonl/event_date=YYYY-MM-DD/` → 7 días, ~28K eventos (JSON-lines) — ejercicios iniciales de Auto Loader.
# MAGIC - `raw_jsonl_evolucion/` → 1 archivo con **esquema evolucionado** (+`collector_version`, +`latency_ms`) — para schema evolution.
# MAGIC - `multiline_sample/` → array JSON multilínea (para ilustrar un *gotcha* de ingesta).
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC 1. Edita **`config.py`**:
# MAGIC    - `CATALOG` — catálogo de Unity Catalog.
# MAGIC    - `BASE_PATH` — ruta base de los datos (Volume, S3, ADLS o GCS).
# MAGIC    - `WORK_SCHEMA` — esquema donde creas tus tablas (cada participante usa el suyo).
# MAGIC 2. Cada notebook empieza con `%run ./_setup`, que lee `config.py`, crea tu esquema y expone las variables.
# MAGIC 3. Ejecuta celda por celda. Las celdas marcadas **`# TODO`** son ejercicios para ti.
# MAGIC
# MAGIC ## Agenda
# MAGIC | # | Notebook | Sesión | Tema |
# MAGIC |---|---|---|---|
# MAGIC | 01 | `01_delta_lab` | Día 1 | Tablas Delta: crear, MERGE, time travel |
# MAGIC | 02 | `02_autoloader_lab` | Día 1 | Auto Loader: ingesta incremental, checkpoint, `_metadata`, multiline, schema evolution |
# MAGIC | 03 | `03_declarative_pipeline` | Día 2 | Lakeflow Declarative Pipelines: teoría + prompt Genie Code (bronze→silver→gold SCD2) |
# MAGIC | 04 | `04_unity_catalog_lab` | Día 2 | Unity Catalog: permisos, linaje, tags |
# MAGIC | 05 | `05_optimizacion_delta_lab` | Día 2 | Liquid Clustering, ZSTD, file sizing |
# MAGIC
# MAGIC > *Lakehouse Federation* se cubre de forma teórica (sin lab dedicado).

# COMMAND ----------

# MAGIC %run ./_setup