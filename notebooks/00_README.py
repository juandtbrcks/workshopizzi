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
# MAGIC La ruta de datos se configura en `config.py` (variable `DATA_PATH`).
# MAGIC Por defecto apunta al Volume con los JSONL preprocesados de Kentik (1.5M+ flujos reales).
# MAGIC Acepta cualquier ruta que Spark pueda leer (Volume UC, S3, ADLS, GCS vía External Location).
# MAGIC
# MAGIC Los datos son **flujos de red reales (Kentik/KFlow)** exportados desde la plataforma de monitoreo
# MAGIC de Izzi. Cada registro es un flujo IP muestreado: IPs origen/destino, puertos,
# MAGIC protocolo, bytes/paquetes, geolocalización, dispositivo de red, ASN, etc.
# MAGIC
# MAGIC Dos carpetas (configurables en `config.py`):
# MAGIC - **`DATA_PATH`** → archivos JSONL preprocesados (1.5M+ flujos reales, ~66 campos).
# MAGIC - **`EVOLUTION_PATH`** → 1 archivo JSONL con **esquema evolucionado** (+`collector_version`, +`latency_ms`) — para schema evolution.
# MAGIC
# MAGIC ## Cómo trabajar
# MAGIC 1. Edita **`config.py`**:
# MAGIC    - `CATALOG` — catálogo de Unity Catalog.
# MAGIC    - `DATA_PATH` — ruta donde viven los JSONL (Volume, S3, ADLS o GCS).
# MAGIC    - `WORK_SCHEMA` — esquema donde creas tus tablas (cada participante usa el suyo).
# MAGIC 2. Cada notebook empieza con `%run ./_setup`, que lee `config.py`, crea tu esquema y expone las variables.
# MAGIC 3. Ejecuta celda por celda. Las celdas marcadas **`# TODO`** son ejercicios para ti.
# MAGIC
# MAGIC ## Agenda
# MAGIC | # | Notebook | Sesión | Tema |
# MAGIC |---|---|---|---|
# MAGIC | 01 | `01_delta_lab` | Día 1 | Tablas Delta: crear, MERGE, time travel (sobre flujos Kentik) |
# MAGIC | 02 | `02_autoloader_lab` | Día 1 | Auto Loader: ingesta incremental (1.5M+ flujos), checkpoint, `_metadata`, schema evolution |
# MAGIC | 03 | `03_declarative_pipeline` | Día 2 | Lakeflow Declarative Pipelines: teoría + prompt Genie Code (bronze→silver→gold) |
# MAGIC | 04 | `04_unity_catalog_lab` | Día 2 | Unity Catalog: permisos, linaje, tags, masking de IPs |
# MAGIC | 05 | `05_optimizacion_delta_lab` | Día 2 | Liquid Clustering, ZSTD, file sizing |
# MAGIC
# MAGIC > *Lakehouse Federation* se cubre de forma teórica (sin lab dedicado).

# COMMAND ----------

# MAGIC %run ./_setup