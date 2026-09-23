# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 03 · Lakeflow Declarative Pipeline (Día 2) ⭐
# MAGIC
# MAGIC En este lab usaremos **Genie Code** para generar el código fuente de una
# MAGIC **Lakeflow Spark Declarative Pipeline** que construye el medallion completo del caso Izzi:
# MAGIC
# MAGIC ```
# MAGIC BRONZE  bronze_network_flows   ← Auto Loader lee los JSONL del Volume (casi crudo)
# MAGIC SILVER  silver_network_flows   ← Expectations (WARN/DROP/FAIL) + tipado + dedup por flow_id
# MAGIC GOLD    gold_device_site_map   ← SCD2 (AUTO CDC): dispositivo ⇄ site/zona con vigencia  [ENTREGABLE]
# MAGIC         gold_traffic_summary   ← tráfico agregado por dispositivo, protocolo, geo y hora
# MAGIC ```
# MAGIC
# MAGIC Primero repasaremos los conceptos clave (expectations, SCD2, medallion),
# MAGIC luego usarás un prompt detallado para que Genie Code genere la pipeline en un **notebook nuevo**,
# MAGIC y finalmente lo adjuntarás a una pipeline de Databricks para ejecutarlo.
# MAGIC
# MAGIC > ⚠️ El código de una Declarative Pipeline **no se corre celda por celda** — se adjunta a una
# MAGIC > *pipeline* que Databricks orquesta. Si lo corres aquí verás `ModuleNotFoundError: No module named 'dlt'`;
# MAGIC > eso es normal.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ./_setup

# COMMAND ----------

# DBTITLE 1,Concepto: Arquitectura Medallion en Declarative Pipelines
# MAGIC %md
# MAGIC ## 1. Arquitectura Medallion en Declarative Pipelines
# MAGIC
# MAGIC Una **Lakeflow Spark Declarative Pipeline** define tablas con decoradores Python (`@dlt.table`).
# MAGIC Databricks se encarga de la orquestación, reintentos, linaje y métricas.
# MAGIC
# MAGIC | Capa | Tabla | Qué hace |
# MAGIC |---|---|---|
# MAGIC | **Bronze** | `bronze_network_flows` | Auto Loader (`cloudFiles`) lee JSONL crudo. Agrega metadata de ingesta. Sin transformar. |
# MAGIC | **Silver** | `silver_network_flows` | Tipado fuerte, dedup por clave compuesta (src/dst/port/protocol/ts), **expectations** de calidad. |
# MAGIC | **Gold** | `gold_device_site_map` | **SCD2** con `apply_changes`: historial dispositivo ⇄ site/zona con vigencia temporal. |
# MAGIC | **Gold** | `gold_traffic_summary` | Tráfico agregado por dispositivo, protocolo, geo y hora (bytes, paquetes, flujos). |
# MAGIC
# MAGIC Cada tabla declara su `cluster_by` (Liquid Clustering) y `table_properties` (capa, compresión).

# COMMAND ----------

# DBTITLE 1,Concepto: Expectations (calidad de datos)
# MAGIC %md
# MAGIC ## 2. Expectations — calidad de datos declarativa
# MAGIC
# MAGIC Las **expectations** validan cada fila del DataFrame de salida. Hay 3 modos:
# MAGIC
# MAGIC | Modo | Decorador | Efecto | Cuándo usarlo |
# MAGIC |---|---|---|---|
# MAGIC | **WARN** | `@dlt.expect` | Registra la violación en métricas, **no descarta** la fila | Monitoreo: alertar sin bloquear (ej: SNR fuera de rango esperado) |
# MAGIC | **DROP** | `@dlt.expect_or_drop` | **Descarta** las filas que violan la regla | Calidad: filtrar datos inválidos (ej: IP malformada, MAC inválida) |
# MAGIC | **FAIL** | `@dlt.expect_or_fail` | **Detiene** la pipeline completa si alguna fila viola | Invariantes críticas (ej: `event_id IS NOT NULL`) |
# MAGIC
# MAGIC Puedes combinar múltiples reglas con `@dlt.expect_all_or_drop({...})`.
# MAGIC Las métricas de expectations se ven en el DAG de la pipeline (% de filas que pasan/fallan cada regla).
# MAGIC
# MAGIC ### Expectations sugeridas para el caso Izzi:
# MAGIC * **FAIL:** `src_addr IS NOT NULL AND dst_addr IS NOT NULL` — sin IPs no hay flujo válido
# MAGIC * **DROP:** `protocol IS NOT NULL`, `device_name IS NOT NULL`, filas completamente NULL (filas fantasma de archivos corruptos)
# MAGIC * **WARN:** `in_bytes >= 0`, `sample_rate > 0` — valores negativos o cero no invalidan, pero se monitorean
# MAGIC
# MAGIC > 💡 `flow_id` NO existe en los datos reales de Kentik. La deduplicación usa clave compuesta:
# MAGIC > `(src_addr, dst_addr, l4_src_port, l4_dst_port, protocol, timestamp)`.

# COMMAND ----------

# DBTITLE 1,Concepto: SCD2 con apply_changes (AUTO CDC)
# MAGIC %md
# MAGIC ## 3. SCD2 con `apply_changes` — el entregable clave
# MAGIC
# MAGIC El reto de Izzi: el **mapeo dispositivo ⇄ site/zona cambia con el tiempo** (reclasificaciones,
# MAGIC migraciones de infraestructura). Necesitamos un historial versionado:
# MAGIC *¿qué dispositivo estaba en qué zona en qué momento?*
# MAGIC
# MAGIC `dlt.apply_changes()` implementa **AUTO CDC** (Change Data Capture):
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="gold_device_site_map",
# MAGIC     source="device_site_changes",       # vista con cambios detectados de device → site
# MAGIC     keys=["device_name"],               # clave natural: el dispositivo
# MAGIC     sequence_by=F.col("event_ts"),       # ordena cronológicamente
# MAGIC     stored_as_scd_type=2,               # genera __START_AT / __END_AT
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **¿Qué pasa internamente?**
# MAGIC * Cuando un dispositivo aparece con un site nuevo → abre un registro con `__START_AT = event_ts`, `__END_AT = NULL`.
# MAGIC * Si el site cambia (reclasificación) → cierra la versión anterior (`__END_AT`) y abre una nueva.
# MAGIC * Resultado: historial completo dispositivo ⇄ site con vigencia temporal.

# COMMAND ----------

# DBTITLE 1,Instrucciones para generar la pipeline
# MAGIC %md
# MAGIC ## 4. Actividad: genera tu pipeline con Genie Code
# MAGIC
# MAGIC ### Pasos:
# MAGIC 1. **Abre un notebook nuevo** (File → New → Notebook) con lenguaje Python.
# MAGIC 2. **Abre Genie Code** (ícono de chat a la derecha, o `Cmd+I` / `Ctrl+I`).
# MAGIC 3. **Copia y pega el prompt** de la celda de abajo (ya tiene tus variables de catálogo/esquema/volumen).
# MAGIC 4. Genie Code generará el código de la Declarative Pipeline con las 4 tablas.
# MAGIC 5. **Revisa** que incluya: expectations (WARN/DROP/FAIL), apply_changes SCD2, Liquid Clustering.
# MAGIC 6. **No lo ejecutes** — es código de pipeline, no interactivo.
# MAGIC
# MAGIC ### Crear y ejecutar la pipeline:
# MAGIC 1. Menú **Jobs & Pipelines → Create → ETL Pipeline**.
# MAGIC 2. **Pipeline name:** usa el nombre que aparece abajo (`pipeline_izzi_<tu_alias>`).
# MAGIC 3. **Source code:** tu notebook recién generado.
# MAGIC 4. **Destino:** catálogo y **tu esquema personal** (se muestra abajo).
# MAGIC 5. **Serverless** ✔ · Pipeline mode: **Triggered**.
# MAGIC 6. (Opcional) Advanced → Configuration: `izzi.source_path` = ruta de tu Volume si difiere del default.
# MAGIC 7. **Start**. Verás el DAG bronze→silver→gold y las métricas de expectations por tabla.

# COMMAND ----------

# DBTITLE 1,Prompt para Genie Code
# Ejecuta esta celda para generar el prompt con tus variables del workshop.
# Luego cópialo y pégalo en Genie Code dentro de un notebook nuevo.

user_alias = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")
pipeline_name = f"pipeline_izzi_{user_alias}"

prompt = f"""
Genera un Declarative Pipeline llamado `{pipeline_name}` en Python con la siguiente informacion :
Agregar un comentario al inicio del notebook: # Pipeline: {pipeline_name} | Autor: {user_alias}

Utiliza el catalogo {CATALOG}

y el esquema {WORK_SCHEMA}


## Datos de origen
- Carpeta de JSON-lines: `{DATA_PATH}`
- Cada archivo tiene flujos de red Kentik (KFlow) con ~66 campos top-level.
  Campos principales: timestamp (epoch LONG), protocol, src_addr, dst_addr,
  l4_src_port, l4_dst_port, in_bytes, in_pkts, out_bytes, out_pkts,
  src_as, dst_as, src_geo, dst_geo, device_name, device_id, sample_rate, eventType, provider.
  Campos de negocio dentro de `custom_str` (MAP): device_site, device_site_market,
  application, src_as_name, dst_as_name, service_provider, service_type, src_connect_type,
  dst_connect_type, SamplerAddress.
- La ruta debe ser configurable vía spark.conf.get("izzi.source_path", "{DATA_PATH}")
- IMPORTANTE: Usar **esquema explícito** (no inferColumnTypes) para:
  1. Evitar errores por caracteres especiales en custom_bigint/custom_int/custom_str (forzar MAP)
  2. Ganar ~10% de rendimiento al evitar el paso de inferencia
  Definir un StructType con los campos clave + MAP types:
  ```python
  from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
  SCHEMA = StructType([
      StructField("timestamp", LongType()),
      StructField("protocol", StringType()),
      StructField("src_addr", StringType()),
      StructField("dst_addr", StringType()),
      StructField("l4_src_port", LongType()),
      StructField("l4_dst_port", LongType()),
      StructField("in_bytes", LongType()),
      StructField("in_pkts", LongType()),
      StructField("out_bytes", LongType()),
      StructField("out_pkts", LongType()),
      StructField("src_as", LongType()),
      StructField("dst_as", LongType()),
      StructField("src_geo", StringType()),
      StructField("dst_geo", StringType()),
      StructField("device_name", StringType()),
      StructField("device_id", LongType()),
      StructField("sample_rate", LongType()),
      StructField("eventType", StringType()),
      StructField("provider", StringType()),
      StructField("custom_str", MapType(StringType(), StringType())),
      StructField("custom_int", MapType(StringType(), LongType())),
      StructField("custom_bigint", MapType(StringType(), LongType())),
  ])
  ```

## Proteccion contra datos corruptos
- Usar `badRecordsPath` en Bronze para capturar lineas no-JSON en ruta de auditoria
  (sin badRecordsPath, lineas corruptas entran como filas con TODOS los campos NULL).
  Ruta configurable: spark.conf.get("izzi.bad_records_path", "/tmp/izzi_bad_records")
- Usar `rescuedDataColumn = "_rescued_data"` para capturar columnas extra no definidas en el esquema
- En Silver, agregar expectation DROP para filas fantasma (todos los campos clave NULL)

## Tablas a crear (arquitectura medallion)

### BRONZE: `bronze_network_flows`
- Streaming table con Auto Loader (cloudFiles) leyendo JSONL
- Usar `.schema(SCHEMA)` (el StructType definido arriba, NO inferColumnTypes)
- `rescuedDataColumn = "_rescued_data"` para schema mismatches
- `badRecordsPath` para capturar archivos corruptos en ruta de auditoria
- Columnas de linaje: _ingest_timestamp (timestamp actual) y _source_file (_metadata.file_path)

### SILVER: `silver_network_flows`
- Lee de bronze como stream
- Convertir timestamp (epoch LONG) a timestamp real como event_ts
- Derivar event_date como DATE desde event_ts
- Castear tipos explícitos: in_bytes/in_pkts/out_bytes/out_pkts a long
- Deduplicar por (src_addr, dst_addr, l4_src_port, l4_dst_port, protocol, timestamp)
  con watermark de 2 horas sobre event_ts
- Liquid Clustering por: device_name, protocol, event_date
- Expectations (usar los 3 modos):
  * FAIL: src_addr IS NOT NULL AND dst_addr IS NOT NULL
  * DROP (expect_all_or_drop): protocol IS NOT NULL, device_name IS NOT NULL,
    NOT (src_addr IS NULL AND dst_addr IS NULL AND protocol IS NULL) -- anti-phantom: filtra filas fantasma de archivos corruptos
  * WARN: in_bytes >= 0, sample_rate > 0

### GOLD 1: `gold_device_site_map` — ENTREGABLE PRINCIPAL
- SCD2 con dlt.apply_changes (AUTO CDC)
- Vista intermedia device_site_changes que detecta cambios de device_name → custom_str.device_site / custom_str.device_site_market
- Key: device_name
- Sequence by: event_ts
- stored_as_scd_type=2 (genera __START_AT / __END_AT)
- Liquid Clustering por: device_name, device_site_market

### GOLD 2: `gold_traffic_summary`
- Tráfico agregado por dispositivo, protocolo, geo de origen y ventana de 1 hora (F.window)
- Agregados: sum(in_bytes), sum(in_pkts), sum(out_bytes), sum(out_pkts),
  countDistinct(src_addr) (IPs origen), countDistinct(dst_addr) (IPs destino), count(*) flujos
- Liquid Clustering por: device_name, window_start

## Requisitos adicionales
- Todo en un solo notebook con import dlt y from pyspark.sql import functions as F
- Cada tabla debe tener comment descriptivo y table_properties con quality = bronze/silver/gold
- Usar delta.dataSkippingNumIndexedCols = 40 en silver (muchas columnas)
- NOTA: flow_id NO existe en los datos reales de Kentik. La deduplicacion usa clave compuesta
"""

print(prompt)