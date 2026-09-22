# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 02 · Auto Loader — ingesta incremental (Día 1) 
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) ingiere archivos nuevos de un almacenamiento de objetos de forma
# MAGIC **incremental y con estado**: recuerda qué archivos ya procesó, así que en cada corrida solo lee lo nuevo.
# MAGIC Es la base recomendada para la capa **bronce** a escala (en Izzi: TB/día de JSON).
# MAGIC
# MAGIC En este lab verás:
# MAGIC 1. Ingesta incremental con `trigger(availableNow=True)`.
# MAGIC 2. **Backfill vs incremental** (procesar histórico vs solo lo nuevo).
# MAGIC 3. `cloud_files_state()` — inspeccionar qué archivos ha descubierto el checkpoint.
# MAGIC 4. Columnas de **linaje** (`_metadata`) para trazabilidad en producción.
# MAGIC 5. `file notification` vs `directory listing` + **throttling** (`maxFilesPerTrigger`, `maxBytesPerTrigger`).
# MAGIC 6. Inferencia de esquema vs **esquema explícito** (~10% más rápido).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Primera ingesta incremental
# MAGIC Leemos los JSON del Volume con `cloudFiles` y escribimos a una tabla bronce.
# MAGIC `availableNow=True` procesa todo lo disponible y **se detiene** (ideal para batch/incremental programado).
# MAGIC `_rescued_data` captura columnas que no calzan con el esquema (nunca pierdes datos).

# COMMAND ----------

bronze = tbl("bronze_autoloader")
chk = f"{CHK_PATH}/bronze_autoloader"
schema_loc = f"{CHK_PATH}/bronze_autoloader_schema"

# limpieza para poder re-ejecutar el lab desde cero
spark.sql(f"DROP TABLE IF EXISTS {bronze}")
dbutils.fs.rm(chk, True); dbutils.fs.rm(schema_loc, True)

(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", schema_loc)
   .option("cloudFiles.inferColumnTypes", "true")
   .option("rescuedDataColumn", "_rescued_data")
   .load(RAW_PATH)
 .writeStream
   .option("checkpointLocation", chk)
   .trigger(availableNow=True)
   .toTable(bronze))

# esperar a que el micro-batch termine
for s in spark.streams.active:
    s.awaitTermination()

print(f"Bronce: {spark.table(bronze).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Re-ejecutar = 0 filas nuevas (estado incremental)
# MAGIC Volvemos a correr el MISMO stream. Auto Loader recuerda los archivos ya vistos vía el checkpoint,
# MAGIC así que **no reprocesa nada**. Ese es el corazón de la ingesta incremental.

# COMMAND ----------

before = spark.table(bronze).count()

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", schema_loc)
   .option("cloudFiles.inferColumnTypes", "true")
   .option("rescuedDataColumn", "_rescued_data")
   .load(RAW_PATH)
 .writeStream.option("checkpointLocation", chk).trigger(availableNow=True).toTable(bronze))
for s in spark.streams.active: s.awaitTermination()

print(f"Antes: {before:,}  |  Después: {spark.table(bronze).count():,}  (sin cambios = incremental OK)")

# COMMAND ----------

# DBTITLE 1,Inspeccionar checkpoint con cloud_files_state()
# MAGIC %md
# MAGIC ### 2b. Inspeccionar el checkpoint con `cloud_files_state()`
# MAGIC Auto Loader registra cada archivo descubierto en su checkpoint (RocksDB). La función SQL
# MAGIC `cloud_files_state()` permite consultar ese estado: qué archivos se procesaron, cuándo se
# MAGIC descubrieron y su tamaño. Es clave para **auditoría** y **troubleshooting** en producción.

# COMMAND ----------

# DBTITLE 1,cloud_files_state query
df_state = spark.sql(f"""
    SELECT path, size, create_time, discovery_time, commit_time
    FROM cloud_files_state('{chk}')
    ORDER BY discovery_time DESC
""")

print(f"Archivos registrados en el checkpoint: {df_state.count()}")
display(df_state)

# COMMAND ----------

# DBTITLE 1,Metadata de ingesta para trazabilidad
# MAGIC %md
# MAGIC ### 2c. Columnas de linaje con `_metadata` — trazabilidad en producción
# MAGIC Auto Loader expone el struct `_metadata` con información del archivo fuente. En producción
# MAGIC (Izzi: TB/día) **siempre** incluir al menos `file_path` y `file_modification_time` en bronze
# MAGIC para saber de qué archivo vino cada registro.
# MAGIC
# MAGIC | Campo | Tipo | Uso |
# MAGIC |---|---|---|
# MAGIC | `_metadata.file_path` | STRING | Trazabilidad: de qué archivo vino |
# MAGIC | `_metadata.file_name` | STRING | Identificar el archivo sin ruta completa |
# MAGIC | `_metadata.file_size` | LONG | Detectar archivos vacíos o anómalos |
# MAGIC | `_metadata.file_modification_time` | TIMESTAMP | Detectar re-escrituras del origen |

# COMMAND ----------

# DBTITLE 1,_metadata ingestion demo
# Ingesta con columnas de linaje (sin stream, lectura directa para demo)
from pyspark.sql import functions as F

df_with_meta = (
    spark.read.format("json")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATH)
    .select(
        "event_id", "event_type", "plaza",
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_name").alias("source_file_name"),
        F.col("_metadata.file_size").alias("source_file_bytes"),
        F.col("_metadata.file_modification_time").alias("source_modified_at"),
        F.current_timestamp().alias("ingestion_ts"),
    )
)

print(f"Registros con metadata: {df_with_meta.count():,}")
print("\nDistribución por archivo fuente:")
display(
    df_with_meta
    .groupBy("source_file_name", "source_file_bytes")
    .agg(F.count("*").alias("eventos"), F.min("source_modified_at").alias("file_modified"))
    .orderBy("source_file_name")
)

# COMMAND ----------

# DBTITLE 1,Directory listing vs File notification + throttling
# MAGIC %md
# MAGIC ## 3. Directory listing vs File notification
# MAGIC - **Directory listing** (default): Auto Loader lista el directorio para detectar archivos nuevos.
# MAGIC   Simple, sin permisos extra; puede volverse lento con **millones** de archivos.
# MAGIC - **File notification** (`cloudFiles.useNotifications=true`): usa eventos del cloud (SNS/SQS en AWS)
# MAGIC   para enterarse de archivos nuevos sin listar. **Recomendado a escala** (caso Izzi: carpetas
# MAGIC   `anio/mes/dia/hora/min/seg` con enorme fan-out). Requiere permisos para crear la cola de notificaciones.
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.useNotifications", "true")   # requiere setup de SNS/SQS
# MAGIC ```
# MAGIC
# MAGIC ### Opciones de throttling (producción)
# MAGIC A escala TB/día conviene limitar cuánto procesa cada micro-batch para controlar costos y memoria:
# MAGIC
# MAGIC | Opción | Default | Efecto |
# MAGIC |---|---|---|
# MAGIC | `cloudFiles.maxFilesPerTrigger` | 1000 | Máx archivos por micro-batch |
# MAGIC | `cloudFiles.maxBytesPerTrigger` | (ilimitado) | Máx bytes por micro-batch |
# MAGIC | `cloudFiles.maxFileAge` | (ilimitado) | Ignora archivos más viejos que este umbral (mín 14 días si se activa) |
# MAGIC
# MAGIC En Izzi, con archivos de 2 GB, fijar `maxBytesPerTrigger` evita OOM en el driver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ⚠️ Gotcha: JSON multilínea NO es *splittable*
# MAGIC Cuando se trabaja con **arrays JSON multilínea** (un archivo = un `[ {...}, {...} ]`),
# MAGIC no JSON-lines. Eso obliga a `multiLine=true`, y **cada archivo se procesa por un solo core**
# MAGIC (no se puede partir) → cuello de botella con archivos de 2 GB.
# MAGIC
# MAGIC Primero, leer el array multilínea SIN la opción falla o produce basura:

# COMMAND ----------

# Sin multiLine: intenta parsear cada LÍNEA como un JSON -> filas corruptas / _corrupt_record
df_bad = spark.read.json(MULTILINE_PATH)
print("Sin multiLine → filas:", df_bad.count(), "| columnas:", df_bad.columns[:5])

# COMMAND ----------

# DBTITLE 1,multiLine=true correcto
# Con multiLine=true: parsea el array completo correctamente
from pyspark.sql import functions as F
df_ok = spark.read.option("multiLine", "true").json(MULTILINE_PATH)
print("Con multiLine=true → filas:", df_ok.count())
df_ok.select("event_id", "event_type", F.col("network.client_ip")).show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mitigación en producción:** pedir al origen **JSON-lines (JSONL)** o añadir un paso de *landing*
# MAGIC que explote el array en registros — recupera el paralelismo y baja el costo por TB.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Esquema explícito (≈10% más rápido)
# MAGIC Cuando el esquema es **fijo** , declararlo evita el paso de inferencia y acelera.
# MAGIC Con Auto Loader se pasa vía `.schema(...)` en lugar de `inferColumnTypes`.

# COMMAND ----------

from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType, IntegerType)

net = StructType([StructField("client_ip", StringType()), StructField("public_ip", StringType()),
    StructField("mac_address", StringType()), StructField("cmts_name", StringType()),
    StructField("nas_ip", StringType()), StructField("dhcp_server", StringType()),
    StructField("vlan", LongType()), StructField("lease_seconds", LongType())])
equip = StructType([StructField("modem_mac", StringType()), StructField("serial", StringType()),
    StructField("model", StringType()), StructField("firmware", StringType()), StructField("device_type", StringType())])
subsc = StructType([StructField("subscriber_id", StringType()), StructField("plan", StringType()), StructField("account_status", StringType())])

EXPLICIT = StructType([
    StructField("event_id", StringType()), StructField("event_timestamp", StringType()),
    StructField("event_type", StringType()), StructField("ingest_source", StringType()),
    StructField("plaza", StringType()),
    StructField("network", net), StructField("equipment", equip), StructField("subscriber", subsc),
    StructField("event_date", StringType()),   # ← incluir columna de partición
])

df_expl = (spark.read.format("json").schema(EXPLICIT).option("multiLine", "false").load(RAW_PATH))
print("Con esquema explícito → filas:", df_expl.count())
df_expl.printSchema()

# COMMAND ----------

# DBTITLE 1,Schema evolution intro
# MAGIC %md
# MAGIC ## 6. Schema evolution — cuando llega un esquema nuevo ⭐
# MAGIC En la vida real el origen agrega campos sin avisar. Auto Loader ofrece 4 modos para manejarlo
# MAGIC vía `cloudFiles.schemaEvolutionMode`:
# MAGIC
# MAGIC | Modo | ¿Falla? | Comportamiento | Cuándo usarlo |
# MAGIC |---|---|---|---|
# MAGIC | **`addNewColumns`** | Sí (1 vez) | Falla al ver la columna nueva; al **reiniciar** la agrega al schema automáticamente | Producción con Jobs (el reintento es transparente) |
# MAGIC | **`rescue`** | No | Columnas nuevas van a `_rescued_data` como JSON | Nunca perder datos; analizar los campos nuevos después |
# MAGIC | **`failOnNewColumns`** | Sí | Falla y **no** agrega la columna; requiere intervención manual | Control estricto: revisar antes de aceptar cambios |
# MAGIC | **`none`** | No | Ignora columnas nuevas silenciosamente | Esquema fijo (caso Izzi en producción) |
# MAGIC
# MAGIC La carpeta **`raw_jsonl_evolucion/`** tiene un archivo con **2 columnas nuevas** (`collector_version`,
# MAGIC `latency_ms`) que NO existen en los datos base. Veamos cómo funciona el modo `rescue`.
# MAGIC
# MAGIC Simulamos la llegada de archivos a **tu** carpeta de aterrizaje (writable): primero el esquema base,
# MAGIC luego el archivo evolucionado.

# COMMAND ----------

# comparar los esquemas de las dos carpetas
base_cols = set(spark.read.json(f"{RAW_PATH}/event_date=2026-08-10").columns)
evol_cols = set(spark.read.json(EVOLUTION_PATH).columns)
print("Columnas NUEVAS en la carpeta de evolución:", sorted(evol_cols - base_cols))

# COMMAND ----------

# preparar carpeta de aterrizaje personal (plana) y checkpoint
landing = f"{CHK_PATH}/landing_evol"
chk_e = f"{CHK_PATH}/evol_stream"; sloc_e = f"{CHK_PATH}/evol_schema"
for p in (landing, chk_e, sloc_e): dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing)
evol_bronze = tbl("bronze_evol")
spark.sql(f"DROP TABLE IF EXISTS {evol_bronze}")

# 1er archivo: esquema BASE
dbutils.fs.cp(f"{RAW_PATH}/event_date=2026-08-10/part-000.json", f"{landing}/lote_1_base.json")

def run_autoloader():
    (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", sloc_e)
       .option("cloudFiles.inferColumnTypes", "true")
       .option("cloudFiles.schemaEvolutionMode", "rescue")   # campos nuevos → _rescued_data (no falla)
       .option("rescuedDataColumn", "_rescued_data")
       .load(landing)
     .writeStream.option("checkpointLocation", chk_e).trigger(availableNow=True).toTable(evol_bronze))
    for s in spark.streams.active: s.awaitTermination()

run_autoloader()
print("Tras lote BASE →", spark.table(evol_bronze).count(), "filas | columnas:", "collector_version" in spark.table(evol_bronze).columns)

# COMMAND ----------

# 2do archivo: esquema EVOLUCIONADO (con collector_version y latency_ms)
dbutils.fs.cp(f"{EVOLUTION_PATH}/events_v2.json", f"{landing}/lote_2_evol.json")
run_autoloader()

# En modo 'rescue', los campos nuevos NO rompen el stream: se guardan en _rescued_data
from pyspark.sql import functions as F
print("Total tras lote EVOLUCIONADO:", spark.table(evol_bronze).count())
(spark.table(evol_bronze)
   .where("_rescued_data IS NOT NULL")
   .select("event_id", "_rescued_data").show(3, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué pasó?** Con `schemaEvolutionMode="rescue"` los campos nuevos (`collector_version`, `latency_ms`)
# MAGIC se capturan en `_rescued_data` (nunca se pierden), sin detener el stream.
# MAGIC
# MAGIC Otros modos de `cloudFiles.schemaEvolutionMode`:
# MAGIC - **`addNewColumns`** (default con `schemaLocation`): el stream **falla** con `UnknownFieldException`
# MAGIC   al ver la columna nueva; al **reiniciarlo** la agrega como columna real. Ideal cuando quieres que las
# MAGIC   columnas nuevas se materialicen automáticamente (en un Job programado, el reintento es transparente).
# MAGIC - **`rescue`**: lo que acabamos de ver — nuevas columnas van a `_rescued_data`, sin fallar.
# MAGIC - **`failOnNewColumns`** / **`none`**: falla / ignora, respectivamente.
# MAGIC
# MAGIC En Izzi el esquema es **fijo**, así que en producción se prefiere esquema explícito + `none`; pero
# MAGIC entender la evolución es clave para tolerar cambios inesperados del origen sin perder datos.