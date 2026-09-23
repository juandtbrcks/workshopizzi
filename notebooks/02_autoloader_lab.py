# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 02 · Auto Loader — ingesta incremental de flujos Kentik (Día 1) 
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) ingiere archivos nuevos de un almacenamiento de objetos de forma
# MAGIC **incremental y con estado**: recuerda qué archivos ya procesó, así que en cada corrida solo lee lo nuevo.
# MAGIC Es la base recomendada para la capa **bronce** a escala (en Izzi: TB/día de flujos Kentik).
# MAGIC
# MAGIC En este lab verás:
# MAGIC 1. Ingesta incremental con `trigger(availableNow=True)`.
# MAGIC 2. **Backfill vs incremental** (procesar histórico vs solo lo nuevo).
# MAGIC 3. `cloud_files_state()` — inspeccionar qué archivos ha descubierto el checkpoint.
# MAGIC 4. Columnas de **linaje** (`_metadata`) para trazabilidad en producción.
# MAGIC 5. `file notification` vs `directory listing` + **throttling** (`maxFilesPerTrigger`, `maxBytesPerTrigger`).
# MAGIC 6. **Archivos corruptos** — qué pasa cuando llega un archivo inválido.
# MAGIC 7. **Schema evolution** — qué pasa cuando el origen agrega campos nuevos.
# MAGIC
# MAGIC > 💡 Todos los Auto Loaders de este lab usan **esquema explícito** (`.schema(...)`).
# MAGIC > También existe `inferColumnTypes=true` que detecta tipos automáticamente, pero es
# MAGIC > ~10% más lento por el paso adicional de inferencia. En producción siempre esquema fijo.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Primera ingesta incremental
# MAGIC Leemos **todos** los JSONL preprocesados (1.5M+ flujos reales) con `cloudFiles` y escribimos a una tabla bronce.
# MAGIC `availableNow=True` procesa todo lo disponible y **se detiene** (ideal para batch/incremental programado).
# MAGIC `_rescued_data` captura columnas que no calzan con el esquema (nunca pierdes datos).
# MAGIC
# MAGIC > ⚠️ Los JSONL originales de Kentik contienen campos `custom_str`, `custom_int`, `custom_bigint`
# MAGIC > con nombres de campo que tienen caracteres especiales (`/`, `()`). El esquema explícito los
# MAGIC > declara como `MAP<STRING, ...>` en vez de `STRUCT`, evitando errores de nombres inválidos en Delta.

# COMMAND ----------

bronze = tbl("bronze_flujos")
chk = f"{CHK_PATH}/bronze_flujos"
schema_loc = f"{CHK_PATH}/bronze_flujos_schema"

# Esquema explícito: ~10% más rápido que inferencia 
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType

EXPLICIT = StructType([
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

# limpieza para poder re-ejecutar el lab desde cero
spark.sql(f"DROP TABLE IF EXISTS {bronze}")
dbutils.fs.rm(chk, True); 
dbutils.fs.rm(schema_loc, True)

q = (spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", schema_loc)
   .schema(EXPLICIT)
   .option("rescuedDataColumn", "_rescued_data")
   .load(DATA_PATH) 
 .writeStream
   .option("checkpointLocation", chk)
   .trigger(availableNow=True)
   .toTable(bronze))
q.awaitTermination()

print(f"Bronce: {spark.table(bronze).count():,} flujos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Re-ejecutar = 0 filas nuevas (estado incremental)
# MAGIC Volvemos a correr el MISMO stream. Auto Loader recuerda los archivos ya vistos vía el checkpoint,
# MAGIC así que **no reprocesa nada**. Ese es el corazón de la ingesta incremental.

# COMMAND ----------

before = spark.table(bronze).count()

q = (spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", schema_loc)
   .schema(EXPLICIT)
   .option("rescuedDataColumn", "_rescued_data")
   .load(DATA_PATH)
 .writeStream.option("checkpointLocation", chk).trigger(availableNow=True).toTable(bronze))
q.awaitTermination()

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
    SELECT path, size, create_time, discovery_time, processed_time
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
# MAGIC (Izzi: TB/día de flujos) **siempre** incluir al menos `file_path` y `file_modification_time` en bronze
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
    .schema(EXPLICIT)
    .load(DATA_PATH)
    .select(
        "protocol", "device_name",
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
# MAGIC   para enterarse de archivos nuevos sin listar. **Recomendado a escala** . Requiere permisos para crear la cola de notificaciones.
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

# COMMAND ----------

# DBTITLE 1,Archivos corruptos - teoría
# MAGIC %md
# MAGIC ## 4. Manejo de archivos corruptos en producción 
# MAGIC
# MAGIC En producción no todos los archivos que llegan son válidos. Causas comunes:
# MAGIC - **Archivos truncados** — transferencia interrumpida (Izzi: archivos de 2 GB via SFTP/S3)
# MAGIC - **Formato incorrecto** — texto plano con extensión `.json`, o CSV disfrazado
# MAGIC - **Codificación rota** — bytes inválidos que rompen el parser JSON
# MAGIC
# MAGIC ### Comportamiento según formato
# MAGIC - **Parquet / Avro**: archivos corruptos causan **fail-fast** (el header binario es requerido).
# MAGIC - **JSON (nuestro caso)**: cada línea se parsea independientemente. Líneas inválidas
# MAGIC   **no detienen el stream** — producen filas con TODOS los campos en NULL.
# MAGIC   Esto es peligroso: datos basura entran a bronze **sin alerta**.
# MAGIC
# MAGIC ### Estrategias de protección
# MAGIC
# MAGIC | Estrategia | Efecto | Cuándo usarla |
# MAGIC |---|---|---|
# MAGIC | `mode=PERMISSIVE` + `_corrupt_record` | Líneas inválidas van a una columna especial | Cuarentena a nivel de registro |
# MAGIC | `badRecordsPath` | Registra registros problemáticos en ruta de auditoría | Debugging y trazabilidad |
# MAGIC | `ignoreCorruptFiles=true` | Salta archivos completamente ilegibles (binarios, permisos) | Bronze tolerante a fallos |
# MAGIC | `_rescued_data` | Columnas extra/faltantes van a columna JSON | Schema mismatch sin perder nada |
# MAGIC
# MAGIC **Combinación recomendada para Bronze JSON a escala:**
# MAGIC ```python
# MAGIC .option("ignoreCorruptFiles", "true")
# MAGIC .option("badRecordsPath", "/ruta/auditoria")
# MAGIC .option("rescuedDataColumn", "_rescued_data")
# MAGIC ```
# MAGIC
# MAGIC Veamos qué pasa cuando un archivo con líneas no-JSON entra a la carpeta de landing.

# COMMAND ----------

# DBTITLE 1,Archivo corrupto: badRecordsPath al rescate
# Sandbox personal dentro de CHK_PATH (ya es unico por usuario/esquema).
# No toca DATA_PATH ni el checkpoint principal = cero interferencia.
landing_c = f"{CHK_PATH}/corrupt_demo"
chk_c     = f"{CHK_PATH}/corrupt_chk"
sloc_c    = f"{CHK_PATH}/corrupt_schema"
bad_c     = f"{CHK_PATH}/corrupt_bad"
for p in (landing_c, chk_c, sloc_c, bad_c): dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing_c)

corrupt_tbl = tbl("bronze_corrupt_demo")
spark.sql(f"DROP TABLE IF EXISTS {corrupt_tbl}")

# Crear archivo corrupto (texto plano con extension .jsonl)
with open(f"{landing_c}/archivo_corrupto.jsonl", "w") as f:
    f.write("ESTO NO ES JSON VALIDO\n" * 50)
print("Archivo corrupto creado (50 lineas de texto plano con extension .jsonl)")

# 3. Autoloader con badRecordsPath sobre el sandbox
q = (spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", sloc_c)
   .schema(EXPLICIT)
   .option("rescuedDataColumn", "_rescued_data")
   .option("badRecordsPath", bad_c)
   .load(landing_c)
 .writeStream.option("checkpointLocation", chk_c)
   .trigger(availableNow=True).toTable(corrupt_tbl))
q.awaitTermination()

total = spark.table(corrupt_tbl).count()
print(f"\n50 lineas corruptas -> {total} registros en tabla")

# COMMAND ----------

# DBTITLE 1,Limpieza: eliminar sandbox corrupto
# Eliminar sandbox personal completo (tabla + carpetas)
spark.sql(f"DROP TABLE IF EXISTS {corrupt_tbl}")
for p in (landing_c, chk_c, sloc_c, bad_c): dbutils.fs.rm(p, True)
print("Sandbox corrupto eliminado. Nada queda en el workspace.")

# COMMAND ----------

# DBTITLE 1,Schema evolution intro
# MAGIC %md
# MAGIC ## 5. Schema evolution — cuando llega un esquema nuevo 
# MAGIC  Auto Loader ofrece 4 modos para manejar cambios en los esquemas de los datos
# MAGIC
# MAGIC vía `cloudFiles.schemaEvolutionMode`:
# MAGIC
# MAGIC | Modo | ¿Falla? | Comportamiento | Cuándo usarlo |
# MAGIC |---|---|---|---|
# MAGIC | **`addNewColumns`** | Sí (1 vez) | Falla al ver la columna nueva; al **reiniciar** la agrega al schema automáticamente | Producción con Jobs (el reintento es transparente) |
# MAGIC | **`rescue`** | No | Columnas nuevas van a `_rescued_data` como JSON | Nunca perder datos; analizar los campos nuevos después |
# MAGIC | **`failOnNewColumns`** | Sí | Falla y **no** agrega la columna; requiere intervención manual | Control estricto: revisar antes de aceptar cambios |
# MAGIC | **`none`** | No | Ignora columnas nuevas silenciosamente | Esquema fijo (caso Izzi en producción) |
# MAGIC
# MAGIC La carpeta **`_evolucion/`** tiene un archivo con **2 columnas nuevas** (`collector_version`,
# MAGIC `latency_ms`) que NO existen en los flujos base. Veamos cómo funciona el modo `rescue`.
# MAGIC
# MAGIC Simulamos la llegada de archivos a **tu** carpeta de aterrizaje (writable): primero el esquema base,
# MAGIC luego el archivo evolucionado.

# COMMAND ----------

# DBTITLE 1,Comparar esquemas: base vs evolucionado
# Antes de simular: verificar qué columnas NUEVAS trae el archivo evolucionado
base_cols = set(spark.read.json(DATA_PATH).columns)
evol_cols = set(spark.read.json(EVOLUTION_PATH).columns)
print("Columnas NUEVAS en la carpeta de evolución:", sorted(evol_cols - base_cols))

# COMMAND ----------

# DBTITLE 1,Lote 1: ingesta con esquema base
# ── 1. Preparar sandbox limpio ──────────────────────────────────
# Carpeta temporal donde simularemos la llegada de archivos.
# Cada re-ejecución borra todo para empezar de cero.
landing = f"{CHK_PATH}/landing_evol"        # carpeta de aterrizaje
chk_e   = f"{CHK_PATH}/evol_stream"         # checkpoint del stream
sloc_e  = f"{CHK_PATH}/evol_schema"         # esquema inferido
for p in (landing, chk_e, sloc_e):
    dbutils.fs.rm(p, True)
dbutils.fs.mkdirs(landing)

evol_bronze = tbl("bronze_flujos_evol")
spark.sql(f"DROP TABLE IF EXISTS {evol_bronze}")

# ── 2. Copiar el PRIMER archivo (esquema base, sin columnas nuevas) ─
base_files = [f.path for f in dbutils.fs.ls(DATA_PATH) if f.name.endswith(".jsonl")]
dbutils.fs.cp(base_files[0], f"{landing}/lote_1_base.json")
print(f"Lote 1 copiado: {base_files[0].split('/')[-1]} → landing/lote_1_base.json")

# ── 3. Definir el Auto Loader con modo 'rescue' ────────────────
# NOTA: aquí usamos inferColumnTypes=true (NO esquema explícito).
# ¿Por qué? Para que Auto Loader DETECTE las columnas nuevas del
# archivo evolucionado. Con esquema fijo las ignoraría.
def run_autoloader():
    q = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", sloc_e)
       .option("cloudFiles.inferColumnTypes", "true")
       .option("cloudFiles.schemaEvolutionMode", "rescue")
       .option("rescuedDataColumn", "_rescued_data")
       .load(landing)
     .writeStream
       .option("checkpointLocation", chk_e)
       .trigger(availableNow=True)
       .toTable(evol_bronze))
    q.awaitTermination()

# ── 4. Ejecutar: solo el lote base ─────────────────────────────
run_autoloader()

count = spark.table(evol_bronze).count()
has_new = "collector_version" in spark.table(evol_bronze).columns
print(f"\nTras lote BASE → {count:,} filas")

# COMMAND ----------

# DBTITLE 1,Lote 2: llega archivo con columnas nuevas
# ── 5. Copiar el SEGUNDO archivo (esquema evolucionado: +2 columnas) ─
dbutils.fs.cp(f"{EVOLUTION_PATH}/flows_v2.json", f"{landing}/lote_2_evol.json")
print("Lote 2 copiado: flows_v2.json → landing/lote_2_evol.json")

# ── 6. Re-ejecutar el MISMO autoloader (incremental) ───────────
# Modo 'rescue': las columnas nuevas NO rompen el stream.
# Se guardan como JSON en _rescued_data para análisis posterior.
run_autoloader()

count = spark.table(evol_bronze).count()
rescued = spark.table(evol_bronze).where("_rescued_data IS NOT NULL").count()
print(f"Total filas: {count:,}  |  Filas con _rescued_data: {rescued}")
print(f"Las {rescued} filas del lote 2 tienen collector_version y latency_ms en _rescued_data:\n")

display(spark.table(evol_bronze)
   .where("_rescued_data IS NOT NULL")
   .select("device_name", "protocol", "src_addr", "_rescued_data")
   .limit(5))

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