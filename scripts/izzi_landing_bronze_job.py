# Databricks notebook source
# MAGIC %md
# MAGIC # Izzi · Landing (arreglo JSON → JSONL) + Auto Loader → Bronce
# MAGIC
# MAGIC Job de 2 pasos, parametrizable:
# MAGIC 1. **Landing**: convierte cada archivo (arreglo JSON en 1 línea, ~GB) a **JSONL** en streaming
# MAGIC    (bajo consumo de memoria), en **paralelo** (un task Spark por archivo). Idempotente: salta los ya convertidos.
# MAGIC 2. **Bronce**: **Auto Loader** (`cloudFiles`) ingiere el JSONL de forma incremental y splittable → tabla Delta.

# COMMAND ----------

dbutils.widgets.text("source_path", "/Volumes/jgworkspaceclassic_catalog/prueba_izzi/jsonscliente", "1. Carpeta con arreglos JSON")
dbutils.widgets.text("landing_path", "/Volumes/jgworkspaceclassic_catalog/prueba_izzi/landing_jsonl", "2. Landing JSONL (Volume)")
dbutils.widgets.text("bronze_table", "jgworkspaceclassic_catalog.prueba_izzi.bronze_netflow", "3. Tabla bronce")
dbutils.widgets.text("checkpoint_path", "/Volumes/jgworkspaceclassic_catalog/prueba_izzi/landing_jsonl/_chk", "4. Checkpoint")

SRC   = dbutils.widgets.get("source_path").rstrip("/")
LAND  = dbutils.widgets.get("landing_path").rstrip("/")
BRONZE= dbutils.widgets.get("bronze_table")
CHK   = dbutils.widgets.get("checkpoint_path").rstrip("/")
print(f"SRC={SRC}\nLAND={LAND}\nBRONZE={BRONZE}\nCHK={CHK}")

# COMMAND ----------

# MAGIC %md ## Convertidor streaming array→JSONL (stdlib, bajo consumo de memoria)

# COMMAND ----------

def array_to_jsonl(src, dst, chunk=8 << 20):
    """Convierte un arreglo JSON grande (en 1 línea) a JSONL, en streaming.
    Asume que el literal '},{' NO aparece dentro de strings (cierto en NetFlow plano)."""
    DELIM = b'},{'; REP = b'}\n{'
    carry = b''; started = False
    with open(src, 'rb') as f, open(dst, 'wb') as out:
        while True:
            cd = f.read(chunk)
            data = carry + cd
            if not started:
                data = data.lstrip()
                if data[:1] == b'[':
                    data = data[1:]
                started = True
            if cd:
                data = data.replace(DELIM, REP)
                carry = data[-2:]; out.write(data[:-2])
            else:
                data = data.replace(DELIM, REP).rstrip()
                if data.endswith(b']'):
                    data = data[:-1].rstrip()
                out.write(data)
                break

# COMMAND ----------

# MAGIC %md ## Paso 1 — Landing: convertir en paralelo (1 task por archivo, salta los ya hechos)

# COMMAND ----------

import os

# asegurar Volume de landing
if LAND.startswith("/Volumes/"):
    parts = LAND.strip("/").split("/")  # ['Volumes', cat, schema, vol, ...]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[1]}`.`{parts[2]}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{parts[1]}`.`{parts[2]}`.`{parts[3]}`")

def _local(p):  # dbfs:/Volumes/... -> /Volumes/...
    return p[5:] if p.startswith("dbfs:") else p

# listar archivos fuente (excluye subcarpetas y cualquier carpeta *_jsonl)
files = [_local(f.path) for f in dbutils.fs.ls(SRC)
         if not f.isDir() and not f.path.rstrip("/").endswith("_jsonl")]

# pares (src, dst) que faltan por convertir (idempotencia)
existing = set()
try:
    existing = {_local(f.path) for f in dbutils.fs.ls(LAND)}
except Exception:
    pass
todo = [(s, f"{LAND}/{os.path.basename(s)}.jsonl") for s in files
        if f"{LAND}/{os.path.basename(s)}.jsonl" not in existing]

print(f"Archivos fuente: {len(files)} | por convertir: {len(todo)}")

if todo:
    # Distribuido y compatible con serverless: 1 archivo por partición vía mapInPandas
    def _convert(iterator):
        import os as _os, pandas as _pd
        def _a2j(src, dst, chunk=8 << 20):
            DELIM = b'},{'; REP = b'}\n{'
            carry = b''; started = False
            with open(src, 'rb') as f, open(dst, 'wb') as out:
                while True:
                    cd = f.read(chunk)
                    data = carry + cd
                    if not started:
                        data = data.lstrip()
                        if data[:1] == b'[':
                            data = data[1:]
                        started = True
                    if cd:
                        data = data.replace(DELIM, REP)
                        carry = data[-2:]; out.write(data[:-2])
                    else:
                        data = data.replace(DELIM, REP).rstrip()
                        if data.endswith(b']'):
                            data = data[:-1].rstrip()
                        out.write(data)
                        break
        for pdf in iterator:
            rows = []
            for s, d in zip(pdf["src"], pdf["dst"]):
                _a2j(s, d)
                rows.append((s, d, _os.path.getsize(d)))
            yield _pd.DataFrame(rows, columns=["src", "dst", "bytes"])

    pairs = spark.createDataFrame(todo, "src string, dst string").repartition(len(todo))
    res = pairs.mapInPandas(_convert, "src string, dst string, bytes long").collect()
    for r in res:
        print(f"  {r['dst']}  ({r['bytes']/1e9:.2f} GB)")
else:
    print("  Nada nuevo que convertir.")

# COMMAND ----------

# MAGIC %md ## Paso 2 — Auto Loader: JSONL → bronce (incremental)

# COMMAND ----------

# Bronce = JSON CRUDO como texto (1 registro por fila) + linaje.
# Por qué texto y no esquema inferido: los datos del cliente tienen nombres de campo inválidos
# para Delta (p.ej. 'packets/s_in_(broadcast)' con '/', '(', ')'). Landear crudo nunca falla;
# el parseo/tipado con esquema saneado se hace en la capa PLATA (from_json).
from pyspark.sql import functions as F

(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "text")
   .option("cloudFiles.schemaLocation", f"{CHK}/schema")
   .load(LAND)
   .select(
       F.col("value").alias("raw_json"),
       F.col("_metadata.file_name").alias("source_file"),
       F.col("_metadata.file_path").alias("source_path"),
       F.current_timestamp().alias("ingest_ts"))
 .writeStream
   .option("checkpointLocation", f"{CHK}/stream")
   .trigger(availableNow=True)
   .toTable(BRONZE))

for s in spark.streams.active:
    s.awaitTermination()

n = spark.table(BRONZE).count()
print(f"Bronce {BRONZE}: {n:,} filas (JSON crudo como texto)")
dbutils.notebook.exit(f"OK bronce={n}")
