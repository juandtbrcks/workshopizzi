# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Tablas Delta — fundamentos (Día 1)
# MAGIC
# MAGIC **Objetivo:** entender Delta Lake creando tu primera tabla a partir de los eventos de red,
# MAGIC y ver las capacidades que la diferencian de un archivo Parquet: transacciones ACID,
# MAGIC `MERGE`, *time travel* y metadatos con `DESCRIBE`.
# MAGIC
# MAGIC Al terminar sabrás: leer JSON, escribir una tabla Delta gestionada por Unity Catalog,
# MAGIC actualizar con `MERGE`, y consultar versiones históricas.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer los eventos JSON crudos
# MAGIC Los datos son JSON-lines particionados por `event_date`. Leemos un día para explorar la estructura.

# COMMAND ----------

df_raw = spark.read.json(f"{RAW_PATH}/event_date=2026-08-10")
print(f"Eventos del 2026-08-10: {df_raw.count():,}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Observa el esquema **anidado** (`network`, `equipment`, `subscriber`, ...). Delta y Spark
# MAGIC manejan structs nativamente. Veamos algunos campos aplanados con notación de punto:

# COMMAND ----------

from pyspark.sql import functions as F
(df_raw
 .select("event_id", "event_type", "plaza",
         F.col("network.client_ip").alias("client_ip"),
         F.col("equipment.model").alias("modelo"),
         F.col("subscriber.subscriber_id").alias("suscriptor"))
 .show(5, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear una tabla Delta gestionada
# MAGIC Escribimos TODOS los días como una tabla Delta en **tu esquema de trabajo**. Al ser gestionada
# MAGIC por Unity Catalog, su ubicación física la administra Databricks dentro del bucket configurado.

# COMMAND ----------

df_all = spark.read.json(RAW_PATH)   # lee las 7 particiones de fecha
(df_all.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(tbl("delta_eventos")))

print(f"Tabla creada: {tbl('delta_eventos')}")
spark.sql(f"SELECT COUNT(*) AS eventos FROM {tbl('delta_eventos')}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `DESCRIBE DETAIL` — metadatos físicos
# MAGIC Número de archivos, tamaño, formato. Útil para diagnosticar *small files* y compresión.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_eventos')}"))

# COMMAND ----------

# DBTITLE 1,MERGE contexto
# MAGIC %md
# MAGIC ## 4. Transacciones: `MERGE` (upsert)
# MAGIC Simulamos una corrección: marcar como `inactive` la cuenta de un suscriptor.
# MAGIC `MERGE` aplica cambios de forma atómica (ACID) — algo imposible con Parquet plano.
# MAGIC
# MAGIC > ⚠️ Delta no permite actualizar campos **dentro** de un struct anidado directamente.
# MAGIC > Usamos `named_struct(...)` para reconstruir el struct completo con el campo modificado.
# MAGIC
# MAGIC Primero, un suscriptor con eventos:

# COMMAND ----------

sample_sub = spark.sql(f"""
  SELECT subscriber.subscriber_id AS sid, COUNT(*) c
  FROM {tbl('delta_eventos')} GROUP BY 1 ORDER BY c DESC LIMIT 1
""").collect()[0][0]
print("Suscriptor de ejemplo:", sample_sub)

# COMMAND ----------

# DBTITLE 1,MERGE con named_struct
# Reconstruimos el struct 'subscriber' con named_struct para cambiar account_status
# (Delta no permite UPDATE directo sobre campos anidados dentro de un struct)
spark.sql(f"""
  MERGE INTO {tbl('delta_eventos')} AS t
  USING (SELECT '{sample_sub}' AS sid) AS u
  ON t.subscriber.subscriber_id = u.sid
  WHEN MATCHED THEN UPDATE SET
    t.subscriber = named_struct(
      'subscriber_id', t.subscriber.subscriber_id,
      'plan',          t.subscriber.plan,
      'account_status', 'inactive'
    )
""")

spark.sql(f"""
  SELECT DISTINCT subscriber.account_status
  FROM {tbl('delta_eventos')} WHERE subscriber.subscriber_id = '{sample_sub}'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time Travel — historial de versiones
# MAGIC Cada operación crea una versión. Puedes auditar y consultar el pasado.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl('delta_eventos')}"))

# COMMAND ----------

# La versión 0 (antes del MERGE) aún tiene el estado original:
before = spark.sql(f"""
  SELECT DISTINCT subscriber.account_status
  FROM {tbl('delta_eventos')} VERSION AS OF 0
  WHERE subscriber.subscriber_id = '{sample_sub}'
""")
before.show()