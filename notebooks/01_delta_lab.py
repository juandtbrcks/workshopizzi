# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 01 · Tablas Delta — fundamentos (Día 1)
# MAGIC
# MAGIC **Objetivo:** entender Delta Lake creando tu primera tabla a partir de flujos de red reales
# MAGIC (Kentik/KFlow), y ver las capacidades que la diferencian de un archivo Parquet: transacciones ACID,
# MAGIC `MERGE`, *time travel* y metadatos con `DESCRIBE`.
# MAGIC
# MAGIC Al terminar sabrás: leer JSON, escribir una tabla Delta gestionada por Unity Catalog,
# MAGIC actualizar con `MERGE`, y consultar versiones históricas.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer los eventos JSON crudos
# MAGIC Los datos son **flujos de red Kentik (JSONL)** — 1.5M+ registros reales.
# MAGIC Para este lab tomamos una muestra de ~4K registros para explorar la estructura.

# COMMAND ----------

# Leer una muestra de flujos (columnas limpias, extrae campos útiles de custom_str)
df_raw = read_clean(limit=4000)
print(f"Muestra de flujos: {df_raw.count():,}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Observa el esquema **plano** con 35 campos de flujos IP: direcciones, puertos, protocolo,
# MAGIC bytes/paquetes, geolocalización, dispositivo de red, ASN, etc. (25 top-level + 10 extraídos de `custom_str`).
# MAGIC Veamos algunos campos clave:

# COMMAND ----------

(df_raw
 .select("device_name", "protocol",
         "src_addr", "dst_addr",
         "in_bytes", "src_geo", "device_site_market")
 .show(5, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear una tabla Delta gestionada
# MAGIC Escribimos una muestra de flujos como tabla Delta en **tu esquema de trabajo**. Al ser gestionada
# MAGIC por Unity Catalog, su ubicación física la administra Databricks dentro del bucket configurado.

# COMMAND ----------

df_all = read_clean(limit=28000)  # muestra de 28K flujos
(df_all.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(tbl("delta_flujos")))

print(f"Tabla creada: {tbl('delta_flujos')}")
spark.sql(f"SELECT COUNT(*) AS flujos FROM {tbl('delta_flujos')}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `DESCRIBE DETAIL` — metadatos físicos
# MAGIC Número de archivos, tamaño, formato. Útil para diagnosticar *small files* y compresión.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_flujos')}"))

# COMMAND ----------

# DBTITLE 1,MERGE contexto
# MAGIC %md
# MAGIC ## 4. Transacciones: `MERGE` (upsert)
# MAGIC Simulamos una corrección: actualizar el `device_site_market` de un dispositivo (p. ej. una reclasificación de zona).
# MAGIC `MERGE` aplica cambios de forma atómica (ACID) — algo imposible con Parquet plano.
# MAGIC
# MAGIC Primero, un dispositivo con muchos flujos:

# COMMAND ----------

sample_dev = spark.sql(f"""
  SELECT device_name, device_site_market, COUNT(*) c
  FROM {tbl('delta_flujos')} GROUP BY 1, 2 ORDER BY c DESC LIMIT 1
""").collect()[0]
print(f"Dispositivo de ejemplo: {sample_dev['device_name']} (zona: {sample_dev['device_site_market']})")

# COMMAND ----------

# DBTITLE 1,MERGE con named_struct
# MERGE: reclasificar el device_site_market del dispositivo de ejemplo
new_zone = "Zona Reclasificada"
spark.sql(f"""
  MERGE INTO {tbl('delta_flujos')} AS t
  USING (SELECT '{sample_dev["device_name"]}' AS dev_name) AS u
  ON t.device_name = u.dev_name
  WHEN MATCHED THEN UPDATE SET t.device_site_market = '{new_zone}'
""")

# Verificar
spark.sql(f"""
  SELECT DISTINCT device_site_market
  FROM {tbl('delta_flujos')}
  WHERE device_name = '{sample_dev["device_name"]}'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Time Travel — historial de versiones
# MAGIC Cada operación crea una versión. Puedes auditar y consultar el pasado.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl('delta_flujos')}"))

# COMMAND ----------

# La versión 0 (antes del MERGE) aún tiene la zona original:
before = spark.sql(f"""
  SELECT DISTINCT device_site_market
  FROM {tbl('delta_flujos')} VERSION AS OF 0
  WHERE device_name = '{sample_dev["device_name"]}'
""")
print("Zona ANTES del MERGE (versión 0):")
before.show()