# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 05 · Optimización Delta — costo y rendimiento a escala (Día 2)
# MAGIC
# MAGIC A escala PB (el caso Izzi), las decisiones de *layout* y compresión dominan el costo. En este lab:
# MAGIC **Liquid Clustering**, `OPTIMIZE` / compaction, **file sizing**, compresión **ZSTD**, y
# MAGIC cómo medir todo con `DESCRIBE DETAIL`.
# MAGIC
# MAGIC > Requiere el lab **01** (usa tu tabla `delta_flujos`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Punto de partida: ¿cómo está tu tabla?
# MAGIC `DESCRIBE DETAIL` muestra número de archivos y tamaño. Muchos archivos pequeños = lecturas lentas.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_flujos')}")
        .select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# DBTITLE 1,Liquid Clustering explicacion
# MAGIC %md
# MAGIC ## 2. Liquid Clustering
# MAGIC Reemplaza al particionado rígido y al `ZORDER`. Agrupa físicamente los datos por las columnas que
# MAGIC más se filtran → menos archivos leídos (**data skipping**). En Izzi las consultas van por
# MAGIC `src_addr`, `device_name`, `protocol`, `src_geo`.
# MAGIC
# MAGIC > 💡 Abajo usamos `(device_site_market, protocol, src_geo)` como **ejemplo didáctico** para ver el efecto.
# MAGIC > En el ejercicio TODO te pedimos cambiarlo a las columnas reales de producción.
# MAGIC
# MAGIC ⚠️ *Gotcha*: el clustering necesita **estadísticas** en esas columnas. Deben estar entre las
# MAGIC primeras 32 columnas, o subir `delta.dataSkippingNumIndexedCols`.

# COMMAND ----------

spark.sql(f"ALTER TABLE {tbl('delta_flujos')} CLUSTER BY (device_site_market, protocol, src_geo)")
# materializa el clustering sobre los datos existentes
spark.sql(f"OPTIMIZE {tbl('delta_flujos')}")
display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_flujos')}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data skipping en acción
# MAGIC Con clustering por `plaza`, una consulta filtrada por plaza lee muchos menos archivos.
# MAGIC Usa `EXPLAIN` para ver `PartitionFilters` / `DataFilters` y el número de archivos leídos.

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {tbl('delta_flujos')} WHERE device_site_market = 'CORE_IZZI'").show()
print(spark.sql(f"EXPLAIN FORMATTED SELECT COUNT(*) FROM {tbl('delta_flujos')} WHERE device_site_market = 'CORE_IZZI'")
      .collect()[0][0][:1500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compresión ZSTD y file sizing
# MAGIC **ZSTD** es el códec recomendado (mejor ratio y velocidad que Snappy/GZIP). Se fija por tabla.
# MAGIC

# COMMAND ----------

spark.sql(f"""
  ALTER TABLE {tbl('delta_flujos')} SET TBLPROPERTIES (
    'delta.parquet.compression.codec' = 'zstd',
    'delta.targetFileSize' = '134217728'   -- 128 MB por archivo (buen tamaño para lectura)
  )
""")
spark.sql(f"OPTIMIZE {tbl('delta_flujos')}")
display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_flujos')}").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mantenimiento: VACUUM y compaction automática
# MAGIC - **Compaction automática:** `delta.autoOptimize.optimizeWrite=true` compacta al escribir (ideal para streaming).
# MAGIC - **VACUUM:** borra archivos viejos ya no referenciados (libera storage; respeta el retention del time travel).
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE <tabla> SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
# MAGIC VACUUM <tabla> RETAIN 168 HOURS;   -- 7 días (default)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reglas de dimensionamiento medidas en el PoC de Izzi (1 TB de JSON crudo)
# MAGIC | Capa | Tamaño / TB de JSON crudo | Nota |
# MAGIC |---|---|---|
# MAGIC | bronze | ~61 GB | compresión JSON→Delta ≈ **16×** |
# MAGIC | silver | ~64 GB | aplanado + Liquid Clustering + stats |
# MAGIC | gold_ip_equipment_map | ~11.5 GB | SCD2, escala con rotación de IP (no con eventos) |
# MAGIC
# MAGIC **Total lakehouse Delta ≈ 13.6% del JSON crudo.** Serverless con Photon salió ~2.5× más rápido
# MAGIC y con costo sub-lineal (~$20/TB a escala TB). El JSON crudo es zona **efímera** (retención corta).