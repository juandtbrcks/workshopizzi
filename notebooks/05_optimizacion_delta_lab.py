# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 05 · Optimización Delta — costo y rendimiento a escala (Día 2)
# MAGIC
# MAGIC A escala PB (el caso Izzi), las decisiones de *layout* y compresión dominan el costo. En este lab:
# MAGIC **Liquid Clustering**, `OPTIMIZE` / compaction, **file sizing**, compresión **ZSTD**, y
# MAGIC cómo medir todo con `DESCRIBE DETAIL`.
# MAGIC
# MAGIC > Requiere el lab **01** (usa tu tabla `delta_eventos`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Punto de partida: ¿cómo está tu tabla?
# MAGIC `DESCRIBE DETAIL` muestra número de archivos y tamaño. Muchos archivos pequeños = lecturas lentas.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_eventos')}")
        .select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# DBTITLE 1,Liquid Clustering explicacion
# MAGIC %md
# MAGIC ## 2. Liquid Clustering
# MAGIC Reemplaza al particionado rígido y al `ZORDER`. Agrupa físicamente los datos por las columnas que
# MAGIC más se filtran → menos archivos leídos (**data skipping**). En Izzi las consultas van por
# MAGIC `client_ip`, `mac_address`, `subscriber_id`, `event_date`.
# MAGIC
# MAGIC > 💡 Abajo usamos `(plaza, event_type, event_date)` como **ejemplo didáctico** para ver el efecto.
# MAGIC > En el ejercicio TODO te pedimos cambiarlo a las columnas reales de producción.
# MAGIC
# MAGIC ⚠️ *Gotcha*: el clustering necesita **estadísticas** en esas columnas. Deben estar entre las
# MAGIC primeras 32 columnas, o subir `delta.dataSkippingNumIndexedCols`.

# COMMAND ----------

spark.sql(f"ALTER TABLE {tbl('delta_eventos')} CLUSTER BY (plaza, event_type, event_date)")
# materializa el clustering sobre los datos existentes
spark.sql(f"OPTIMIZE {tbl('delta_eventos')}")
display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_eventos')}").select("numFiles", "sizeInBytes", "clusteringColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data skipping en acción
# MAGIC Con clustering por `plaza`, una consulta filtrada por plaza lee muchos menos archivos.
# MAGIC Usa `EXPLAIN` para ver `PartitionFilters` / `DataFilters` y el número de archivos leídos.

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) FROM {tbl('delta_eventos')} WHERE plaza = 'Monterrey'").show()
print(spark.sql(f"EXPLAIN FORMATTED SELECT COUNT(*) FROM {tbl('delta_eventos')} WHERE plaza = 'Monterrey'")
      .collect()[0][0][:1500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Compresión ZSTD y file sizing
# MAGIC **ZSTD** es el códec recomendado (mejor ratio y velocidad que Snappy/GZIP). Se fija por tabla.
# MAGIC
# MAGIC ⚠️ *Gotcha medido en Izzi (2026-09):* el **nivel** de ZSTD **NO es configurable** en el writer de
# MAGIC Databricks (ignora `parquet.compression.codec.zstd.level`) — usa un nivel fijo afinado.
# MAGIC No existe una capa "cold ZSTD-alto gratis" vía nivel; la palanca real de *cold* es la clase de
# MAGIC almacenamiento del objeto (S3 Intelligent-Tiering) + compaction, no el códec.

# COMMAND ----------

spark.sql(f"""
  ALTER TABLE {tbl('delta_eventos')} SET TBLPROPERTIES (
    'delta.parquet.compression.codec' = 'zstd',
    'delta.targetFileSize' = '134217728'   -- 128 MB por archivo (buen tamaño para lectura)
  )
""")
spark.sql(f"OPTIMIZE {tbl('delta_eventos')}")
display(spark.sql(f"DESCRIBE DETAIL {tbl('delta_eventos')}").select("numFiles", "sizeInBytes"))

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 TODO — Ejercicios
# MAGIC 1. Cambia el clustering a `(client_ip, mac_address)` y vuelve a `OPTIMIZE`. Compara `numFiles`.
# MAGIC 2. Corre `DESCRIBE HISTORY` y localiza las operaciones `OPTIMIZE` y `CLUSTER BY`.
# MAGIC 3. (Reto) Estima el tamaño del lakehouse para 15 días de datos reales (~300–360 TB de JSON crudo)
# MAGIC    usando la regla de la tabla del paso 6.

# COMMAND ----------

# TODO: tu código aquí
