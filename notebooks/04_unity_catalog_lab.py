# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC # 04 · Unity Catalog — gobierno de datos (Día 2)
# MAGIC
# MAGIC Unity Catalog (UC) centraliza **permisos, linaje, descubrimiento y clasificación** de todos los
# MAGIC activos. En Izzi es lo que permite exponer la capa **oro** a otras herramientas de forma gobernada.
# MAGIC
# MAGIC En este lab: jerarquía de objetos, permisos (RBAC), `information_schema`, tags de clasificación,
# MAGIC linaje, y (avanzado) filtros de fila / máscaras de columna para datos sensibles (PII de suscriptores).
# MAGIC
# MAGIC > Requiere haber corrido el lab **01** (usa tu tabla `delta_eventos`).

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Jerarquía: catálogo › esquema › tabla
# MAGIC Todo objeto se referencia por su nombre de 3 niveles `catalog.schema.table`.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{WORK_SCHEMA}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Permisos (RBAC) — GRANT / REVOKE
# MAGIC Se otorgan privilegios a usuarios/grupos/service principals. En Izzi típico:
# MAGIC un grupo `analistas_red` con `SELECT` sobre la capa oro, sin acceso a bronce/silver.
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG jgworkspaceclassic_catalog TO `analistas_red`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA <tu_esquema> TO `analistas_red`;
# MAGIC GRANT SELECT ON TABLE <tu_esquema>.gold_ip_equipment_map TO `analistas_red`;
# MAGIC REVOKE SELECT ON TABLE <tu_esquema>.silver_network_events FROM `analistas_red`;
# MAGIC ```
# MAGIC Veamos los permisos actuales de tu tabla:

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON TABLE {tbl('delta_eventos')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `information_schema` — metadatos consultables con SQL
# MAGIC UC expone catálogos/esquemas/tablas/columnas como vistas SQL. Útil para auditoría y automatización.

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_name, table_type, created, last_altered
  FROM `{CATALOG}`.information_schema.tables
  WHERE table_schema = '{WORK_SCHEMA}'
  ORDER BY created DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tags de clasificación
# MAGIC Etiqueta objetos/columnas para gobierno (p. ej. capa medallion, sensibilidad PII).

# COMMAND ----------

spark.sql(f"ALTER TABLE {tbl('delta_eventos')} SET TAGS ('capa' = 'demo', 'dominio' = 'red')")
# tag de PII a nivel columna sobre el struct subscriber
spark.sql(f"ALTER TABLE {tbl('delta_eventos')} ALTER COLUMN subscriber SET TAGS ('pii' = 'true')")

display(spark.sql(f"""
  SELECT tag_name, tag_value FROM `{CATALOG}`.information_schema.table_tags
  WHERE schema_name = '{WORK_SCHEMA}' AND table_name = 'delta_eventos'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Linaje
# MAGIC UC captura linaje automáticamente al leer/escribir tablas (tabla y columna). Se ve en la pestaña
# MAGIC **Lineage** del Catalog Explorer, y también en `system.access` (si está habilitado):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT source_table_full_name, target_table_full_name, event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_schema = '<tu_esquema>' ORDER BY event_time DESC LIMIT 20;
# MAGIC ```
# MAGIC Abre tu tabla `delta_eventos` en **Catalog Explorer → Lineage** para verlo gráficamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. (Avanzado) Máscara de columna para PII
# MAGIC Una **column mask** enmascara datos sensibles según quién consulta — sin duplicar la tabla.
# MAGIC Ejemplo: enmascarar `subscriber_id` a todos salvo un grupo autorizado.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE FUNCTION <tu_esquema>.mask_sub(v STRING)
# MAGIC   RETURN CASE WHEN is_account_group_member('analistas_red') THEN v ELSE '***MASKED***' END;
# MAGIC
# MAGIC ALTER TABLE <tu_esquema>.silver_network_events
# MAGIC   ALTER COLUMN subscriber_id SET MASK <tu_esquema>.mask_sub;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 TODO — Ejercicios
# MAGIC 1. Consulta `information_schema.columns` para listar las columnas de `delta_eventos` y sus tipos.
# MAGIC 2. Ponle un tag `sensibilidad='alta'` a la columna `network`.
# MAGIC 3. (Reto) Crea la función de máscara del paso 6 sobre tu tabla y comprueba el efecto con `SELECT`.

# COMMAND ----------

# TODO: tu código aquí
