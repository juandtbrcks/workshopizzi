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
# MAGIC BRONZE  bronze_network_events  ← Auto Loader lee los JSON del Volume (casi crudo)
# MAGIC SILVER  silver_network_events  ← Expectations (WARN/DROP/FAIL) + aplanado + tipado + dedup
# MAGIC GOLD    gold_ip_equipment_map  ← SCD2 (AUTO CDC): IP ⇄ equipo ⇄ suscriptor con vigencia  [ENTREGABLE]
# MAGIC         gold_network_health    ← agregado DOCSIS por plaza y hora
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
# MAGIC | **Bronze** | `bronze_network_events` | Auto Loader (`cloudFiles`) lee JSON crudo. Agrega metadata de ingesta. Sin transformar. |
# MAGIC | **Silver** | `silver_network_events` | Aplana structs, tipado fuerte, dedup por `event_id`, **expectations** de calidad. |
# MAGIC | **Gold** | `gold_ip_equipment_map` | **SCD2** con `apply_changes`: historial IP ⇄ equipo ⇄ suscriptor con vigencia. |
# MAGIC | **Gold** | `gold_network_health` | Métricas DOCSIS (SNR, errores, utilización) agregadas por plaza y hora. |
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
# MAGIC * **FAIL:** `event_id IS NOT NULL` — sin ID no hay dedup posible
# MAGIC * **DROP:** MAC válida (regex hex `XX:XX:XX:XX:XX:XX`), IP válida (regex IPv4), `event_type` conocido
# MAGIC * **WARN:** `snr_db IS NULL OR snr_db BETWEEN 20 AND 50` — SNR fuera de rango no invalida, pero se monitorea

# COMMAND ----------

# DBTITLE 1,Concepto: SCD2 con apply_changes (AUTO CDC)
# MAGIC %md
# MAGIC ## 3. SCD2 con `apply_changes` — el entregable clave
# MAGIC
# MAGIC El reto de Izzi: la **IP del cliente es estable por (suscriptor, día) pero cambia entre días**.
# MAGIC Necesitamos un historial versionado: *¿qué IP tenía qué equipo en qué momento?*
# MAGIC
# MAGIC `dlt.apply_changes()` implementa **AUTO CDC** (Change Data Capture):
# MAGIC
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="gold_ip_equipment_map",
# MAGIC     source="ip_binding_events",        # vista con solo dhcp_ack, dhcp_release, radius_start
# MAGIC     keys=["client_ip"],                # clave natural: la IP
# MAGIC     sequence_by=F.col("event_ts"),      # ordena cronológicamente
# MAGIC     apply_as_deletes=F.expr("event_type = 'dhcp_release'"),  # dhcp_release = baja lógica
# MAGIC     stored_as_scd_type=2,              # genera __START_AT / __END_AT
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **¿Qué pasa internamente?**
# MAGIC * Cuando llega un `dhcp_ack` con una IP nueva → abre un registro con `__START_AT = event_ts`, `__END_AT = NULL`.
# MAGIC * Cuando esa IP cambia de equipo/suscriptor → cierra la versión anterior (`__END_AT`) y abre una nueva.
# MAGIC * `dhcp_release` → marca como baja lógica (cierra sin abrir nueva versión).
# MAGIC
# MAGIC ### Optimización: IP y MAC como BIGINT
# MAGIC Convertir `client_ip` (string `"10.20.30.40"`) a un BIGINT numérico acelera joins y reduce storage.
# MAGIC Igual con `mac_address` (hex `"AA:BB:CC:DD:EE:FF"` → BIGINT vía `conv(replace, 16, 10)`).

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
# MAGIC 2. **Source code:** tu notebook recién generado.
# MAGIC 3. **Destino:** catálogo y **tu esquema personal** (se muestra abajo).
# MAGIC 4. **Serverless** ✔ · Pipeline mode: **Triggered**.
# MAGIC 5. (Opcional) Advanced → Configuration: `izzi.source_path` = ruta de tu Volume si difiere del default.
# MAGIC 6. **Start**. Verás el DAG bronze→silver→gold y las métricas de expectations por tabla.

# COMMAND ----------

# DBTITLE 1,Prompt para Genie Code
# Ejecuta esta celda para generar el prompt con tus variables del workshop.
# Luego cópialo y pégalo en Genie Code dentro de un notebook nuevo.

prompt = f"""
Genera el código fuente completo de una Lakeflow Spark Declarative Pipeline en Python.
El notebook resultante se adjuntará a una pipeline de Databricks (NO se ejecuta interactivamente).

## Datos de origen
- Carpeta de JSON-lines: `{RAW_PATH}`
- Cada archivo tiene eventos de red con structs anidados: network, equipment, subscriber,
  docsis_metrics, session, geo
- La ruta debe ser configurable vía spark.conf.get("izzi.source_path", "{RAW_PATH}")

## Tablas a crear (arquitectura medallion)

### BRONZE: `bronze_network_events`
- Streaming table con Auto Loader (cloudFiles) leyendo JSON
- Inferir tipos de columna, agregar _rescued_data
- Columnas de linaje: _ingest_timestamp (timestamp actual) y _source_file (_metadata.file_path)

### SILVER: `silver_network_events`
- Lee de bronze como stream
- Aplanar TODOS los structs: network.* (client_ip, public_ip, mac_address, cmts_name, nas_ip,
  dhcp_server, vlan, lease_seconds), equipment.* (serial, model, firmware, device_type),
  subscriber.* (subscriber_id, plan, account_status), docsis_metrics.* (snr_db,
  downstream_power_dbmv, upstream_power_dbmv, channel_utilization_pct, uncorrectable_errors),
  session.* (session_id, bytes_in, bytes_out, session_time_s), geo.* (ciudad, estado)
- Convertir event_timestamp a timestamp como event_ts
- Castear tipos: vlan/lease_seconds/uncorrectable_errors a int, métricas DOCSIS a double,
  bytes_in/bytes_out a long, event_date a date
- Convertir client_ip a BIGINT numérico (client_ip_num) y mac_address a BIGINT (mac_num)
  para joins rápidos
- Deduplicar por event_id con watermark de 2 horas sobre event_ts
- Liquid Clustering por: subscriber_id, mac_address, event_date
- Expectations (usar los 3 modos):
  * FAIL: event_id IS NOT NULL
  * DROP (expect_all_or_drop): MAC válida (regex hex), IP válida (regex IPv4),
    event_type IN (dhcp_ack, dhcp_release, radius_start, radius_stop, docsis_poll)
  * WARN: snr_db IS NULL OR snr_db BETWEEN 20 AND 50

### GOLD 1: `gold_ip_equipment_map` — ENTREGABLE PRINCIPAL
- SCD2 con dlt.apply_changes (AUTO CDC)
- Vista intermedia ip_binding_events que filtra solo dhcp_ack, dhcp_release, radius_start
- Key: client_ip
- Sequence by: event_ts
- apply_as_deletes: event_type = 'dhcp_release'
- stored_as_scd_type=2 (genera __START_AT / __END_AT)
- Liquid Clustering por: client_ip, mac_address, subscriber_id

### GOLD 2: `gold_network_health`
- Métricas DOCSIS agregadas por plaza y ventana de 1 hora (F.window)
- Solo eventos docsis_poll
- Agregados: avg snr_db, avg channel_utilization_pct, sum uncorrectable_errors,
  countDistinct mac_address (modems), count polls
- Liquid Clustering por: plaza, window_start

## Requisitos adicionales
- Todo en un solo notebook con import dlt y from pyspark.sql import functions as F
- Cada tabla debe tener comment descriptivo y table_properties con quality = bronze/silver/gold
- Usar delta.dataSkippingNumIndexedCols = 40 en silver (muchas columnas aplanadas)
"""

print("=" * 70)
print("  PROMPT PARA GENIE CODE")
print("  Catálogo:", CATALOG)
print("  Esquema destino:", WORK_SCHEMA)
print("  Datos:", RAW_PATH)
print("=" * 70)
print(prompt)