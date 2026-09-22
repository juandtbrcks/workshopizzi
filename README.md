# Workshop Izzi — Data Engineering en Databricks

Material del workshop de habilitación (2 sesiones × 2h) para ingenieros de datos.
Se construye, de punta a punta, un pipeline de **network analytics** sobre eventos de red en JSON:
del JSON crudo a una capa **oro** con el mapa histórico **IP ⇄ equipo ⇄ suscriptor** (SCD2).

## 🚀 Quickstart (Databricks Git folder)

1. **Crea la Git folder en Databricks**
   `Workspace` → **Create** → **Git folder** → pega la URL:
   ```
   https://github.com/juandtbrcks/workshopizzi
   ```
2. **Edita `notebooks/config.py`** con tus valores:
   - `CATALOG` — tu catálogo de Unity Catalog.
   - `WORK_SCHEMA` — el esquema donde crearás tus tablas (usa uno propio, p. ej. `prueba_izzi_juan`,
     para no pisarte con otros participantes).
   - `BASE_PATH` — la ruta (Volume / S3 / ADLS / GCS) donde vivirán los datos.
3. **Ejecuta `notebooks/00_cargar_datos`** una vez — copia la carpeta `data/` de este repo a tu Volume
   (Auto Loader y las pipelines leen desde Volumes, no desde archivos del workspace).
   *Si ya usas un Volume compartido que tiene los datos, puedes saltarte este paso.*
4. **Sigue los labs en orden** (`01` → `05`). Cada notebook empieza con `%run ./_setup`, que lee tu
   `config.py`, crea tu esquema y expone las variables (`CATALOG`, `WORK_SCHEMA`, `RAW_PATH`, etc.).

## 📚 Contenido

| # | Notebook | Sesión | Tema |
|---|---|---|---|
| — | `notebooks/_setup` | — | Bootstrap: lee `config.py`, crea esquema y volumen de checkpoints |
| 00 | `notebooks/00_cargar_datos` | — | Copia `data/` → tu Volume (correr una vez) |
| 01 | `notebooks/01_delta_lab` | Día 1 | Tablas Delta: crear, `MERGE`, time travel |
| 02 | `notebooks/02_autoloader_lab` | Día 1 | Auto Loader: ingesta incremental, `cloud_files_state`, `_metadata`, multilínea, schema evolution |
| 03 | `notebooks/03_declarative_pipeline` | Día 2 | Lakeflow Declarative Pipelines: conceptos + generación con Genie Code (bronze→silver→gold SCD2) |
| 04 | `notebooks/04_unity_catalog_lab` | Día 2 | Unity Catalog: permisos, linaje, tags, máscaras |
| 05 | `notebooks/05_optimizacion_delta_lab` | Día 2 | Liquid Clustering, ZSTD, file sizing |

> *Lakehouse Federation* se cubre de forma teórica (sin lab dedicado).

## 🗂️ Estructura del repo

```
notebooks/   Notebooks del workshop + config.py (edítalo aquí)
data/        Datos sintéticos (JSON-lines)
  raw_jsonl/            7 días de eventos (esquema base) — ejercicios de Auto Loader
  raw_jsonl_evolucion/  1 archivo con 2 campos nuevos — demo de schema evolution
  multiline_sample/     array JSON multilínea — gotcha "no splittable"
scripts/     Generadores de los datos sintéticos (referencia, stdlib puro)
```

## 📊 Sobre los datos

Datos **100% sintéticos** (sin información real de clientes). Cada evento es un documento JSON anidado
(`network / equipment / subscriber / docsis_metrics / session / geo`). El hilo clave del caso:
la **IP del cliente (`client_ip`) es estable por (suscriptor, día) y cambia entre días** → por eso se
necesita un histórico versionado (SCD2). Para regenerarlos, ver `scripts/`.
