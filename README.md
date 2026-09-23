# Workshop Izzi — Data Engineering en Databricks

Material del workshop de habilitación (2 sesiones × 2h) para ingenieros de datos.
Se construye, de punta a punta, un pipeline de **network analytics** sobre **flujos de red reales
(Kentik / KFlow)** de Izzi: del JSON crudo a una capa **oro** (bronze → silver → gold).

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
   - `DATA_PATH` — ruta donde viven los **JSONL preprocesados** de Kentik (Volume UC o **S3**).
   - `EVOLUTION_PATH` — carpeta con el archivo de esquema evolucionado (para el lab de schema evolution).
3. **Sigue los labs en orden** (`01` → `05`). Cada notebook empieza con `%run ./_setup`, que lee tu
   `config.py`, crea tu esquema y expone las variables (`CATALOG`, `WORK_SCHEMA`, `DATA_PATH`,
   `EVOLUTION_PATH`, `CHK_PATH`, el helper `read_clean()`, etc.).

## 📦 Datos

Los datos son **flujos de red Kentik/KFlow reales** (IPs origen/destino, puertos, protocolo,
bytes/paquetes, geo, dispositivo, ASN, y bloques `custom_str/custom_int/custom_bigint`).
**No se incluyen en este repo** — viven en un **bucket S3 / Volume** al que apunta `DATA_PATH`.

> **Formato de origen:** el cliente exporta **arreglos JSON en una sola línea** (`[{...},{...}]`, archivos de ~GB).
> Los readers nativos de Spark truncan o hacen OOM con ese formato a escala, así que hay un **paso de landing**
> (`scripts/izzi_landing_bronze_job.py`) que convierte **array → JSONL** en streaming y lo deja splittable
> para Auto Loader. Los labs leen el **JSONL ya preprocesado** desde `DATA_PATH`.
>
> **Nombres de campo inválidos para Delta:** los bloques `custom_*` traen keys con `/`, `(`, `)`.
> Por eso los labs los declaran como **`MAP<STRING, ...>`** (esquema explícito), evitando el error
> `DELTA_INVALID_CHARACTERS`.

## 📚 Contenido

| # | Notebook | Sesión | Tema |
|---|---|---|---|
| — | `notebooks/_setup` | — | Bootstrap: lee `config.py`, crea esquema + volumen de checkpoints, define `read_clean()` |
| 01 | `notebooks/01_delta_lab` | Día 1 | Tablas Delta: crear, `MERGE`, time travel (sobre flujos Kentik) |
| 02 | `notebooks/02_autoloader_lab` | Día 1 | Auto Loader: ingesta incremental (1.5M+ flujos), `cloud_files_state`, `_metadata`, archivos corruptos, schema evolution |
| 03 | `notebooks/03_declarative_pipeline` | Día 2 | Lakeflow Declarative Pipelines: conceptos + generación con Genie Code (bronze→silver→gold SCD2) |
| 04 | `notebooks/04_unity_catalog_lab` | Día 2 | Unity Catalog: permisos, linaje, tags, masking de IPs |
| 05 | `notebooks/05_optimizacion_delta_lab` | Día 2 | Liquid Clustering, ZSTD, file sizing |

> *Lakehouse Federation* se cubre de forma teórica (sin lab dedicado).

## 🗂️ Estructura del repo

```
notebooks/   Notebooks del workshop + config.py (edítalo aquí)
scripts/
  izzi_landing_bronze_job.py   Job de landing: arreglo JSON → JSONL → bronce (Auto Loader)
README.md
```

## ⚙️ Preprocesamiento (opcional, para reproducir el JSONL)

Si necesitas generar el JSONL desde los arreglos crudos del cliente, `scripts/izzi_landing_bronze_job.py`
es un Job de Databricks parametrizable que:
1. Convierte cada arreglo JSON (1 línea, ~GB) a **JSONL** en streaming (bajo consumo de memoria, paralelo por archivo).
2. Ingesta el JSONL a una tabla **bronce** con Auto Loader.

Parámetros (widgets): `source_path` (arreglos crudos, p. ej. el bucket S3), `landing_path` (JSONL de salida),
`bronze_table`, `checkpoint_path`.
