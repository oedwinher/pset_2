# PSet #2 – Pipeline de Datos End-to-End: Nowcasting de Ventas Sectoriales (F104 – SRI)

Pipeline ELT reproducible para el proyecto del PSet #1 (IMAEC Flash a partir del Formulario 104 del SRI, 46 ramas CIIU):

```
CSV F104 ─► Kestra ─► Snowflake RAW ─► dbt Bronze ─► dbt Silver ─► dbt Gold (star schema) ─► Spark ─► ANALYTICS.OBT_NOWCASTING_VENTAS
```

| Capa | Dónde | Quién |
|---|---|---|
| RAW | `PSET2_DB.RAW.RAW_F104_VENTAS` | Kestra (`kestra/flows/f104_to_snowflake_raw.yml`) |
| Bronze | `PSET2_DB.BRONZE.STG_F104_VENTAS` | dbt (`dbt/models/staging`) |
| Silver | `PSET2_DB.SILVER.INT_F104_VENTAS_LIMPIAS` | dbt (`dbt/models/intermediate`) |
| Gold | `PSET2_DB.GOLD.{DIM_INDUSTRIA, DIM_TIEMPO, DIM_CORTE, FCT_VENTAS_MENSUALES}` | dbt (`dbt/models/marts`) |
| OBT | `PSET2_DB.ANALYTICS.OBT_NOWCASTING_VENTAS` | Spark (`spark/build_obt.py`) |

## Estructura

```
pset_2/
├── docker-compose.yml      # Kestra (+Postgres), Spark master/worker, y jobs dbt / spark-submit
├── .env.example            # plantilla de credenciales (el .env real NO se versiona)
├── data/seed/              # f104_ventas_46_ramas.csv (12,911 registros)
├── kestra/                 # flows/, scripts/ (carga a RAW) y deploy.sh / deploy.ps1
├── dbt/                    # proyecto dbt Core (Bronze/Silver/Gold + tests)
├── spark/                  # build_obt.py y requirements.txt
└── docs/                   # PSet2_memo_Ortega_Pasmay.pdf y .docx (memorando técnico)
```

## Requisitos

- Docker Desktop / Docker Engine con Compose v2 (≥ 4 GB de RAM para Docker).
- Una cuenta de Snowflake con permisos para crear base de datos, esquemas y tablas (p. ej. `ACCOUNTADMIN`) y un warehouse activo.
- Puertos libres: `8080` (Kestra), `8081` (Spark Master UI), `8082` (Worker UI), `7077` (Spark Master).

## 1. Clonar el repositorio y configurar credenciales

```bash
git clone <URL_DEL_REPOSITORIO_PSET_2> pset_2
cd pset_2
cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
```

Edite `.env` con **sus propias** credenciales (`SNOWFLAKE_ACCOUNT` con formato `<locator>.<region>`, usuario, contraseña, rol, warehouse). `SNOWFLAKE_DATABASE` por defecto es `PSET2_DB`; Kestra la crea si no existe. `.env` está en `.gitignore`.

## 2. Levantar la infraestructura

```bash
docker compose up -d
docker compose ps
```

- Kestra: <http://localhost:8080> (usuario `admin@kestra.io`, contraseña `Admin1234`, definidos en el compose para uso local).
- Spark Master UI: <http://localhost:8081> (debe mostrar 1 worker `ALIVE`).

## 3. Ingesta con Kestra (Raw)

Despliegue del flow y de sus archivos de namespace (script de carga + CSV semilla):

```bash
bash kestra/deploy.sh            # Windows PowerShell: .\kestra\deploy.ps1
```

Luego, en la UI de Kestra: namespace `ecuador.bce.coyuntura` → flow `f104_to_snowflake_raw` → **Execute** con `modo = completa` (primera carga: 12,911 filas en `RAW.RAW_F104_VENTAS`). O por API:

```bash
curl -u admin@kestra.io:Admin1234 -X POST \
  -F modo=completa -F reemplazar_raw=false \
  http://localhost:8080/api/v1/main/executions/ecuador.bce.coyuntura/f104_to_snowflake_raw
```

- **Trigger**: `0 6 15 * *` (día 15 de cada mes, 06:00 America/Guayaquil), modo `incremental` (meses fiscales M‑1 y M‑2).
- **Retries**: `type: constant`, `maxAttempts: 3`, `interval: PT10S`.
- **Backfill**: UI de Kestra → flow → pestaña *Triggers* → *Backfill executions* → rango de fechas. Cada ejecución usa su `{{ trigger.date ?? execution.startDate }}` y carga sus meses; las ejecuciones se encolan (`concurrency.limit: 1`).
- RAW es *append-only*: re-ejecutar no rompe nada; Bronze conserva el registro más reciente por clave.

## 4. Transformaciones con dbt (Bronze → Silver → Gold)

Con Docker (no requiere instalar nada):

```bash
docker compose --profile jobs run --rm dbt build
```

O con dbt local (Python 3.10–3.12), con las variables de `.env` cargadas en el shell:

```bash
pip install -r dbt/requirements.txt
cd dbt
export $(grep -v '^#' ../.env | xargs)        # PowerShell: Get-Content ..\.env | % { if ($_ -match '^([^#=]+)=(.*)$') { Set-Item "env:$($Matches[1])" $Matches[2] } }
export DBT_PROFILES_DIR=.                      # PowerShell: $env:DBT_PROFILES_DIR="."
dbt build
```

`dbt build` ejecuta modelos y tests (not_null, unique, relationships, rangos y reglas de negocio). Esquemas creados: `BRONZE`, `SILVER`, `GOLD`.

## 5. OBT con Spark

En el cluster Docker (master + worker):

```bash
docker compose --profile jobs run --rm spark-submit
```

O en local (`local[*]`, requiere Java 17):

```bash
pip install -r spark/requirements.txt
python spark/build_obt.py
```

El job lee las tablas Gold desde Snowflake, construye la OBT (grano rama × mes × corte), valida que los joins conserven el número de filas del hecho y que no haya duplicados (aborta si falla) y escribe `ANALYTICS.OBT_NOWCASTING_VENTAS`.

## 6. Verificación rápida (Snowflake)

```sql
SELECT COUNT(*) FROM PSET2_DB.RAW.RAW_F104_VENTAS;                  -- 12,911 tras la carga completa
SELECT COUNT(*) FROM PSET2_DB.GOLD.FCT_VENTAS_MENSUALES;            -- filas clasificadas en las 46 ramas
SELECT COUNT(*) FROM PSET2_DB.ANALYTICS.OBT_NOWCASTING_VENTAS;      -- igual que la tabla de hechos
```

## 7. Documento técnico

El memorando técnico con el diseño de arquitectura, ingesta, matriz de calidad de datos, modelado dimensional, Spark OBT, batch vs. streaming y limitaciones se encuentra disponible en:
- `docs/PSet2_memo_Ortega_Pasmay.pdf` (Documento oficial de entrega, 5 páginas)
- `docs/PSet2_memo_Ortega_Pasmay.docx` (Versión en formato Word)

## Apagar y limpiar

```bash
docker compose --profile jobs down        # conserva los volúmenes
docker compose --profile jobs down -v     # borra también el estado de Kestra/Postgres
```

## Notas

- Deflactor IPC **simulado** (variables `ipc_base_fecha` e `ipc_tasa_mensual` en `dbt/dbt_project.yml`); debe sustituirse por el IPC real del INEC antes de modelar.
- No se versionan credenciales: `.env` está ignorado y el compose las toma del entorno.
