# FX Volatility Tracker

🌐 **English** · [Español](README.es.md)

[![CI](https://github.com/Mrcipo/fx-volatility-tracker/actions/workflows/ci.yml/badge.svg)](https://github.com/Mrcipo/fx-volatility-tracker/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

> End-to-end data pipeline that tracks official and parallel exchange rates for Argentina and Mexico in near real time, with spread calculation and statistical anomaly detection.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Quick Start](#quick-start)
- [Data Pipeline](#data-pipeline)
- [Data Modeling](#data-modeling)
- [Orchestration](#orchestration)
- [Data Quality](#data-quality)
- [Dashboard](#dashboard)
- [Design Decisions](#design-decisions)
- [License](#license)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          EXTRACTION                             │
│  BCRA API v4.0   │   DolarAPI v1   │   Banxico SIE REST         │
│  (official ARS)  │ (parallel ARG)  │   (FIX + interbank MX)     │
└────────┬─────────┴────────┬────────┴──────────┬─────────────────┘
         │                  │                   │
         ▼                  ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LANDING ZONE (disk)                         │
│              data/raw/{source}/*.json  (immutable)              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  RAW LAYER (PostgreSQL)                         │
│   raw.bcra_tipo_cambio  │  raw.dolarapi_cotizaciones            │
│   raw.bcra_variables    │  raw.banxico_series                   │
│   raw JSONB + audit     │  ON CONFLICT DO NOTHING               │
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGING LAYER (dbt views)                       │
│   stg_bcra_tipo_cambio  │  stg_dolarapi_cotizaciones            │
│   stg_banxico_series    │  Timestamps → UTC │ JSONB unpacked    │
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   MARTS LAYER (dbt tables)                      │
│   fx_daily_rates  →  Gaps-and-islands forward fill (max 3 days) │
│   fx_spreads      →  Spread % blue vs official │ FIX vs interbk │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATA QUALITY (dbt tests - 22 tests)                │
│   Critical (error): uniqueness, not_null, accepted_values       │
│   Warnings:  statistical spread anomalies (mean ± 2σ)           │
└─────────────────────────────────────────────────────────────────┘
                             ▲
                             │
┌─────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION (Apache Airflow)                     │
│   DAG fx_ingestion │ schedule: 0 */2 * * * │ 11 tasks           │
│   Exponential retry │ trigger_rule split by severity            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Extraction | Python + requests | 3.12 / 2.32.3 |
| Storage | PostgreSQL | 15 |
| Transformation | dbt Core + dbt-postgres | 1.8.8 / 1.8.2 |
| Orchestration | Apache Airflow | 2.9.2 |
| Visualization | Metabase | latest |
| Infrastructure | Docker + Docker Compose | - |
| Python dependencies | pandas, psycopg2-binary, python-dotenv | 2.2.2 / 2.9.9 / 1.0.1 |

---

## Project Structure

```
fx-volatility-tracker/
├── extractors/
│   ├── base_client.py          # HTTPAdapter with exponential retry (5 attempts, backoff x2)
│   ├── bcra.py                 # BCRA API v4.0 - official ARS/USD exchange rate
│   ├── dolarapi.py             # DolarAPI - 7 parallel exchange rates for Argentina
│   └── banxico.py              # Banxico SIE - series SF60653 (FIX) and SF46410
├── loaders/
│   ├── db.py                   # psycopg2 connection with env var validation
│   └── load_raw.py             # Idempotent loader: ON CONFLICT (source_file) DO NOTHING
├── sql/
│   ├── 01_schemas.sql          # raw and staging schemas
│   ├── 02_raw_tables.sql       # 4 JSONB tables + GIN and B-tree indexes
│   └── 03_raw_constraints.sql  # UNIQUE constraints for idempotency
├── dbt/
│   ├── profiles/
│   │   └── profiles.yml        # Internal Docker profile (host: postgres, port: 5432)
│   └── fx_volatility/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml              # 16 critical tests
│       │   │   ├── stg_bcra_tipo_cambio.sql
│       │   │   ├── stg_dolarapi_cotizaciones.sql
│       │   │   └── stg_banxico_series.sql
│       │   └── marts/
│       │       ├── schema.yml              # Uniqueness and not_null tests
│       │       ├── fx_daily_rates.sql      # Gaps-and-islands forward fill
│       │       └── fx_spreads.sql          # Daily spread calculation
│       ├── tests/
│       │   └── assert_spread_within_bounds.sql  # Anomaly detection (warn)
│       └── macros/
│           └── generate_schema_name.sql    # Override: avoids staging_staging
├── dags/
│   └── fx_ingestion.py         # Full DAG with 11 tasks and exponential retry
├── data/
│   └── raw/                    # Landing zone (excluded from git via .gitignore)
│       ├── bcra_variables/
│       ├── bcra_tipo_cambio/
│       ├── dolarapi/
│       └── banxico/
├── docker-compose.yml          # Postgres 15 + Airflow (init, webserver, scheduler) + Metabase
├── .env.example                # Environment variables template
├── .gitattributes              # LF line endings for Linux/Windows compatibility
└── requirements.txt
```

---

## Data Sources

| API | Country | Data | Auth | Publication frequency |
|---|---|---|---|---|
| [BCRA v4.0](https://api.bcra.gob.ar) | 🇦🇷 Argentina | Retail ARS/USD exchange rate (idVariable=4) | No token | Business days |
| [DolarAPI](https://dolarapi.com) | 🇦🇷 Argentina | 7 types: official, blue, bolsa, CCL, wholesale, crypto, card | No token | Intraday |
| [Banxico SIE](https://www.banxico.org.mx/SieAPIRest) | 🇲🇽 Mexico | SF60653 (FIX MXN/USD) + SF46410 (48h interbank) | Token required | Business days |

---

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.12+
- Free [Banxico SIE](https://www.banxico.org.mx/SieAPIRest/service/v1/) token

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/Mrcipo/fx-volatility-tracker.git
cd fx-volatility-tracker

# 2. Create a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate           # Linux / macOS
# .venv\Scripts\Activate.ps1        # Windows PowerShell
pip install -r requirements.txt

# 3. Configure environment variables
cp .env.example .env
# Edit .env with your BMX_TOKEN and the Airflow Fernet key

# 4. Start the infrastructure
docker compose up airflow-init       # Initialize the Airflow DB (once)
docker compose up -d                 # Start the full stack

# 5. Apply the PostgreSQL DDL

# Linux / macOS
for f in sql/01_schemas.sql sql/02_raw_tables.sql sql/03_raw_constraints.sql; do
  docker compose exec -T postgres psql -U fxuser -d fxdb < "$f"
done

# Windows PowerShell
# foreach ($f in "sql/01_schemas.sql","sql/02_raw_tables.sql","sql/03_raw_constraints.sql") {
#   Get-Content $f | docker compose exec -T postgres psql -U fxuser -d fxdb
# }
```

### Airflow setup

1. Open `http://localhost:8080` (user: `admin`, password: `admin`)
2. Go to **Admin → Variables** and create the `BMX_TOKEN` variable with your Banxico token
3. Enable the `fx_ingestion` DAG

### Required environment variables

```env
BMX_TOKEN=your_banxico_token
POSTGRES_USER=fxuser
POSTGRES_PASSWORD=fxpass
POSTGRES_DB=fxdb
POSTGRES_HOST=localhost
POSTGRES_PORT=5439
AIRFLOW__CORE__FERNET_KEY=your_fernet_key
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://fxuser:fxpass@postgres:5432/fxdb?options=-csearch_path=airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Generate a Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Data Pipeline

### Extraction with Exponential Retry

All extractors inherit from `BaseAPIClient`, which implements automatic retry with exponential backoff:

```
Attempt 1 → failure → wait 1s
Attempt 2 → failure → wait 2s
Attempt 3 → failure → wait 4s
Attempt 4 → failure → wait 8s
Attempt 5 → failure → APIClientError
```

HTTP status codes that trigger a retry: `429, 500, 502, 503, 504`

### Loader Idempotency

The loader uses `INSERT ... ON CONFLICT (source_file) DO NOTHING`. The `source_file` (the timestamped JSON filename) acts as a natural key: the same file can never be inserted twice into the same table. If Airflow retries a failed task, data that was already loaded is silently ignored.

### Forward Fill with a 3-Day Limit

Exchange rates are not published on weekends or holidays. To keep a continuous series without distorting the data, the **gaps-and-islands** pattern is used:

```sql
-- The group number only increments when a real value appears
sum(case when valor is not null then 1 else 0 end)
    over (order by fecha) as grp

-- FIRST_VALUE within the group propagates the last known value
first_value(valor) over (partition by grp order by fecha) as valor_filled

-- ROW_NUMBER - 1 within the group = consecutive days without data
row_number() over (partition by grp order by fecha) - 1 as gap
```

If `gap > 3`, the value is intentionally set to `NULL` — it is not propagated beyond the limit. Every column includes an `*_is_filled` flag for traceability.

### Timezone Normalization to UTC

| Source | Original format | Conversion |
|---|---|---|
| BCRA | `"2026-06-29"` (date, no tz) | `date + interval '18h'` AT TIME ZONE `America/Argentina/Buenos_Aires` |
| DolarAPI | `"2026-06-29T18:00:00.000Z"` (explicit UTC) | No conversion needed |
| Banxico | `"29/06/2026"` (DD/MM/YYYY, no tz) | `to_date(..., 'DD/MM/YYYY')::timestamp` AT TIME ZONE `America/Mexico_City` |

---

## Data Modeling

### Raw Layer — Immutability Principle

```sql
CREATE TABLE raw.bcra_tipo_cambio (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL UNIQUE,  -- idempotency guarantee
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL           -- untransformed raw JSON
);
-- GIN index for efficient JSONB queries
-- B-tree index on ingested_at for time-based queries
```

The raw layer is never updated or deleted. It is the source of truth for replaying any transformation.

### Marts — Final Output

**`fx_daily_rates`** — one row per day with the exchange rates from every source, forward fill applied and traceability flags.

**`fx_spreads`** — percentage spread calculated as:
```
spread_arg_pct = (dolar_blue - bcra_oficial) / bcra_oficial * 100
```

---

## Orchestration

The `fx_ingestion` DAG runs every 2 hours (`0 */2 * * *`) with the following characteristics:

```
extract_bcra ──→ load_bcra_variables ──┐
                 load_bcra_tipo_cambio ─┤
extract_dolar──→ load_dolarapi ─────────┼──→ dbt_staging ──→ dbt_marts ──→ dbt_test_critical ──→ dbt_test_warnings
extract_banxico─→ load_banxico ─────────┘
```

| Parameter | Value |
|---|---|
| Retries | 3 with exponential backoff |
| Initial delay | 5 minutes |
| Maximum delay | 30 minutes |
| `dbt_staging` trigger rule | `all_done` — runs even if an extraction fails |
| `dbt_test_warnings` trigger rule | `all_done` — reports anomalies without blocking |

---

## Data Quality

22 dbt tests organized in two severity levels:

### Critical Tests (`severity: error`) — block the pipeline

- `not_null` on all key columns in staging and marts
- `unique` on the date columns of `fx_daily_rates` and `fx_spreads`
- `accepted_values` for `casa` in DolarAPI (`oficial`, `blue`, `bolsa`, `contadoconliqui`, `mayorista`, `cripto`, `tarjeta`)
- `accepted_values` for `id_serie` in Banxico (`SF60653`, `SF46410`)
- `accepted_values` for `fuente` in BCRA (`bcra_oficial_minorista`)

### Anomaly Tests (`severity: warn`) — alert without blocking

- **`assert_spread_within_bounds`**: detects spreads outside the historical `mean ± 2σ` range. When the daily spread exceeds this threshold, Airflow logs the warning but the pipeline keeps running.

---

## Dashboard

The pipeline serves the final data through a **Metabase** dashboard (`http://localhost:3000`) with four visualizations:

- **ARS/USD Official vs Blue** — historical evolution of Argentina's official and parallel (blue) exchange rates on the same timeline
- **ARG Spread % Blue vs Official** — daily percentage spread calculated by dbt
- **MXN/USD FIX Banxico Exchange Rate** — historical series of the official Mexican exchange rate (series SF60653)
- **Summary table with quality flags** — daily view with ✓ Real data / ⚠ Forward fill indicators for each source

![FX Volatility Tracker Dashboard](docs/dashboard.png)

---

## Design Decisions

**Why raw JSONB in the raw layer?**
The raw layer stores each API's full payload without unpacking it. This makes it possible to reprocess any transformation if a bug is found, without calling the APIs again. If an API changes its structure (as happened between BCRA v3.0 and v4.0), the loader keeps working — only the dbt model changes.

**Why `gaps-and-islands` instead of `LAST_VALUE IGNORE NULLS`?**
PostgreSQL does not support `IGNORE NULLS` in window functions. The common substitute (`MAX()` over a window) distorts the data by picking the maximum of the window instead of the last known value — a silent error that can artificially inflate the calculated spread. The gaps-and-islands pattern solves this correctly using `FIRST_VALUE` partitioned by group.

**Why a separate `profiles.yml` for Docker?**
Inside the Airflow containers, PostgreSQL is called `postgres` (the Docker service name) on internal port `5432`. From Windows it is reached at `localhost:5439`. Keeping two profiles avoids connectivity errors and makes the project portable to any environment.

**Why `ON CONFLICT DO NOTHING` instead of a full UPSERT?**
The raw layer is append-only by design. If a file has already been loaded, ignoring it is the right answer — not updating it. Updating raw data that was already ingested would violate the immutability principle of the landing zone.

---

## License

[MIT](LICENSE)
