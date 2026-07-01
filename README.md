# FX Volatility Tracker

> Pipeline de datos end-to-end para tracking de tipos de cambio oficiales y paralelos de Argentina y México en tiempo cuasi-real, con cálculo de spreads y detección de anomalías estadísticas.

---

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Stack Técnico](#stack-técnico)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Fuentes de Datos](#fuentes-de-datos)
- [Inicio Rápido](#inicio-rápido)
- [Pipeline de Datos](#pipeline-de-datos)
- [Modelado de Datos](#modelado-de-datos)
- [Orquestación](#orquestación)
- [Data Quality](#data-quality)
- [Decisiones de Diseño](#decisiones-de-diseño)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXTRACCIÓN                               │
│  BCRA API v4.0   │   DolarAPI v1   │   Banxico SIE REST         │
│  (oficial ARS)   │  (paralelos ARG)│   (FIX + interbancario MX) │
└────────┬─────────┴────────┬────────┴──────────┬─────────────────┘
         │                  │                   │
         ▼                  ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LANDING ZONE (disco)                        │
│              data/raw/{fuente}/*.json  (inmutable)              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  RAW LAYER (PostgreSQL)                         │
│   raw.bcra_tipo_cambio  │  raw.dolarapi_cotizaciones            │
│   raw.bcra_variables    │  raw.banxico_series                   │
│   JSONB crudo + auditoría │ ON CONFLICT DO NOTHING              │
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                 STAGING LAYER (dbt views)                       │
│   stg_bcra_tipo_cambio  │  stg_dolarapi_cotizaciones            │
│   stg_banxico_series    │  Timestamps → UTC │ JSONB desempaquetado│
└────────────────────────────┬────────────────────────────────────┘
                             │  dbt
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   MARTS LAYER (dbt tables)                      │
│   fx_daily_rates  →  Forward fill gaps-and-islands (max 3 días) │
│   fx_spreads      →  Spread % blue vs oficial │ FIX vs interbanc│
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│              DATA QUALITY (dbt tests - 22 tests)                │
│   Críticos (error): uniqueness, not_null, accepted_values       │
│   Warnings:  anomalías estadísticas spread (media ± 2σ)         │
└─────────────────────────────────────────────────────────────────┘
                             ▲
                             │
┌─────────────────────────────────────────────────────────────────┐
│              ORQUESTACIÓN (Apache Airflow)                      │
│   DAG fx_ingestion │ schedule: 0 */2 * * * │ 11 tareas          │
│   Retry exponencial │ trigger_rule separado por severidad        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Stack Técnico

| Capa | Tecnología | Versión |
|---|---|---|
| Extracción | Python + requests | 3.12 / 2.32.3 |
| Almacenamiento | PostgreSQL | 15 |
| Transformación | dbt Core + dbt-postgres | 1.8.8 / 1.8.2 |
| Orquestación | Apache Airflow | 2.9.2 |
| Infraestructura | Docker + Docker Compose | - |
| Dependencias Python | pandas, psycopg2-binary, python-dotenv | 2.2.2 / 2.9.9 / 1.0.1 |

---

## Estructura del Proyecto

```
fx-volatility-tracker/
├── extractors/
│   ├── base_client.py          # HTTPAdapter con retry exponencial (5 intentos, backoff x2)
│   ├── bcra.py                 # BCRA API v4.0 - tipo de cambio oficial ARS/USD
│   ├── dolarapi.py             # DolarAPI - 7 tipos de cambio paralelos Argentina
│   └── banxico.py              # Banxico SIE - series SF60653 (FIX) y SF46410
├── loaders/
│   ├── db.py                   # Conexión psycopg2 con validación de env vars
│   └── load_raw.py             # Loader idempotente: ON CONFLICT (source_file) DO NOTHING
├── sql/
│   ├── 01_schemas.sql          # Schemas raw y staging
│   ├── 02_raw_tables.sql       # 4 tablas con JSONB + índices GIN y B-tree
│   └── 03_raw_constraints.sql  # UNIQUE constraints para idempotencia
├── dbt/
│   ├── profiles/
│   │   └── profiles.yml        # Perfil Docker interno (host: postgres, port: 5432)
│   └── fx_volatility/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   ├── schema.yml              # 16 tests críticos
│       │   │   ├── stg_bcra_tipo_cambio.sql
│       │   │   ├── stg_dolarapi_cotizaciones.sql
│       │   │   └── stg_banxico_series.sql
│       │   └── marts/
│       │       ├── schema.yml              # Tests de unicidad y not_null
│       │       ├── fx_daily_rates.sql      # Forward fill gaps-and-islands
│       │       └── fx_spreads.sql          # Cálculo de spreads diarios
│       ├── tests/
│       │   └── assert_spread_within_bounds.sql  # Detección anomalías (warn)
│       └── macros/
│           └── generate_schema_name.sql    # Override: evita staging_staging
├── dags/
│   └── fx_ingestion.py         # DAG completo con 11 tareas y retry exponencial
├── data/
│   └── raw/                    # Landing zone (excluida de git via .gitignore)
│       ├── bcra_variables/
│       ├── bcra_tipo_cambio/
│       ├── dolarapi/
│       └── banxico/
├── docker-compose.yml          # Postgres 15 + Airflow (init, webserver, scheduler)
├── .env.example                # Template de variables de entorno
├── .gitattributes              # Finales de línea LF para compatibilidad Linux/Windows
└── requirements.txt
```

---

## Fuentes de Datos

| API | País | Dato | Auth | Frecuencia publicación |
|---|---|---|---|---|
| [BCRA v4.0](https://api.bcra.gob.ar) | 🇦🇷 Argentina | Tipo de cambio minorista ARS/USD (idVariable=4) | Sin token | Días hábiles |
| [DolarAPI](https://dolarapi.com) | 🇦🇷 Argentina | 7 tipos: oficial, blue, bolsa, CCL, mayorista, cripto, tarjeta | Sin token | Intradía |
| [Banxico SIE](https://www.banxico.org.mx/SieAPIRest) | 🇲🇽 México | SF60653 (FIX MXN/USD) + SF46410 (interbancario 48h) | Token requerido | Días hábiles |

---

## Inicio Rápido

### Prerrequisitos

- Docker Desktop
- Python 3.12+
- Token gratuito de [Banxico SIE](https://www.banxico.org.mx/SieAPIRest/service/v1/)

### Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/Mrcipo/fx-volatility-tracker.git
cd fx-volatility-tracker

# 2. Crear entorno virtual e instalar dependencias
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows PowerShell
pip install -r requirements.txt

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con tu BMX_TOKEN y la Fernet Key de Airflow

# 4. Levantar infraestructura
docker compose up airflow-init       # Inicializar DB de Airflow (una vez)
docker compose up -d                 # Levantar stack completo

# 5. Aplicar DDL de PostgreSQL
Get-Content sql/01_schemas.sql | docker exec -i fx-volatility-tracker-postgres-1 psql -U fxuser -d fxdb
Get-Content sql/02_raw_tables.sql | docker exec -i fx-volatility-tracker-postgres-1 psql -U fxuser -d fxdb
Get-Content sql/03_raw_constraints.sql | docker exec -i fx-volatility-tracker-postgres-1 psql -U fxuser -d fxdb
```

### Configuración de Airflow

1. Acceder a `http://localhost:8080` (usuario: `admin`, password: `admin`)
2. Ir a **Admin → Variables** y crear la variable `BMX_TOKEN` con tu token de Banxico
3. Activar el DAG `fx_ingestion`

### Variables de entorno requeridas

```env
BMX_TOKEN=tu_token_banxico
POSTGRES_USER=fxuser
POSTGRES_PASSWORD=fxpass
POSTGRES_DB=fxdb
POSTGRES_HOST=localhost
POSTGRES_PORT=5439
AIRFLOW__CORE__FERNET_KEY=tu_fernet_key
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://fxuser:fxpass@postgres:5432/fxdb?options=-csearch_path=airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Generar Fernet Key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Pipeline de Datos

### Extracción con Retry Exponencial

Todos los extractores heredan de `BaseAPIClient`, que implementa retry automático con backoff exponencial:

```
Intento 1 → fallo → espera 1s
Intento 2 → fallo → espera 2s
Intento 3 → fallo → espera 4s
Intento 4 → fallo → espera 8s
Intento 5 → fallo → APIClientError
```

Códigos HTTP que disparan retry: `429, 500, 502, 503, 504`

### Idempotencia del Loader

El loader usa `INSERT ... ON CONFLICT (source_file) DO NOTHING`. El `source_file` (nombre del archivo JSON con timestamp) actúa como clave natural: el mismo archivo nunca puede insertarse dos veces en la misma tabla. Si Airflow reintenta una tarea fallida, los datos ya cargados se ignoran silenciosamente.

### Forward Fill con Límite de 3 Días

El tipo de cambio no se publica los fines de semana ni feriados. Para mantener una serie continua sin distorsionar los datos, implementamos el patrón **gaps-and-islands**:

```sql
-- Número de grupo se incrementa solo cuando aparece un valor real
sum(case when valor is not null then 1 else 0 end)
    over (order by fecha) as grp

-- FIRST_VALUE dentro del grupo propaga el último valor conocido
first_value(valor) over (partition by grp order by fecha) as valor_filled

-- ROW_NUMBER - 1 dentro del grupo = días consecutivos sin dato
row_number() over (partition by grp order by fecha) - 1 as gap
```

Si `gap > 3`, el valor se marca como `NULL` intencional — no se propaga más allá del límite. Cada columna incluye un flag `*_is_filled` para trazabilidad.

### Normalización de Timezones a UTC

| Fuente | Formato original | Conversión |
|---|---|---|
| BCRA | `"2026-06-29"` (date sin tz) | `date + interval '18h'` AT TIME ZONE `America/Argentina/Buenos_Aires` |
| DolarAPI | `"2026-06-29T18:00:00.000Z"` (UTC explícito) | Sin conversión necesaria |
| Banxico | `"29/06/2026"` (DD/MM/YYYY sin tz) | `to_date(..., 'DD/MM/YYYY')::timestamp` AT TIME ZONE `America/Mexico_City` |

---

## Modelado de Datos

### Raw Layer — Principio de Inmutabilidad

```sql
CREATE TABLE raw.bcra_tipo_cambio (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL UNIQUE,  -- garantía de idempotencia
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL           -- JSON crudo sin transformar
);
-- Índice GIN para queries JSONB eficientes
-- Índice B-tree sobre ingested_at para queries temporales
```

La raw layer nunca se actualiza ni se borra. Es la fuente de verdad para replay de cualquier transformación.

### Marts — Resultado Final

**`fx_daily_rates`** — una fila por día con tipos de cambio de todas las fuentes, forward fill aplicado y flags de trazabilidad.

**`fx_spreads`** — spread porcentual calculado como:
```
spread_arg_pct = (dolar_blue - bcra_oficial) / bcra_oficial * 100
```

---

## Orquestación

El DAG `fx_ingestion` corre cada 2 horas (`0 */2 * * *`) con las siguientes características:

```
extract_bcra ──→ load_bcra_variables ──┐
                 load_bcra_tipo_cambio ─┤
extract_dolar──→ load_dolarapi ─────────┼──→ dbt_staging ──→ dbt_marts ──→ dbt_test_critical ──→ dbt_test_warnings
extract_banxico─→ load_banxico ─────────┘
```

| Parámetro | Valor |
|---|---|
| Reintentos | 3 con backoff exponencial |
| Delay inicial | 5 minutos |
| Delay máximo | 30 minutos |
| `dbt_staging` trigger rule | `all_done` — corre aunque alguna extracción falle |
| `dbt_test_warnings` trigger rule | `all_done` — reporta anomalías sin bloquear |

---

## Data Quality

22 tests dbt organizados en dos niveles de severidad:

### Tests Críticos (`severity: error`) — bloquean el pipeline

- `not_null` en todas las columnas clave de staging y marts
- `unique` en columnas de fecha de `fx_daily_rates` y `fx_spreads`
- `accepted_values` para `casa` en DolarAPI (`oficial`, `blue`, `bolsa`, `contadoconliqui`, `mayorista`, `cripto`, `tarjeta`)
- `accepted_values` para `id_serie` en Banxico (`SF60653`, `SF46410`)
- `accepted_values` para `fuente` en BCRA (`bcra_oficial_minorista`)

### Tests de Anomalías (`severity: warn`) — alertan sin bloquear

- **`assert_spread_within_bounds`**: detecta spreads fuera del rango `media ± 2σ` histórico. Cuando el spread diario supera este umbral, Airflow registra el warning en los logs pero el pipeline continúa ejecutándose.

---

## Decisiones de Diseño

**¿Por qué JSONB crudo en raw layer?**
La raw layer guarda el payload completo de cada API sin desempaquetar. Esto permite reprocesar cualquier transformación si se encuentra un bug, sin necesidad de re-llamar a las APIs. Si la API cambia su estructura (como pasó entre BCRA v3.0 y v4.0), el loader sigue funcionando — solo cambia el modelo dbt.

**¿Por qué `gaps-and-islands` en vez de `LAST_VALUE IGNORE NULLS`?**
PostgreSQL no soporta `IGNORE NULLS` en funciones de ventana. El sustituto común (`MAX()` sobre ventana) distorsiona los datos eligiendo el máximo de la ventana en vez del último valor conocido — un error silencioso que puede inflar artificialmente el spread calculado. El patrón gaps-and-islands resuelve esto correctamente usando `FIRST_VALUE` particionado por grupo.

**¿Por qué un `profiles.yml` separado para Docker?**
Dentro de los contenedores de Airflow, PostgreSQL se llama `postgres` (nombre del servicio Docker) en el puerto `5432` interno. Desde Windows se accede por `localhost:5439`. Mantener dos perfiles evita errores de conectividad y hace el proyecto portable a cualquier entorno.

**¿Por qué `ON CONFLICT DO NOTHING` en vez de UPSERT completo?**
La raw layer es append-only por diseño. Si un archivo ya fue cargado, ignorarlo es la respuesta correcta — no actualizarlo. Actualizar datos crudos ya ingestados violaría el principio de inmutabilidad de la landing zone.