-- ---------------------------------------------------------------------------
-- raw.bcra_variables
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.bcra_variables (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL
);

COMMENT ON TABLE raw.bcra_variables IS
    'Snapshot del catálogo completo de series monetarias. '
    'Alimentada por GET /estadisticas/v4.0/Monetarias (BCRA).';

CREATE INDEX idx_bcra_variables_payload
    ON raw.bcra_variables USING GIN (raw_payload);

CREATE INDEX idx_bcra_variables_ingested_at
    ON raw.bcra_variables (ingested_at);


-- ---------------------------------------------------------------------------
-- raw.bcra_tipo_cambio
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.bcra_tipo_cambio (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL
);

COMMENT ON TABLE raw.bcra_tipo_cambio IS
    'Serie histórica del tipo de cambio minorista ARS/USD (idVariable=4). '
    'Alimentada por GET /estadisticas/v4.0/Monetarias/4 (BCRA).';

CREATE INDEX idx_bcra_tipo_cambio_payload
    ON raw.bcra_tipo_cambio USING GIN (raw_payload);

CREATE INDEX idx_bcra_tipo_cambio_ingested_at
    ON raw.bcra_tipo_cambio (ingested_at);


-- ---------------------------------------------------------------------------
-- raw.dolarapi_cotizaciones
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.dolarapi_cotizaciones (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL
);

COMMENT ON TABLE raw.dolarapi_cotizaciones IS
    'Snapshot de las cotizaciones de las 7 casas de cambio paralelas (array completo). '
    'Alimentada por GET /v1/dolares (DolarAPI).';

CREATE INDEX idx_dolarapi_cotizaciones_payload
    ON raw.dolarapi_cotizaciones USING GIN (raw_payload);

CREATE INDEX idx_dolarapi_cotizaciones_ingested_at
    ON raw.dolarapi_cotizaciones (ingested_at);


-- ---------------------------------------------------------------------------
-- raw.banxico_series
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.banxico_series (
    id           BIGSERIAL    PRIMARY KEY,
    source_file  TEXT         NOT NULL,
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    raw_payload  JSONB        NOT NULL
);

COMMENT ON TABLE raw.banxico_series IS
    'Series SF60653 (tipo de cambio FIX MXN/USD) y SF46410 (interbancario 48h) combinadas. '
    'Alimentada por GET /SieAPIRest/service/v1/series/SF60653,SF46410/datos/{ini}/{fin} (Banxico).';

CREATE INDEX idx_banxico_series_payload
    ON raw.banxico_series USING GIN (raw_payload);

CREATE INDEX idx_banxico_series_ingested_at
    ON raw.banxico_series (ingested_at);
