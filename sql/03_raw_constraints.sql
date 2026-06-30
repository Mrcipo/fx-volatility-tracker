-- Estrategia de idempotencia: source_file es único por tabla, lo que permite
-- usar INSERT ... ON CONFLICT (source_file) DO NOTHING en el loader.
-- Cada archivo JSON fuente puede insertarse una sola vez por tabla;
-- re-ejecuciones del pipeline con el mismo archivo son silenciosamente ignoradas.

ALTER TABLE raw.bcra_variables
    ADD CONSTRAINT uq_bcra_variables_source_file UNIQUE (source_file);

ALTER TABLE raw.bcra_tipo_cambio
    ADD CONSTRAINT uq_bcra_tipo_cambio_source_file UNIQUE (source_file);

ALTER TABLE raw.dolarapi_cotizaciones
    ADD CONSTRAINT uq_dolarapi_cotizaciones_source_file UNIQUE (source_file);

ALTER TABLE raw.banxico_series
    ADD CONSTRAINT uq_banxico_series_source_file UNIQUE (source_file);
