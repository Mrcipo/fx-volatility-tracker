CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS
    'Datos crudos inmutables tal como llegan de las APIs. '
    'Ninguna transformación se aplica aquí; cada fila es un ingreso directo de un archivo JSON.';

CREATE SCHEMA IF NOT EXISTS staging;
COMMENT ON SCHEMA staging IS
    'Datos tipados y normalizados generados por dbt a partir del schema raw. '
    'Las tablas en este schema son reconstruibles en su totalidad desde raw.';
