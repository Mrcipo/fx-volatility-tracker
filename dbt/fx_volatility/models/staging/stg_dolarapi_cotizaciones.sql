with source as (
    select id, source_file, ingested_at, raw_payload
    from {{ source('raw', 'dolarapi_cotizaciones') }}
),
unnested as (
    select
        source.id                                               as raw_id,
        source.source_file,
        source.ingested_at,
        cotizacion ->> 'casa'                                   as casa,
        cotizacion ->> 'nombre'                                 as nombre,
        (cotizacion ->> 'compra')::numeric(12,4)                as compra,
        (cotizacion ->> 'venta')::numeric(12,4)                 as venta,
        (cotizacion ->> 'fechaActualizacion')::timestamptz      as fecha_utc,
        ((cotizacion ->> 'fechaActualizacion')::timestamptz)::date as fecha_local,
        'ARS'                                                   as moneda
    from source,
    jsonb_array_elements(source.raw_payload) as cotizacion
)
select * from unnested
