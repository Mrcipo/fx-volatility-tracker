with source as (
    select id, source_file, ingested_at, raw_payload
    from {{ source('raw', 'banxico_series') }}
),
unnested as (
    select
        source.id                                               as raw_id,
        source.source_file,
        source.ingested_at,
        serie ->> 'idSerie'                                     as id_serie,
        serie ->> 'titulo'                                      as titulo,
        to_date(dato ->> 'fecha', 'DD/MM/YYYY')                 as fecha_local,
        to_date(dato ->> 'fecha', 'DD/MM/YYYY')::timestamp
            at time zone 'America/Mexico_City'                  as fecha_utc,
        (dato ->> 'dato')::numeric(12,4)                        as valor,
        'MXN'                                                   as moneda
    from source,
    jsonb_array_elements(source.raw_payload -> 'bmx' -> 'series') as serie,
    jsonb_array_elements(serie -> 'datos') as dato
)
select * from unnested
