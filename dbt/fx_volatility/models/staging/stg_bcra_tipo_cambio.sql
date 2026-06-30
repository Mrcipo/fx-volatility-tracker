with source as (
    select id, source_file, ingested_at, raw_payload
    from {{ source('raw', 'bcra_tipo_cambio') }}
),
unnested as (
    select
        source.id                                                            as raw_id,
        source.source_file,
        source.ingested_at,
        (source.raw_payload -> 'results' -> 0 ->> 'idVariable')::integer    as id_variable,
        (detalle ->> 'fecha')::date                                          as fecha_local,
        ((detalle ->> 'fecha')::date + interval '18 hours')
            at time zone 'America/Argentina/Buenos_Aires'                    as fecha_utc,
        (detalle ->> 'valor')::numeric(12,4)                                 as valor,
        'ARS'                                                                as moneda,
        'bcra_oficial_minorista'                                             as fuente
    from source,
    jsonb_array_elements(source.raw_payload -> 'results' -> 0 -> 'detalle') as detalle
)
select * from unnested
