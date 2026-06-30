with rates as (
    select * from {{ ref('fx_daily_rates') }}
    where bcra_oficial is not null
       or mxn_fix      is not null
),

spreads as (
    select
        fecha,
        -- Datos Argentina
        bcra_oficial,
        dolar_blue,
        -- Spread ARG: blue vs oficial BCRA
        -- Cuántos pesos más caro es el dólar blue vs el oficial
        round(
            (dolar_blue - bcra_oficial) / bcra_oficial * 100,
        2) as spread_arg_pct,
        -- Datos México
        mxn_fix,
        -- Flag de calidad
        bcra_is_filled,
        dolar_blue_is_filled,
        mxn_is_filled,
        -- Metadata
        current_timestamp as calculated_at
    from rates
)

select * from spreads
order by fecha desc
