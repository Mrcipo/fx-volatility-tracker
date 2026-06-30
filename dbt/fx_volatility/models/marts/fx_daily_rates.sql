with date_spine as (
    select generate_series(
        (select min(fecha_local) from {{ ref('stg_bcra_tipo_cambio') }}),
        current_date,
        interval '1 day'
    )::date as fecha
),

bcra_dedup as (
    select distinct on (fecha_local)
        fecha_local,
        valor as bcra_oficial
    from {{ ref('stg_bcra_tipo_cambio') }}
    order by fecha_local, ingested_at desc
),

dolar_dedup as (
    select distinct on (fecha_local, casa)
        fecha_local,
        casa,
        venta
    from {{ ref('stg_dolarapi_cotizaciones') }}
    order by fecha_local, casa, ingested_at desc
),

dolar_pivot as (
    select
        fecha_local,
        max(case when casa = 'blue'    then venta end) as dolar_blue,
        max(case when casa = 'oficial' then venta end) as dolar_oficial_dapi
    from dolar_dedup
    group by fecha_local
),

banxico_dedup as (
    select distinct on (fecha_local)
        fecha_local,
        valor as mxn_fix
    from {{ ref('stg_banxico_series') }}
    where id_serie = 'SF60653'
    order by fecha_local, ingested_at desc
),

joined as (
    select
        d.fecha,
        b.bcra_oficial,
        dp.dolar_blue,
        dp.dolar_oficial_dapi,
        mx.mxn_fix
    from date_spine d
    left join bcra_dedup    b  on b.fecha_local  = d.fecha
    left join dolar_pivot   dp on dp.fecha_local = d.fecha
    left join banxico_dedup mx on mx.fecha_local = d.fecha
),

/*
  Patrón gaps-and-islands:
  SUM(CASE WHEN valor IS NOT NULL THEN 1 ELSE 0 END) OVER (ORDER BY fecha)
  genera un número de grupo que se incrementa solo cuando aparece un valor
  real. Todos los NULLs que siguen a un valor real quedan en el mismo grupo,
  lo que permite usar FIRST_VALUE para propagar el último valor conocido.
*/
groups as (
    select
        fecha,
        bcra_oficial,
        dolar_blue,
        dolar_oficial_dapi,
        mxn_fix,
        sum(case when bcra_oficial    is not null then 1 else 0 end)
            over (order by fecha) as bcra_grp,
        sum(case when dolar_blue      is not null then 1 else 0 end)
            over (order by fecha) as dolar_grp,
        sum(case when mxn_fix         is not null then 1 else 0 end)
            over (order by fecha) as mxn_grp
    from joined
),

filled as (
    select
        fecha,
        bcra_oficial,
        dolar_blue,
        mxn_fix,
        -- FIRST_VALUE dentro de cada grupo = último valor real conocido
        first_value(bcra_oficial) over (
            partition by bcra_grp order by fecha
        ) as bcra_filled,
        first_value(dolar_blue) over (
            partition by dolar_grp order by fecha
        ) as dolar_blue_filled,
        first_value(mxn_fix) over (
            partition by mxn_grp order by fecha
        ) as mxn_filled,
        -- ROW_NUMBER - 1 dentro del grupo = días consecutivos sin dato
        -- (0 = el día tiene dato real, 1 = 1 día de fill, etc.)
        row_number() over (partition by bcra_grp  order by fecha) - 1
            as bcra_gap,
        row_number() over (partition by dolar_grp order by fecha) - 1
            as dolar_gap,
        row_number() over (partition by mxn_grp   order by fecha) - 1
            as mxn_gap
    from groups
)

select
    fecha,
    -- Aplica filled solo si el gap está dentro del límite de 3 días
    case when bcra_gap  <= 3 then bcra_filled       end as bcra_oficial,
    case when dolar_gap <= 3 then dolar_blue_filled  end as dolar_blue,
    case when mxn_gap   <= 3 then mxn_filled         end as mxn_fix,
    -- Flags de trazabilidad: true = valor proviene de forward fill
    (bcra_oficial  is null and bcra_gap  <= 3) as bcra_is_filled,
    (dolar_blue    is null and dolar_gap <= 3) as dolar_blue_is_filled,
    (mxn_fix       is null and mxn_gap   <= 3) as mxn_is_filled
from filled
order by fecha desc
