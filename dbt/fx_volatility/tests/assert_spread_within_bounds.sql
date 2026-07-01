-- Test singular: falla (con severity warn) si el spread del día
-- supera la media histórica en más de 2 desviaciones estándar.
-- En producción esto dispara una alerta sin detener el pipeline.

with stats as (
    select
        avg(spread_arg_pct)    as media,
        stddev(spread_arg_pct) as sigma
    from {{ ref('fx_spreads') }}
    where spread_arg_pct is not null
),

anomalias as (
    select
        s.fecha,
        s.spread_arg_pct,
        st.media,
        st.sigma,
        st.media + 2 * st.sigma as upper_bound,
        st.media - 2 * st.sigma as lower_bound
    from {{ ref('fx_spreads') }} s
    cross join stats st
    where s.spread_arg_pct is not null
      and (
          s.spread_arg_pct > st.media + 2 * st.sigma
          or
          s.spread_arg_pct < st.media - 2 * st.sigma
      )
)

-- dbt tests fallan cuando retornan filas.
-- Si hay anomalías, este test retorna las filas anómalas
-- y dbt las reporta como warning (configurado en dbt_project.yml)
select * from anomalias
