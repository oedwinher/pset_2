{{ config(materialized='table') }}

{#- GOLD - dim_tiempo: calendario mensual continuo (sin huecos) entre el primer y ultimo mes observados. -#}

with limites as (

    select min(fecha_mes) as mes_min, max(fecha_mes) as mes_max
    from {{ ref('int_f104_ventas_limpias') }}

),

calendario as (

    select dateadd('month', g.n, l.mes_min)::date as fecha_mes
    from limites l
    cross join (
        select row_number() over (order by seq4()) - 1 as n
        from table(generator(rowcount => 600))
    ) g
    where dateadd('month', g.n, l.mes_min) <= l.mes_max

)

select
    year(fecha_mes) * 100 + month(fecha_mes)             as tiempo_key,
    fecha_mes,
    year(fecha_mes)                                      as anio,
    month(fecha_mes)                                     as mes,
    decode(month(fecha_mes),
        1, 'Enero', 2, 'Febrero', 3, 'Marzo', 4, 'Abril', 5, 'Mayo', 6, 'Junio',
        7, 'Julio', 8, 'Agosto', 9, 'Septiembre', 10, 'Octubre', 11, 'Noviembre', 12, 'Diciembre'
    )                                                    as nombre_mes,
    quarter(fecha_mes)                                   as trimestre,
    case when month(fecha_mes) <= 6 then 1 else 2 end    as semestre,
    year(fecha_mes) || '-T' || quarter(fecha_mes)        as anio_trimestre
from calendario
