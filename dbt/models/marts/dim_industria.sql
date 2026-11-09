{{ config(materialized='table') }}

{#- GOLD - dim_industria: las 46 ramas CIIU (CNT) y su clasificacion macro. El centinela '000' queda fuera. -#}

with ramas as (

    select
        codigo_industria,
        max(nombre_industria) as nombre_industria
    from {{ ref('int_f104_ventas_limpias') }}
    where not es_sin_clasificar
    group by codigo_industria

)

select
    codigo_industria,
    to_number(codigo_industria)                         as numero_rama,
    nombre_industria,
    case
        when to_number(codigo_industria) between 1 and 7   then 'Agricultura, ganaderia, silvicultura y pesca'
        when to_number(codigo_industria) between 8 and 9   then 'Petroleo y minas'
        when to_number(codigo_industria) between 10 and 32 then 'Manufactura'
        when to_number(codigo_industria) = 33              then 'Electricidad y agua'
        when to_number(codigo_industria) = 34              then 'Construccion'
        when to_number(codigo_industria) = 35              then 'Comercio'
        when to_number(codigo_industria) in (42, 43, 44)   then 'Servicios sociales y administracion publica'
        else 'Servicios empresariales y otros servicios'
    end                                                  as sector_macro,
    case
        when to_number(codigo_industria) between 1 and 9   then 'Primario'
        when to_number(codigo_industria) between 10 and 34 then 'Secundario'
        else 'Terciario'
    end                                                  as sector_economico
from ramas
