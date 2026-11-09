{{ config(materialized='table') }}

{#- GOLD - dim_corte: version del F104 con la que se observa cada mes (vintage). -#}

select
    corte_key,
    tipo_corte,
    descripcion_corte,
    dias_aprox_desde_cierre,
    es_definitivo
from (
    values
        (1, 'temprano_15d', 'Corte temprano: declaraciones recibidas ~15 dias despues del cierre del mes fiscal', 15, false),
        (2, 'definitivo',   'Corte definitivo: F104 consolidado con declaraciones tardias y rectificatorias',     null, true)
) as t (corte_key, tipo_corte, descripcion_corte, dias_aprox_desde_cierre, es_definitivo)
