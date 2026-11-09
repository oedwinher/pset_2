{{ config(severity='warn') }}
-- Coherencia del nowcasting: el corte temprano no puede superar al definitivo (el definitivo
-- acumula las mismas declaraciones mas las tardias). Se reporta como advertencia, no como error,
-- porque una rectificatoria a la baja podria violarlo legitimamente.
select codigo_industria, tiempo_key, ventas_nominales_usd, ratio_temprano_definitivo
from {{ ref('fct_ventas_mensuales') }}
where flag_temprano_mayor_definitivo
