{{ config(materialized='table') }}

{#-
  GOLD - fct_ventas_mensuales
  Grano: UNA fila por rama CIIU (46) x mes fiscal x corte (temprano_15d | definitivo).
  Se excluyen las filas "sin clasificar" (centinela '000') porque no pertenecen a ninguna rama.
-#}

select
    md5(s.codigo_industria || '|' || to_varchar(s.fecha_mes) || '|' || s.tipo_corte) as venta_key,
    s.codigo_industria,
    (year(s.fecha_mes) * 100 + month(s.fecha_mes))                                    as tiempo_key,
    c.corte_key,
    s.ventas_nominales_usd,
    s.ventas_reales_usd,
    s.num_declarantes,
    s.ventas_por_declarante_usd,
    s.indice_ipc_sim,
    s.ratio_temprano_definitivo,
    s.flag_ventas_cero_con_declarantes,
    s.flag_temprano_mayor_definitivo
from {{ ref('int_f104_ventas_limpias') }} s
left join {{ ref('dim_corte') }} c on c.tipo_corte = s.tipo_corte
where not s.es_sin_clasificar
