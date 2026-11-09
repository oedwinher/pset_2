-- Conciliacion Silver -> Gold: la tabla de hechos debe tener exactamente las filas clasificadas de Silver.
with s as (select count(*) as n from {{ ref('int_f104_ventas_limpias') }} where not es_sin_clasificar),
     f as (select count(*) as n from {{ ref('fct_ventas_mensuales') }})
select s.n as filas_silver, f.n as filas_fact
from s cross join f
where s.n <> f.n
