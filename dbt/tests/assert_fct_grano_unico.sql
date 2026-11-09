-- Grano de la tabla de hechos: rama x mes x corte. Devuelve las combinaciones duplicadas (debe estar vacio).
select codigo_industria, tiempo_key, corte_key, count(*) as n
from {{ ref('fct_ventas_mensuales') }}
group by 1, 2, 3
having count(*) > 1
