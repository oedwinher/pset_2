{% test non_negative(model, column_name) %}
{#- Falla por cada fila con valor negativo (regla de negocio: ventas y conteos >= 0). -#}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
