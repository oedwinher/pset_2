{% test value_between(model, column_name, min_value, max_value) %}
{#- Falla por cada fila fuera de [min_value, max_value]; los NULL se ignoran (usar not_null aparte). -#}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
