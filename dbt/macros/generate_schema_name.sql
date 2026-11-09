{#-
  Usa el nombre de esquema personalizado tal cual (BRONZE / SILVER / GOLD) en lugar del
  comportamiento por defecto de dbt (<schema_del_target>_<schema_personalizado>).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim | upper }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
