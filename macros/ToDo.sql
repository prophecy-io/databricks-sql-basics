{% macro ToDo(relation, schema, error_string, code_string) %}
 select
     {% for column in schema %}
         CAST(NULL AS {{ column.type }}) as {{ column.name }}{% if not loop.last %},{% endif %}
     {% endfor %}
 from {{ relation_name }}
{% endmacro %}



