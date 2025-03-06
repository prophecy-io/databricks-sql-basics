{% macro evaluate_expression(expression) %}
{% set sql_query = 'select ' ~ expression ~ ' as result' %}
{% set result = run_query(sql_query) %}
{% if result %}
  {% if execute %}
    {% for row in result.rows %}
      {{row[0]}}
    {% endfor %}
  {% endif %}
{% else %}
  {# Added to fake dbt query at compile time #}
  {{ "`" ~ expression ~ "`" | string }}
{% endif %}
{% endmacro %}