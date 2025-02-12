{% macro evaluate_expression(expression) %}

{% set sql_query = 'select ' ~ expression ~ ' as result' %}
{% set result = run_query(sql_query) %}

 
{% if result %}
  {% for row in result %}
    {{row['result']}}
  {% endfor %}
   
{% else %}
  {{ log("Query failed or returned no results", info = True) }}
  {{ log(expression, info = True) }}
{% endif %}
{% endmacro %}

 