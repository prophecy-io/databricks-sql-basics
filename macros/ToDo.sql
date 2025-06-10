{% macro ToDo(diag_message) %}
  {{ return("SELECT raise_error('" ~ diag_message ~ "')") }}
{% endmacro %}