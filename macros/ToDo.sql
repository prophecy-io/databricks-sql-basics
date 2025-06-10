{% macro ToDo(diag_message) %}
 select raise_error('{{diag_message}}') as error_message
{% endmacro %}



