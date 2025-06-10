{% macro ToDo(diag_messaage) %}
 select raise_error('{{diag_message}}')
{% endmacro %}



