{% macro ToDo(diag_messaage) %}
 select raise_error('{{diag_message}}') as error_message
{% endmacro %}



