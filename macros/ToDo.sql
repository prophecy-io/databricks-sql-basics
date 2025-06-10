{% macro ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT 1 as error_message
    ) AS dummy
    WHERE raise_error('ToDo: {{ diag_message }}') IS NULL
{% endmacro %}