{% macro ToDo(diag_message) %}
    SELECT *
    FROM (
        SELECT 1
    ) AS dummy
    WHERE raise_error('Fix ToDo gem: {{ diag_message }}') IS NULL
{% endmacro %}