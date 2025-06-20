{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr     = none,
        end_expr       = none,
        step_expr      = '1',
        data_type      = 'int',
        interval_unit  = 'day'
) %}
{%- set numeric = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_table = relation_name | trim != '' -%}

{# build step expression ---------------------------------------------------- #}
{% if data_type in numeric %}
    {% set step_safe = "
        CASE
            WHEN CAST(" ~ step_expr  ~ " AS " ~ data_type ~ ") = 0 THEN 1
            WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                 CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
                 THEN -ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
            ELSE  ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
        END" %}
{% else %}
    {% set step_safe = "
        CASE
            WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                 CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
                 THEN -ABS(CAST(" ~ step_expr ~ " AS INT))
            ELSE  ABS(CAST(" ~ step_expr ~ " AS INT))
        END" %}
{% endif %}

{# wrap date/timestamp literals with DATE/TIMESTAMP syntax ------------------ #}
{% if data_type in ['date','timestamp'] %}
    {% set start_sql = (
         "DATE '" ~ start_expr ~ "'" if start_expr is string and not start_expr|string.startswith(("'", 'DATE ', 'TIMESTAMP '))
         else start_expr
    ) %}
    {% set end_sql = (
         "DATE '" ~ end_expr ~ "'" if end_expr is string and not end_expr|string.startswith(("'", 'DATE ', 'TIMESTAMP '))
         else end_expr
    ) %}
{% else %}
    {% set start_sql = start_expr %}
    {% set end_sql   = end_expr %}
{% endif %}

(
{% if not has_table %}
    {% if data_type in numeric %}
        SELECT explode(sequence(CAST({{ start_sql }} AS {{ data_type }}),
                                 CAST({{ end_sql   }} AS {{ data_type }}),
                                 {{ step_safe }})) AS {{ new_field_name }}
    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(sequence(CAST({{ start_sql }} AS {{ data_type }}),
                                 CAST({{ end_sql   }} AS {{ data_type }}),
                                 interval ({{ step_safe }}) {{ interval_unit }})) AS {{ new_field_name }}
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% else %}
    {% if data_type in numeric %}
        SELECT r.*,
               val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(
            sequence(CAST({{ start_sql }} AS {{ data_type }}),
                     CAST({{ end_sql   }} AS {{ data_type }}),
                     {{ step_safe }})) t AS val
    {% elif data_type in ['date','timestamp'] %}
        SELECT r.*,
               val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(
            sequence(CAST({{ start_sql }} AS {{ data_type }}),
                     CAST({{ end_sql   }} AS {{ data_type }}),
                     interval ({{ step_safe }}) {{ interval_unit }})) t AS val
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% endif %}
)
{% endmacro %}