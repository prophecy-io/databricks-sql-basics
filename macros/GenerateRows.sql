{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr     = none,
        end_expr       = none,
        step_expr      = '1',
        data_type      = 'int',
        interval_unit  = 'day'
) %}

{%- set numeric_types = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_table     = relation_name | trim != '' -%}

{# ---------- safe step (auto-flip sign if descending) ----------------------- #}
{% if data_type in numeric_types %}
    {% set step_safe = "CASE WHEN (" ~ step_expr ~ ") = 0 THEN 1 " ~
                       "WHEN (" ~ start_expr ~ ") > (" ~ end_expr ~ ") " ~
                       "THEN -ABS(" ~ step_expr ~ ") ELSE ABS(" ~ step_expr ~ ") END" %}
{% else %}
    {% set step_safe = "CASE WHEN (" ~ start_expr ~ ") > (" ~ end_expr ~ ") " ~
                       "THEN -ABS(CAST(" ~ step_expr ~ " AS INT)) " ~
                       "ELSE  ABS(CAST(" ~ step_expr ~ " AS INT)) END" %}
{% endif %}

{# ---------- wrap date/timestamp literals with quotes ----------------------- #}
{% if data_type in ['date','timestamp'] %}
    {% set cast_start = (
           "CAST('" ~ start_expr ~ "' AS " ~ data_type ~ ")"
           if "'" not in start_expr and "." not in start_expr and "(" not in start_expr
           else "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")"
    ) %}
    {% set cast_end = (
           "CAST('" ~ end_expr ~ "' AS " ~ data_type ~ ")"
           if "'" not in end_expr and "." not in end_expr and "(" not in end_expr
           else "CAST(" ~ end_expr ~ " AS " ~ data_type ~ ")"
    ) %}
{% else %}
    {% set cast_start = "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")" %}
    {% set cast_end   = "CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")" %}
{% endif %}

(
{% if not has_table %}
    {# ---------------- stand-alone generator ------------------------------- #}
    {% if data_type in numeric_types %}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_safe }})) AS {{ new_field_name }}
    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }},
                                interval ({{ step_safe }}) {{ interval_unit }})) AS {{ new_field_name }}
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% else %}
    {# ---------------- per-row generator (retain all columns) --------------- #}
    {% if data_type in numeric_types %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_safe }})) t AS val
    {% elif data_type in ['date','timestamp'] %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }},
                                      interval ({{ step_safe }}) {{ interval_unit }})) t AS val
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% endif %}
)
{% endmacro %}