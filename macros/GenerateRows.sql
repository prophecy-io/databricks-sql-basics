{# =============================================================================
    GenerateRows
    -----------------------------------------------------------------------------
    relation_name  : ''           → stand-alone generator
                    'table_name'  → lateral explode joined to that table

    new_field_name : name for the generated column
    start_expr     : literal or column
    end_expr       : literal or column
    step_expr      : literal or column   (0 is auto-converted to ±1)
    data_type      : int | bigint | double | decimal | date | timestamp
    interval_unit  : day | hour | minute | … (used only for temporal types)
============================================================================= #}

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
{%- set rel_trimmed   = relation_name | trim -%}
{%- set has_table     = rel_trimmed != '' -%}

{# build step expression that auto-flips sign for descending ranges #}
{% if data_type in numeric_types %}
    {% set step_safe = "
        CASE
            WHEN CAST(" ~ step_expr ~ " AS " ~ data_type ~ ") = 0
                 THEN 1
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

(
{% if not has_table %}
    {# ---------- stand-alone generator ---------- #}

    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     {{ step_safe }}
                 )
               ) AS {{ new_field_name }}

    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval {{ step_safe }} {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{% else %}
    {# ---------- lateral explode joined to table ---------- #}

    {% if data_type in numeric_types %}
        SELECT r.* , seq.val AS {{ new_field_name }}
        FROM {{ rel_trimmed }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                {{ step_safe }}
            )
        ) seq

    {% elif data_type in ['date','timestamp'] %}
        SELECT r.* , seq.val AS {{ new_field_name }}
        FROM {{ rel_trimmed }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                interval {{ step_safe }} {{ interval_unit }}
            )
        ) seq

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% endif %}
)
{% endmacro %}