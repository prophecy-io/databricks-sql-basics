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

{#------------ step expression (auto-flip sign) -----------------------------#}
{% if data_type in numeric %}
    {% set step_safe = "
        CASE WHEN CAST(" ~ step_expr ~ " AS " ~ data_type ~ ") = 0
             THEN 1
             WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                  CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
             THEN -ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
             ELSE  ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
        END" %}
{% else %}
    {% set step_safe = "
        CASE WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                  CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
             THEN -ABS(CAST(" ~ step_expr ~ " AS INT))
             ELSE  ABS(CAST(" ~ step_expr ~ " AS INT))
        END" %}
{% endif %}

{#------------ helpers for date literals ------------------------------------#}
{% macro _cast(expr) %}
    {% if data_type in ['date','timestamp'] and expr is string and not (expr.startswith("'") or expr.startswith("DATE")) %}
        CAST('{{ expr }}' AS {{ data_type }})
    {% else %}
        CAST({{ expr }} AS {{ data_type }})
    {% endif %}
{% endmacro %}

(
{% if not has_table %}
    {% if data_type in numeric %}
        SELECT explode(
                 sequence(
                     {{ _cast(start_expr) }},
                     {{ _cast(end_expr)   }},
                     {{ step_safe }}
                 )
               ) AS {{ new_field_name }}

    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(
                 sequence(
                     {{ _cast(start_expr) }},
                     {{ _cast(end_expr)   }},
                     interval ({{ step_safe }}) {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{% else %}
    {% if data_type in numeric %}
        SELECT r.*,
               val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(
            sequence(
                {{ _cast(start_expr) }},
                {{ _cast(end_expr)   }},
                {{ step_safe }}
            )
        ) t AS val

    {% elif data_type in ['date','timestamp'] %}
        SELECT r.*,
               val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(
            sequence(
                {{ _cast(start_expr) }},
                {{ _cast(end_expr)   }},
                interval ({{ step_safe }}) {{ interval_unit }}
            )
        ) t AS val
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% endif %}
)
{% endmacro %}