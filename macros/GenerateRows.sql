{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr     = none,      -- literal 2025-01-01  or column
        end_expr       = none,
        step_expr      = 1,         -- must be a numeric literal for dates
        data_type      = 'int',     -- int|bigint|double|decimal|date|timestamp
        interval_unit  = 'day'      -- for date/timestamp
) %}
{%- set nums = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_table = relation_name | trim != '' -%}

{% if data_type in nums %}
    {% set cast_start = "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")" %}
    {% set cast_end   = "CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")" %}
    {% set step_part  = step_expr %}
{% else %}
    {% set cast_start = (
           "DATE '" ~ start_expr ~ "'"      if data_type=='date'       and start_expr.isdigit()==False and "'" not in start_expr
       else "TIMESTAMP '" ~ start_expr ~ "'" if data_type=='timestamp' and start_expr.isdigit()==False and "'" not in start_expr
       else start_expr ) %}
    {% set cast_end = (
           "DATE '" ~ end_expr ~ "'"      if data_type=='date'       and end_expr.isdigit()==False and "'" not in end_expr
       else "TIMESTAMP '" ~ end_expr ~ "'" if data_type=='timestamp' and end_expr.isdigit()==False and "'" not in end_expr
       else end_expr ) %}
    {% set step_part = step_expr ~ ' ' ~ interval_unit %}
{% endif %}

(
{% if not has_table %}
    {% if data_type in nums %}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_part }})) AS {{ new_field_name }}
    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }},
                                interval {{ step_part }})) AS {{ new_field_name }}
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% else %}
    {% if data_type in nums %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_part }})) t AS val
    {% elif data_type in ['date','timestamp'] %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }},
                                      interval {{ step_part }})) t AS val
    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}
{% endif %}
)
{% endmacro %}