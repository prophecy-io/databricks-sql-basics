{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr,
        end_expr,
        step_expr      = 1,
        data_type      = 'int',
        interval_unit  = 'day'
) %}
{%- set nums = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_tbl = relation_name | trim != '' -%}

{# quote helper — inline, no nested macros #}
{% set s = ("'" ~ start_expr ~ "'") if data_type not in nums
           and "-" in start_expr and "'" not in start_expr else start_expr %}
{% set e = ("'" ~ end_expr   ~ "'") if data_type not in nums
           and "-" in end_expr   and "'" not in end_expr   else end_expr %}

{% if data_type in nums %}
    {% set step_val = step_expr %}
{% else %}
    {% set step_val = step_expr ~ ' ' ~ interval_unit %}
{% endif %}

(
{% if not has_tbl %}
    {% if data_type in nums %}
        SELECT explode(sequence(CAST({{ s }} AS {{ data_type }}),
                                 CAST({{ e }} AS {{ data_type }}),
                                 {{ step_val }})) AS {{ new_field_name }}
    {% else %}
        SELECT explode(sequence(CAST({{ s }} AS {{ data_type }}),
                                 CAST({{ e }} AS {{ data_type }}),
                                 interval {{ step_val }})) AS {{ new_field_name }}
    {% endif %}
{% else %}
    {% if data_type in nums %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence(CAST({{ s }} AS {{ data_type }}),
                                       CAST({{ e }} AS {{ data_type }}),
                                       {{ step_val }})) t AS val
    {% else %}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence(CAST({{ s }} AS {{ data_type }}),
                                       CAST({{ e }} AS {{ data_type }}),
                                       interval {{ step_val }})) t AS val
    {% endif %}
{% endif %}
)
{% endmacro %}