{% macro GenerateRows(
        relation_name,
        new_field_name,
        start_expr,
        end_expr,
        step_expr         = 1,
        data_type         = 'int',
        interval_unit     = 'day'
) %}
{%- set numeric = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_tbl = relation_name | trim != '' -%}

{%- if data_type in numeric -%}
    {%- set cast_start = "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr -%}
{%- else -%}
    {%- set s_lit = ("'" ~ start_expr ~ "'") if "'" not in start_expr else start_expr -%}
    {%- set e_lit = ("'" ~ end_expr   ~ "'") if "'" not in end_expr   else end_expr   -%}
    {%- set cast_start = "CAST(" ~ s_lit ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ e_lit ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr ~ ' ' ~ interval_unit -%}
{%- endif -%}

(
{%- if not has_tbl -%}
    {%- if data_type in numeric -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_val }})) AS {{ new_field_name }}
    {%- else -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, interval {{ step_val }})) AS {{ new_field_name }}
    {%- endif -%}
{%- else -%}
    {%- if data_type in numeric -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_val }})) t AS val
    {%- else -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ relation_name }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, interval {{ step_val }})) t AS val
    {%- endif -%}
{%- endif -%}
)
{% endmacro %}