{% macro GenerateRows(
        relation_name,
        new_field_name,
        start_expr,
        end_expr,
        step_expr      = 1,
        data_type      = 'int',
        interval_unit  = 'day'
) %}
{%- set nums = ['int','integer','bigint','float','double','decimal'] -%}
{%- set has_tbl = relation_name | trim != '' -%}

{%- set is_lit = lambda x: x[0]|int(default=-1) > -1 and '-' in x -%}

{%- if data_type in nums -%}
    {%- set cast_start = "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr -%}
{%- else -%}
    {%- set s_tok = "'" ~ start_expr ~ "'" if is_lit(start_expr|string) else start_expr -%}
    {%- set e_tok = "'" ~ end_expr   ~ "'" if is_lit(end_expr|string)   else end_expr   -%}
    {%- set cast_start = "CAST(" ~ s_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ e_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr ~ ' ' ~ interval_unit -%}
{%- endif -%}

(
{%- if not has_tbl -%}
    {%- if data_type in nums -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_val }})) AS {{ new_field_name }}
    {%- else -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, interval {{ step_val }})) AS {{ new_field_name }}
    {%- endif -%}
{%- else -%}
    {%- if data_type in nums -%}
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