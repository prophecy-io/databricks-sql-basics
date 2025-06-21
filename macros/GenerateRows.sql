{% macro GenerateRows(
        relation_name,
        new_field_name,
        start_expr,
        end_expr,
        step_expr      = 1,
        data_type      = 'int',
        interval_unit  = 'day'
) %}

{%- set num_types = ['int','integer','bigint','float','double','decimal'] -%}
{%- set tbl       = relation_name | trim -%}
{%- set has_tbl   = tbl != '' -%}

{%- if data_type in num_types -%}
    {%- set cast_start = "CAST(" ~ start_expr ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr -%}
{%- else -%}
    {%- set s_tok = ("'" ~ start_expr ~ "'")
                    if ("-" in start_expr or ":" in start_expr) and "'" not in start_expr
                    else start_expr -%}
    {%- set e_tok = ("'" ~ end_expr ~ "'")
                    if ("-" in end_expr   or ":" in end_expr)   and "'" not in end_expr
                    else end_expr -%}
    {%- set cast_start = "CAST(" ~ s_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ e_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_val   = step_expr ~ ' ' ~ interval_unit -%}
{%- endif -%}

(
{%- if not has_tbl -%}
    {%- if data_type in num_types -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_val }}))
               AS {{ new_field_name }}
    {%- else -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }},
                                interval {{ step_val }}))
               AS {{ new_field_name }}
    {%- endif -%}
{%- else -%}
    {%- if data_type in num_types -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ tbl }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_val }}))
                    t AS val
    {%- else -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ tbl }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }},
                                      interval {{ step_val }}))
                    t AS val
    {%- endif -%}
{%- endif -%}
)
{% endmacro %}