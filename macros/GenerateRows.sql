{% macro GenerateRows(
        relation_name,          -- table or '' for stand-alone
        new_field_name,         -- column to create
        start_expr,             -- literal 2025-01-01  or column
        end_expr,               -- literal or column
        step_expr      = 1,     -- literal or column
        data_type      = 'int', -- int / bigint / float / decimal / date / timestamp
        interval_unit  = 'day'  -- for date / timestamp
) %}
{%- set nums = ['int','integer','bigint','float','double','decimal'] -%}
{%- set tbl  = relation_name | trim -%}
{%- set has_tbl = tbl != '' -%}

{%- set s_tok = start_expr|string -%}
{%- set e_tok = end_expr|string -%}

{%- if data_type not in nums -%}
    {%- if ("-" in s_tok or ":" in s_tok) and ("'" not in s_tok) and ("(" not in s_tok) -%}
        {%- set s_tok = "'" ~ s_tok ~ "'" -%}
    {%- endif -%}
    {%- if ("-" in e_tok or ":" in e_tok) and ("'" not in e_tok) and ("(" not in e_tok) -%}
        {%- set e_tok = "'" ~ e_tok ~ "'" -%}
    {%- endif -%}
{%- endif -%}

{%- if data_type in nums -%}
    {%- set cast_start = "CAST(" ~ s_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ e_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_part  = step_expr -%}
{%- else -%}
    {%- set cast_start = "CAST(" ~ s_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set cast_end   = "CAST(" ~ e_tok ~ " AS " ~ data_type ~ ")" -%}
    {%- set step_part  = step_expr ~ ' ' ~ interval_unit -%}
{%- endif -%}

(
{%- if not has_tbl -%}

    {%- if data_type in nums -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_part }})) AS {{ new_field_name }}
    {%- else -%}
        SELECT explode(sequence({{ cast_start }}, {{ cast_end }}, interval {{ step_part }})) AS {{ new_field_name }}
    {%- endif -%}

{%- else -%}

    {%- if data_type in nums -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ tbl }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, {{ step_part }})) t AS val
    {%- else -%}
        SELECT r.*, val AS {{ new_field_name }}
        FROM {{ tbl }} AS r
        LATERAL VIEW explode(sequence({{ cast_start }}, {{ cast_end }}, interval {{ step_part }})) t AS val
    {%- endif -%}

{%- endif -%}
)
{% endmacro %}