{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr     = none,
        end_expr       = none,
        step_expr      = '1',
        data_type      = 'int',
        interval_unit  = 'day'
    ) %}
{# -------------------------------------------------------------------------
   Setup helpers
------------------------------------------------------------------------- #}
{% set numeric_types = ['int','integer','bigint','float','double','decimal'] %}
{% set rel_trimmed   = relation_name | trim %}
{% set is_table      = rel_trimmed != '' %}

{%- if data_type in numeric_types %}
    {# step expression for numeric types #}
    {% set step_calc = "
        CASE
            WHEN CAST(" ~ step_expr ~ " AS " ~ data_type ~ ") = 0
                 THEN 1
            WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                 CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
                 THEN -ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
            ELSE  ABS(CAST(" ~ step_expr ~ " AS " ~ data_type ~ "))
        END" %}
{%- else %}
    {# step expression for date / timestamp #}
    {% set step_calc = "
        CASE
            WHEN CAST(" ~ start_expr ~ " AS " ~ data_type ~ ") >
                 CAST(" ~ end_expr   ~ " AS " ~ data_type ~ ")
                 THEN -ABS(CAST(" ~ step_expr ~ " AS INT))
            ELSE  ABS(CAST(" ~ step_expr ~ " AS INT))
        END" %}
{%- endif %}

{# -------------------------------------------------------------------------
   Emit SQL
------------------------------------------------------------------------- #}
(
{% if not is_table %}
    {# ---- stand-alone generator (one column) ---- #}

    select 1 as a

{% else %}
    {# ---- cross-join to input table, retain all columns ---- #}

    {% if data_type in numeric_types %}
        select *, 1 as d from {{ rel_trimmed }}

    {% elif data_type in ['date','timestamp'] %}
        select *, 1 as e from {{ rel_trimmed }}

    {% else %}
        select *, 1 as f from {{ rel_trimmed }}
    {% endif %}
{% endif %}
)
{% endmacro %}