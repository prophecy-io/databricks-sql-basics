{% macro GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value<=10',
    loop_expr='1',
    column_name='value',
    max_rows=100000,
    force_mode=None
) %}

    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("GenerateRows: init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("GenerateRows: condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("GenerateRows: loop_expr is required") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set col = DatabricksSqlBasics.safe_identifier(column_name) %}
    {% set unquoted_col = DatabricksSqlBasics.unquote_identifier(column_name) %}
    {% set loop_expr_lc = loop_expr | lower %}
    {% set alias = "src" %}

    {% set self_ref = unquoted_col in loop_expr_lc %}

    {% if (force_mode == 'linear' and not self_ref)
       or (not self_ref and 'interval' not in loop_expr_lc) %}
    {% if relation_name %}
        with base as (
            select * from {{ relation_name }}
        ),
        gen as (
            select b.*, explode(sequence(1, {{ max_rows | int }})) as _iter
            from base b
        ),
        calc as (
            select
                *,
                case
                    when '{{ init_expr | lower }}' like '%-%' or '{{ loop_expr_lc }}' like '%interval%' then
                        date_add(to_date({{ init_expr }}), (_iter - 1) * coalesce(cast(regexp_extract('{{ loop_expr }}','[0-9]+',0) as int), 1))
                    else
                        (cast({{ init_expr }} as double) + (_iter - 1) * cast({{ loop_expr }} as double))
                end as {{ col }}
            from gen
        )
        select * from calc where {{ condition_expr }} order by _iter
    {% else %}
        with gen as (
            select explode(sequence(1, {{ max_rows | int }})) as _iter
        ),
        calc as (
            select
                case
                    when '{{ init_expr | lower }}' like '%-%' or '{{ loop_expr_lc }}' like '%interval%' then
                        date_add(to_date({{ init_expr }}), (_iter - 1) * coalesce(cast(regexp_extract('{{ loop_expr }}','[0-9]+',0) as int), 1))
                    else
                        (cast({{ init_expr }} as double) + (_iter - 1) * cast({{ loop_expr }} as double))
                end as {{ col }},
                _iter
            from gen
        )
        select * from calc where {{ condition_expr }} order by _iter
    {% endif %}
    {% else %}
    {% if relation_name %}
        with recursive gen as (
            select {{ alias }}.*, {{ init_expr }} as {{ col }}, 1 as iteration
            from {{ relation_name }} {{ alias }}
            union all
            select g_src.*, {{ loop_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }} as {{ col }}, iteration + 1
            from gen g_src
            where {{ condition_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }}
              and iteration < {{ max_rows | int }}
        )
        select * from gen order by iteration
    {% else %}
        with recursive gen as (
            select {{ init_expr }} as {{ col }}, 1 as iteration
            union all
            select {{ loop_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }} as {{ col }}, iteration + 1
            from gen
            where {{ condition_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }}
              and iteration < {{ max_rows | int }}
        )
        select * from gen order by iteration
    {% endif %}
    {% endif %}
{% endmacro %}