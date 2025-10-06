{% macro GenerateRows(
    relation_name=None,
    init_expr,
    condition_expr,
    loop_expr,
    column_name='value',
    max_rows=100000,
    force_mode=None
) %}
    {# Handle special chars or spaces in column names safely #}
    {% set col = DatabricksSqlBasics.safe_identifier(column_name) %}
    {% set unquoted_col = DatabricksSqlBasics.unquote_identifier(column_name) %}
    {% set loop_expr_lc = loop_expr | lower %}
    {% set alias = "src" %}

    {# --- Defensive Guards --- #}
    {% if not init_expr %}
        {% do exceptions.raise_compiler_error("Parameter `init_expr` is required") %}
    {% endif %}
    {% if not condition_expr %}
        {% do exceptions.raise_compiler_error("Parameter `condition_expr` is required") %}
    {% endif %}
    {% if not loop_expr %}
        {% do exceptions.raise_compiler_error("Parameter `loop_expr` is required") %}
    {% endif %}

    {% if force_mode == 'linear'
       or ('+' in loop_expr_lc and unquoted_col not in loop_expr_lc)
       or ('interval' in loop_expr_lc and unquoted_col not in loop_expr_lc)
    %}

    -- ========================================
    -- 🟢 Linear / Arithmetic Sequence Mode
    -- ========================================
    {% if relation_name %}
        with base as (
            select * from {{ relation_name }}
        ),
        gen as (
            select
                b.*,
                explode(sequence(1, {{ max_rows }})) as _iter
            from base b
        ),
        calc as (
            select
                *,
                case
                    when typeof({{ init_expr }}) in ('date', 'timestamp') then
                        cast({{ init_expr }} + interval (_iter - 1) * ({{ loop_expr }}) as date)
                    else
                        cast({{ init_expr }} + (_iter - 1) * ({{ loop_expr }}) as double)
                end as {{ col }}
            from gen
        )
        select *
        from calc
        where {{ condition_expr }}
        order by _iter
    {% else %}
        with gen as (
            select explode(sequence(1, {{ max_rows }})) as _iter
        ),
        calc as (
            select
                case
                    when typeof({{ init_expr }}) in ('date', 'timestamp') then
                        cast({{ init_expr }} + interval (_iter - 1) * ({{ loop_expr }}) as date)
                    else
                        cast({{ init_expr }} + (_iter - 1) * ({{ loop_expr }}) as double)
                end as {{ col }},
                _iter
            from gen
        )
        select *
        from calc
        where {{ condition_expr }}
        order by _iter
    {% endif %}

    {% else %}

    -- ========================================
    -- 🔵 Recursive / Non-linear Mode
    -- ========================================
    {% if relation_name %}
        with recursive gen as (
            -- Step 1: Initialize
            select
                {{ alias }}.*,
                {{ init_expr }} as {{ col }},
                1 as iteration
            from {{ relation_name }} {{ alias }}

            union all

            -- Step 2: Iterate
            select
                g_src.*,
                {{ loop_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }} as {{ col }},
                iteration + 1
            from gen g_src
            where {{ condition_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }}
              and iteration < {{ max_rows }}
        )
        select *
        from gen
        order by iteration
    {% else %}
        with recursive gen as (
            select
                {{ init_expr }} as {{ col }},
                1 as iteration

            union all

            select
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }} as {{ col }},
                iteration + 1
            from gen
            where {{ condition_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }}
              and iteration < {{ max_rows }}
        )
        select *
        from gen
        order by iteration
    {% endif %}

    {% endif %}
{% endmacro %}