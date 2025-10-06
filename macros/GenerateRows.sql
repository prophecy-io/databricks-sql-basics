{% macro GenerateRows(
    init_expr,
    condition_expr,
    loop_expr,
    relation_name=None,
    column_name='value',
    max_rows=100000,
    force_mode=None
) %}
    {# ===========================================================
       🧠 PURPOSE
       Replicates Alteryx "Generate Rows" functionality in Databricks SQL.
       Supports both:
         - Linear (arithmetic/date increment)
         - Dynamic (recursive / data-dependent) generation.
       =========================================================== #}

    {# ===========================================================
       🛡️ Normalize inputs: if user passes raw values instead of quoted SQL
       =========================================================== #}
    {% macro _stringify(expr) -%}
        {% if expr is none %}
            None
        {% elif expr is string %}
            {{ expr }}
        {% else %}
            '{{ expr }}'
        {% endif %}
    {%- endmacro %}

    {% set init_expr = _stringify(init_expr) %}
    {% set condition_expr = _stringify(condition_expr) %}
    {% set loop_expr = _stringify(loop_expr) %}

    {# ===========================================================
       🛡️ Safety checks
       =========================================================== #}
    {% if init_expr is none or init_expr == 'None' %}
        {% do exceptions.raise_compiler_error("GenerateRows: `init_expr` must be a SQL string expression.") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == 'None' %}
        {% do exceptions.raise_compiler_error("GenerateRows: `condition_expr` must be a SQL string expression.") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == 'None' %}
        {% do exceptions.raise_compiler_error("GenerateRows: `loop_expr` must be a SQL string expression.") %}
    {% endif %}

    {# ===========================================================
       🧱 Base setup
       =========================================================== #}
    {% set col = DatabricksSqlBasics.safe_identifier(column_name) %}
    {% set unquoted_col = DatabricksSqlBasics.unquote_identifier(column_name) %}
    {% set loop_expr_lc = loop_expr | lower %}
    {% set alias = "src" %}

    {# ===========================================================
       ⚙️ Mode Detection
       =========================================================== #}
    {% if force_mode == 'linear'
       or ('+' in loop_expr_lc and unquoted_col not in loop_expr_lc)
       or ('interval' in loop_expr_lc and unquoted_col not in loop_expr_lc)
    %}

    -- ========================================
    -- 🟢 LINEAR / ARITHMETIC MODE
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
    -- 🔵 DYNAMIC / RECURSIVE MODE
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