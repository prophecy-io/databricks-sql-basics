{% macro GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value<=1',
    loop_expr='value+1',
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
       🛡️ Normalize inputs: stringify unquoted expressions
       =========================================================== #}
    {% if init_expr is none %}
        {% set init_expr = 'None' %}
    {% elif not (init_expr is string) %}
        {% set init_expr = "'" ~ init_expr ~ "'" %}
    {% endif %}

    {% if condition_expr is none %}
        {% set condition_expr = 'None' %}
    {% elif not (condition_expr is string) %}
        {% set condition_expr = "'" ~ condition_expr ~ "'" %}
    {% endif %}

    {% if loop_expr is none %}
        {% set loop_expr = 'None' %}
    {% elif not (loop_expr is string) %}
        {% set loop_expr = "'" ~ loop_expr ~ "'" %}
    {% endif %}

    {# ===========================================================
       🛡️ Safety checks
       =========================================================== #}
    {% if init_expr == 'None' %}
        {% do exceptions.raise_compiler_error("GenerateRows: `init_expr` must be a SQL string expression.") %}
    {% endif %}
    {% if condition_expr == 'None' %}
        {% do exceptions.raise_compiler_error("GenerateRows: `condition_expr` must be a SQL string expression.") %}
    {% endif %}
    {% if loop_expr == 'None' %}
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
        case
            -- ✅ If loop_expr already has 'interval' or date keywords, use as-is
            when lower({{ loop_expr }}) like '%interval%'
              or lower({{ loop_expr }}) like '%day%'
              or lower({{ loop_expr }}) like '%month%'
              or lower({{ loop_expr }}) like '%year%' then
                cast({{ init_expr }} + ({{ loop_expr }}) as date)
            -- ✅ Otherwise, just treat as numeric offset (days)
            else cast({{ init_expr }} + interval (_iter - 1) day as date)
        end
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
        case
            -- ✅ If loop_expr already has 'interval' or date keywords, use as-is
            when lower({{ loop_expr }}) like '%interval%'
              or lower({{ loop_expr }}) like '%day%'
              or lower({{ loop_expr }}) like '%month%'
              or lower({{ loop_expr }}) like '%year%' then
                cast({{ init_expr }} + ({{ loop_expr }}) as date)
            -- ✅ Otherwise, just treat as numeric offset (days)
            else cast({{ init_expr }} + interval (_iter - 1) day as date)
        end
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