{% macro GenerateRows(
    relation_name=None,
    init_expr='1',
    condition_expr='value <= 10',
    loop_expr='value + 1',
    column_name='value',
    max_rows=100000,
    focus_mode=None
) %}
    {% if init_expr is none or init_expr == '' %}
        {% do exceptions.raise_compiler_error("init_expr is required") %}
    {% endif %}
    {% if condition_expr is none or condition_expr == '' %}
        {% do exceptions.raise_compiler_error("condition_expr is required") %}
    {% endif %}
    {% if loop_expr is none or loop_expr == '' %}
        {% do exceptions.raise_compiler_error("loop_expr is required") %}
    {% endif %}
    {% if max_rows is none or max_rows == '' %}
        {% set max_rows = 100000 %}
    {% endif %}

    {% set col = DatabricksSqlBasics.safe_identifier(column_name) %}
    {% set unquoted_col = DatabricksSqlBasics.unquote_identifier(column_name) %}
    {% set alias = "src" %}

    {# detect date/timestamp vs numeric by literal shape (simple heuristic) #}
    {% set is_timestamp = " " in init_expr %}
    {% set is_date = ("-" in init_expr) and not is_timestamp %}
    {% set init_strip = init_expr.strip() %}

    {% if init_strip.startswith("'") or init_strip.startswith('"') %}
        {% set init_value = init_strip %}
    {% else %}
        {% set init_value = "'" ~ init_strip ~ "'" %}
    {% endif %}

    {% if is_timestamp %}
        {% set init_select = "to_timestamp(" ~ init_value ~ ")" %}
    {% elif is_date %}
        {% set init_select = "to_date(" ~ init_value ~ ")" %}
    {% else %}
        {% set init_select = init_expr %}
    {% endif %}

    {# normalize condition quotes if user used double quotes #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {# --- Build expressions that reference the previous value correctly --- #}
    {# When used inside the recursive CTE, references to the generated column should be prefixed with "gen." #}
    {% set next_expr_gen = loop_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) %}
    {% set cond_expr_final = condition_expr_sql %}

    {# --- relation_name provided: use struct(payload) to carry original row without column collisions --- #}
    {% if relation_name %}
        with recursive gen as (
            -- base: keep the whole input row inside a single struct column called payload,
            -- plus the generated initial value and iteration counter
            select
                struct({{ alias }}.*) as payload,
                {{ init_select }} as {{ col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            -- recursive step: keep the same payload, compute next value from prior row (gen.{{ col }})
            select
                gen.payload as payload,
                {{ next_expr_gen }} as {{ col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        -- final projection: expand payload.* and include the generated column
        select
            payload.*,
            {{ col }}
        from gen
        where {{ cond_expr_final }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ col }}, 1 as _iter
            union all
            select
                {{ next_expr_gen }} as {{ col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ col }}
        from gen
        where {{ cond_expr_final }}
    {% endif %}
{% endmacro %}