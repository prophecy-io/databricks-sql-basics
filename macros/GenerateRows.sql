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

    {% set alias = "src" %}
    {% set unquoted_col = DatabricksSqlBasics.unquote_identifier(column_name) %}
    {% set internal_col = "__gen_" ~ unquoted_col %}   {# internal alias to prevent duplicate column name #}

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

    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}

    {% if relation_name %}
        with recursive gen as (
            -- base case: one row per input record
            select
                struct({{ alias }}.*) as payload,
                {{ init_select }} as {{ internal_col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            -- recursive step
            select
                gen.payload as payload,
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select
            payload.*,
            {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ internal_col }}, 1 as _iter
            union all
            select
                {{ loop_expr | replace(unquoted_col, 'gen.' ~ internal_col) }} as {{ internal_col }},
                _iter + 1
            from gen
            where _iter < {{ max_rows | int }}
        )
        select {{ internal_col }} as {{ unquoted_col }}
        from gen
        where {{ condition_expr_sql | replace(unquoted_col, internal_col) }}
    {% endif %}
{% endmacro %}