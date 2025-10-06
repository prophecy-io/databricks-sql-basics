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

    {% if relation_name %}
        with recursive gen as (
            select {{ alias }}.*, {{ init_expr }} as {{ col }}, 1 as _iter
            from {{ relation_name }} {{ alias }}
            union all
            select g_src.*, {{ loop_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }} as {{ col }}, _iter + 1
            from gen g_src
            where {{ condition_expr | replace(unquoted_col, 'g_src.' ~ unquoted_col) }}
              and _iter < {{ max_rows | int }}
        )
        select {{ col }} from gen
    {% else %}
        with recursive gen as (
            select {{ init_expr }} as {{ col }}, 1 as _iter
            union all
            select {{ loop_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }} as {{ col }}, _iter + 1
            from gen
            where {{ condition_expr | replace(unquoted_col, 'gen.' ~ unquoted_col) }}
              and _iter < {{ max_rows | int }}
        )
        select {{ col }} from gen
    {% endif %}
{% endmacro %}