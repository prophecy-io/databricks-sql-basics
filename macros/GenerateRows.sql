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

    {# normalize condition: convert double quotes to single, strip backticked column forms and remove any gen/g_src/alias prefixes so replacement is predictable #}
    {% if '"' in condition_expr and "'" not in condition_expr %}
        {% set condition_expr_sql = condition_expr.replace('"', "'") %}
    {% else %}
        {% set condition_expr_sql = condition_expr %}
    {% endif %}
    {% set condition_expr_sql = condition_expr_sql | replace('`' ~ unquoted_col ~ '`', unquoted_col) %}
    {% set condition_expr_sql = condition_expr_sql | replace('gen.' ~ unquoted_col, unquoted_col) %}
    {% set condition_expr_sql = condition_expr_sql | replace('g_src.' ~ unquoted_col, unquoted_col) %}
    {% set condition_expr_sql = condition_expr_sql | replace(alias ~ '.' ~ unquoted_col, unquoted_col) %}

    {# normalize loop_expr similarly so replacements below are predictable #}
    {% set loop_expr_sql = loop_expr | replace('`' ~ unquoted_col ~ '`', unquoted_col) %}
    {% set loop_expr_sql = loop_expr_sql | replace(alias ~ '.' ~ unquoted_col, unquoted_col) %}

    {# build next-value expression (prefixed with gen. when used in the recursive step) #}
    {% set next_expr_gen = loop_expr_sql | replace(unquoted_col, 'gen.' ~ unquoted_col) %}
    {% set cond_on_next_gen = condition_expr_sql | replace(unquoted_col, next_expr_gen) %}

    {# similarly for the relation case, prefix with g_src. #}
    {% set next_expr_gsrc = loop_expr_sql | replace(unquoted_col, 'g_src.' ~ unquoted_col) %}
    {% set cond_on_next_gsrc = condition_expr_sql | replace(unquoted_col, next_expr_gsrc) %}

    {% if relation_name %}
        with recursive gen as (
            select
                {{ alias }}.*,
                {{ init_select }} as {{ col }},
                1 as _iter
            from {{ relation_name }} {{ alias }}

            union all

            select
                g_src.*,
                {{ next_expr_gsrc }} as {{ col }},
                _iter + 1
            from gen g_src
            where {{ cond_on_next_gsrc }}
              and _iter < {{ max_rows | int }}
        )
        select * from gen
    {% else %}
        with recursive gen as (
            select {{ init_select }} as {{ col }}, 1 as _iter
            union all
            select
                {{ next_expr_gen }} as {{ col }},
                _iter + 1
            from gen
            where {{ cond_on_next_gen }}
              and _iter < {{ max_rows | int }}
        )
        select {{ col }} from gen
    {% endif %}
{% endmacro %}