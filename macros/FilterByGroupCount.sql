{% macro FilterByGroupCount(
    relation_name,
    columnNames,
    column_group_condition,
    count_value,
    left_limit,
    right_limit
) %}

    {{ log("Applying Window Function on selected columns", info=True) }}

    {%- if columnNames == [] and column_group_condition == "" and count_value == "" and left_limit == "" and right_limit == "" -%}
        {{ return("SELECT *  FROM " ~ relation_name) }}
    {%- endif -%}

    {%- set partition_columns_str = columnNames | join(', ') -%}

    {%- set select_window_cte -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER(PARTITION BY {{ partition_columns_str }}) AS group_count FROM {{ relation_name }}
            )
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if column_group_condition == "between" -%}
            SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count BETWEEN {{ left_limit }} AND {{ right_limit }}
        {%-else -%}
            SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count = {{ count_value }}
        {%- endif -%}
    {%- endset -%}


    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro %}