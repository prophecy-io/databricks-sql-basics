{% macro FindDuplicates(
    relation_name,
    columnNames,
    column_group_condition,
    outputType,
    grouped_count,
    lower_limit,
    upper_limit
) %}

    {{ log("Applying Window Function on selected columns", info=True) }}

    {%- if columnNames == [] and column_group_condition == "" and grouped_count == "" and lower_limit == "" and upper_limit == "" -%}
        {{ return("SELECT *  FROM " ~ relation_name) }}
    {%- endif -%}

    {%- set partition_columns_str = columnNames | join(', ') -%}

    {%- set select_window_cte -%}
        {%- if outputType == "Custom_output" -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER(PARTITION BY {{ partition_columns_str }}) AS group_count FROM {{ relation_name }}
            )
        {%- else -%}
            WITH select_cte1 AS(
                SELECT *, row_number() OVER(PARTITION BY {{ partition_columns_str }} ORDER BY 1) AS row_num FROM {{relation_name }} ORDER BY {{ partition_columns_str }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if outputType == "Custom_output" -%}
            {%- if column_group_condition == "between" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_condition == "equal_to" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count = {{ grouped_count }}
            {%-elif column_group_condition == "not_equal_to" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count <> {{ grouped_count }}
            {%-elif column_group_condition == "less_than" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count < {{ grouped_count }}
            {%-elif column_group_condition == "greater_than" -%}
                SELECT * EXCEPT(group_count) FROM select_cte1 WHERE group_count > {{ grouped_count }}
            {%- endif -%}
        {%- elif outputType == "U_output" -%}
            SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num = 1
        {%- elif outputType == "D_output" -%}
            SELECT * EXCEPT(row_num) FROM select_cte1 WHERE row_num > 1
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro %}