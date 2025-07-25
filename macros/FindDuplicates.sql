{%- macro FindDuplicates(
    relation_name,
    column_names,
    column_group_condition,
    output_type,
    grouped_count,
    lower_limit,
    upper_limit,
    generationMethod,
    schema_columns,
    orderByRules
) %}

    {{ log("Applying Window Function on selected columns", info=True) }}

    --{%- set partition_columns_str = column_names | join(', ') -%}
    {%- set order_parts = [] -%}
    {%- if generationMethod == "allCols" -%}
        {%- do order_parts.append("1") -%}
    {%- else -%}
        {%- for r in orderByRules -%}
            {%- if r.expr | trim != '' -%}
                {%- set part = r.expr | trim ~ " " -%}
                {%- if r.sort == 'asc' -%}
                    {%- set part = part ~ "asc" -%}
                {%- elif r.sort == 'asc_nulls_last' -%}
                    {%- set part = part ~ "asc nulls last" -%}
                {%- elif r.sort == 'desc_nulls_first' -%}
                    {%- set part = part ~ "desc nulls first" -%}
                {%- else -%}
                    {%- set part = part ~ "desc" -%}
                {%- endif -%}
                {%- do order_parts.append(part) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {%- set window_order_by_str = order_parts | join(', ')-%}

    {%- if window_order_by_str == '' -%}
        {%- set window_order_by_str_param = '' -%}
    {%- else -%}
        {%- set window_order_by_str_param = 'ORDER BY ' ~ window_order_by_str -%}
    {%- endif -%}


    {%- set window_partition_by_str -%}
        {%- if generationMethod == "allCols" -%}
            PARTITION BY {{ schema_columns | join(', ') }}
        {%- else -%}
            PARTITION BY {{ column_names | join(', ') }}
        {% endif %}
    {% endset %}

    {%- set select_window_cte -%}
        {%- if output_type == "custom" -%}
            WITH select_cte1 AS(
                SELECT *, COUNT(*) OVER({{ window_partition_by_str }}) AS group_count FROM {{ relation_name }}
            )
        {%- else -%}
            WITH select_cte1 AS(
                SELECT *, row_number() OVER({{ window_partition_by_str }} {{ window_order_by_str_param }}) AS row_num FROM {{relation_name }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set select_window_filter -%}
        {%- if output_type == "custom" -%}
            {%- if column_group_condition == "between" -%}
                SELECT * FROM select_cte1 WHERE group_count BETWEEN {{ lower_limit }} AND {{ upper_limit }}
            {%-elif column_group_condition == "equal_to" -%}
                SELECT * FROM select_cte1 WHERE group_count = {{ grouped_count }}
            {%-elif column_group_condition == "not_equal_to" -%}
                SELECT * FROM select_cte1 WHERE group_count <> {{ grouped_count }}
            {%-elif column_group_condition == "less_than" -%}
                SELECT * FROM select_cte1 WHERE group_count < {{ grouped_count }}
            {%-elif column_group_condition == "greater_than" -%}
                SELECT * FROM select_cte1 WHERE group_count > {{ grouped_count }}
            {%- endif -%}
        {%- elif output_type == "unique" -%}
            SELECT * FROM select_cte1 WHERE row_num = 1
        {%- elif output_type == "duplicate" -%}
            SELECT * FROM select_cte1 WHERE row_num > 1
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_window_cte ~ "\n" ~ select_window_filter -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}

