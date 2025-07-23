{%- macro MakeColumns(relation_name, relation_schema, num_columns, all_columns, arrangement='horizontal', grouping_fields=[]) -%}

{%- set num_cols = num_columns | default(2) -%}
{%- set arrange_type = arrangement | lower | default('horizontal') -%}

-- Debug: Check if parameters are being passed correctly
{%- if not all_columns or all_columns | length == 0 -%}
    select 'ERROR: all_columns parameter is empty or not provided' as error_message
{%- elif not relation_name -%}
    select 'ERROR: relation_name parameter is empty or not provided' as error_message
{%- else -%}

-- Determine data fields (all fields except grouping fields)
{%- set data_fields = [] -%}
{%- for field in all_columns -%}
    {%- if field not in grouping_fields -%}
        {%- set _ = data_fields.append(field) -%}
    {%- endif -%}
{%- endfor -%}

{%- if data_fields | length == 0 -%}
    select 'ERROR: No data columns found - all columns are in grouping_fields. all_columns: {{ all_columns | join(", ") }}, grouping_fields: {{ grouping_fields | join(", ") }}' as error_message
{%- else -%}

-- Create display expression from all non-grouping fields
{%- if data_fields | length == 1 -%}
    {%- set display_expression = data_fields[0] -%}
{%- else -%}
    {%- set display_expression -%}
    concat(
    {%- for field in data_fields -%}
        coalesce(cast({{ field }} as string), '')
        {%- if not loop.last %}, ' | ', {% endif -%}
    {%- endfor -%}
    )
    {%- endset -%}
{%- endif -%}

{%- if grouping_fields | length > 0 -%}
    -- WITH GROUPING
    with base_data as (
        select
            {%- for group_field in grouping_fields %}
            {{ group_field }},
            {%- endfor %}
            {{ display_expression }} as display_value,
            row_number() over (
                partition by {{ grouping_fields | join(', ') }}
                order by {{ grouping_fields[0] }}
            ) as rn,
            count(*) over (partition by {{ grouping_fields | join(', ') }}) as group_total
        from {{ relation_name }}
        where {{ display_expression }} is not null
    ),

    arranged_data as (
        select
            {%- for group_field in grouping_fields %}
            {{ group_field }},
            {%- endfor %}
            {%- if arrange_type == 'horizontal' %}
            ceiling(rn::float / {{ num_cols }}) as output_row,
            ((rn - 1) % {{ num_cols }}) + 1 as output_col
            {%- else %}
            ((rn - 1) % ceiling(group_total::float / {{ num_cols }})) + 1 as output_row,
            ceiling(rn::float / ceiling(group_total::float / {{ num_cols }})) as output_col
            {%- endif %},
            display_value
        from base_data
    )

    select
        {%- for group_field in grouping_fields %}
        {{ group_field }},
        {%- endfor %}
        output_row
        {%- for col_num in range(1, num_cols + 1) %}
        , max(case when output_col = {{ col_num }} then display_value end) as Column_{{ col_num }}
        {%- endfor %}
    from arranged_data
    group by
        {%- for group_field in grouping_fields %}
        {{ group_field }},
        {%- endfor %}
        output_row
    order by
        {%- for group_field in grouping_fields %}
        {{ group_field }},
        {%- endfor %}
        output_row

{%- else -%}
    -- WITHOUT GROUPING
    with base_data as (
        select
            {{ display_expression }} as display_value,
            row_number() over (order by {{ data_fields[0] }}) as rn,
            count(*) over () as total_rows
        from {{ relation_name }}
        where {{ display_expression }} is not null
    ),

    arranged_data as (
        select
            {%- if arrange_type == 'horizontal' %}
            ceiling(rn::float / {{ num_cols }}) as output_row,
            ((rn - 1) % {{ num_cols }}) + 1 as output_col
            {%- else %}
            ((rn - 1) % ceiling(total_rows::float / {{ num_cols }})) + 1 as output_row,
            ceiling(rn::float / ceiling(total_rows::float / {{ num_cols }})) as output_col
            {%- endif %},
            display_value
        from base_data
    )

    select
        output_row
        {%- for col_num in range(1, num_cols + 1) %}
        , max(case when output_col = {{ col_num }} then display_value end) as Column_{{ col_num }}
        {%- endfor %}
    from arranged_data
    group by output_row
    order by output_row

{%- endif -%}

{%- endif -%}

{%- endif -%}

{%- endmacro -%}