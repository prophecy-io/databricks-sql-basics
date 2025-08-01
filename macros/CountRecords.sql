{%- macro CountRecords(relation_name,
    column_names,
    count_method
) %}
    {{ log("Computing record count from the table " ~ relation_name, info=True) }}
    {%- set select_query = "SELECT COUNT(*) AS total_records FROM " ~ relation_name -%}

    {%- if count_method == "count_non_null_records" -%}
        {{ log("Computing non null records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- do withColumn_clause.append("COUNT(" ~ column ~ ") AS " ~ column ~ "_count") -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ relation_name -%}

    {%- elif count_method == "count_distinct_records" -%}
        {{ log("Computing distinct records count from the table for each column", info=True) }}
        {%- set withColumn_clause = [] -%}
        {% for column in column_names %}
            {%- do withColumn_clause.append("COUNT(DISTINCT " ~ column ~ ") AS " ~ column ~ "_distinct_count") -%}
        {% endfor %}
        {%- set arg_string = withColumn_clause | join(', ') -%}
        {%- set select_query = "SELECT " ~ arg_string ~ " FROM " ~ relation_name -%}
    {%- endif -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(select_query, info=True) }}

    {{ return(select_query) }}
{%- endmacro -%}