{% macro BulkColumnRename(relation, columnNames, renameMethod, prefix='', suffix='', customExpression='') %}
    {% set renamed_columns = [] %}

    {% for column in columnNames %}
        {% if renameMethod == 'Add Prefix' %}
            {% set renamed_column = "`" ~ column ~ "`" ~ " AS `" ~ prefix ~ column ~ "`" %}

        {% elif renameMethod == 'Add Suffix' %}
            {% set renamed_column = "`" ~ column ~ "`" ~ " AS `" ~ column ~ suffix ~ "`" %}

        {% elif renameMethod == 'Custom Expression' %}
            {% set custom_expr_result = DatabricksSqlBasics.evaluate_expression(customExpression | replace('column_name',  "`" ~ column ~ "`")) %}
            {% set custom_expr_result_trimmed = custom_expr_result | trim %}
            {{ log(custom_expr_result_trimmed, info = True) }}
            {% set renamed_column = "`" ~ column ~ "`" ~ " as " ~ "`" ~ custom_expr_result_trimmed ~ "`" %}
        
        {% endif %}
        
        {% do renamed_columns.append(renamed_column) %}
    {% endfor %}

    select 
        {{ renamed_columns | join(',\n    ') }}
    from {{ relation }}
{% endmacro %}
