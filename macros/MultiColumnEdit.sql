{% macro MultiColumnEdit(
    relation,
    columnNames,
    expressionToBeApplied,
    prefixSuffixToBeAdded='',
    changeOutputFieldName=false,
    isPrefix=true,
    changeOutputFieldType=false,
    castOutputTypeName='',
    copyOriginalColumns=false,
    remainingColumns=[],
    prefixSuffixOption = 'prefix / suffix',
    dataType = 'String'
) %}
    {% set column_expressions = [] %}

    {% for column in columnNames %}
        {% if changeOutputFieldName %}
            {% if prefixSuffixOption | lower == "prefix" %}
                {% set alias = prefixSuffixToBeAdded ~ column %}
            {% else %}
                {% set alias = column ~ prefixSuffixToBeAdded %}
            {% endif %}
        {% else %}
            {% set alias = column %}
        {% endif %}

        {% set column_expr = expressionToBeApplied | replace('column_value', '`' ~ column ~ '`') | replace('column_name', "'" ~ column ~ "'") %}
        
        {% if changeOutputFieldType %}
            {% set column_expr = column_expr ~ '::' ~ castOutputTypeName %}
        {% endif %}
        
        {% do column_expressions.append(column_expr ~ ' as ' ~ '`' ~ alias ~ '`') %}
    {% endfor %}

    {% set original_columns = [] %}
    {% if copyOriginalColumns %}
        {% for column in columnNames %}
            {% do original_columns.append('`' ~ column ~ '`') %}
        {% endfor %}
    {% endif %}
    {% set all_columns = column_expressions + remainingColumns + original_columns %}

    select 
        {{ all_columns | join(', ') }}
    from {{ relation }}
{% endmacro %}
