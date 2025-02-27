{% macro BulkColumnExpressions(relation, columnNames, expressionToBeApplied, prefixSuffixToBeAdded='', castOutputTypeName='', copyOriginalColumns=false, remainingColumns=[], prefixSuffixOption = 'prefix / suffix') %}
    {% set column_expressions = [] %}

    {% for column in columnNames %}
        {% if copyOriginalColumns %}
            {% if prefixSuffixOption | lower == "prefix" %}
                {% set alias = prefixSuffixToBeAdded ~ column %}
            {% else %}
                {% set alias = column ~ prefixSuffixToBeAdded %}
            {% endif %}
        {% else %}
            {% set alias = column %}
        {% endif %}

        {% set column_expr = expressionToBeApplied | replace('column_value', '`' ~ column ~ '`') | replace('column_name', "'" ~ column ~ "'") %}
        
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
