{% macro DynamicSelect(relation, schema, targetTypes, selectUsing, customExpression='') %}
    {% set selected_columns = [] %}
    {% for column in schema %}
        {% if selectUsing == 'SELECT_EXPR' %}
            {# Evaluate the custom expression by substituting column name in the expression #}
            {% set expression_to_evaluate = customExpression.replace("column_name", "'" ~ column["name"] ~ "'") %}
            {% set evaluation_result = DatabricksSqlBasics.evaluate_expression(expression_to_evaluate) | trim %}

            {# Only add column if the evaluation result is true #}
            {{ log(evaluation_result, info = True) }}
            {% if evaluation_result == "True" %}
                {% do selected_columns.append("`" ~ column["name"] ~ "`") %}
            {% endif %}
        {% else %}
            {# If no custom expression, select columns based on target types #}
            {% if column["dataType"] in targetTypes %}
                {% do selected_columns.append("`" ~ column["name"] ~ "`") %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# Build and return the SELECT statement #}
    {% if selected_columns %}
        SELECT {{ selected_columns | join(', ') }} FROM {{ relation }}
    {% else %}
        SELECT NULL AS no_columns_matched FROM {{ relation }}
    {% endif %}
{% endmacro %}
