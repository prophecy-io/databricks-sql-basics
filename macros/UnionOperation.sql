{%- macro UnionOperation(relation, operationType) -%}
    {{ log("Parameters are: " ~ relation ~ ", " ~ operationType, info=True) }}

    {# Split and filter the relations list #}
    {%- set relations = relation.split(',') | map('trim') | reject('equalto', '') | list %}

    {# Determine the union operator based on operationType input #}
    {%- if operationType | lower == "union" %}
        {%- set op = "UNION" %}
    {%- elif operationType | lower == "unionall" %}
        {%- set op = "UNION ALL" %}
    {%- else %}
        {%- set op = operationType %}
    {%- endif %}

    {# Create a namespace to hold the accumulating SQL statement #}
    {%- set ns = namespace(sql_statement="") %}

    {%- for rel in relations %}
        {%- if loop.first %}
            {%- set ns.sql_statement = "SELECT * FROM " ~ rel %}
        {%- else %}
            {%- set ns.sql_statement = ns.sql_statement ~ " " ~ op ~ " SELECT * FROM " ~ rel %}
        {%- endif %}
    {%- endfor %}

    {{ log("Final SQL statement is: " ~ ns.sql_statement, info=True) }}
    {{ return(ns.sql_statement) }}
{%- endmacro %}