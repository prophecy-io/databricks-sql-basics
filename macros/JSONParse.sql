{% macro JSONParse(
    relation_name,
    columnNames
    ) %}

    select
        *,
        {%- for col in columnNames %}
            PARSE_JSON({{ col }}) as {{ col }}_parsed {% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ relation_name }}
    
{% endmacro %}