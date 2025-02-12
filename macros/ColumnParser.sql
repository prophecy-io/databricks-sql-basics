
{%- macro ColumnParser(relation, parserType, columnToParse, schema) -%}
    
    {% if parserType | lower == "json" %}
    
    select *, parse_json ( {{ '`' ~ columnToParse ~ '`' }} ) as json_parsed_content from {{ relation }}
    
    {% else %}

    select *, from_xml ( {{ '`' ~ columnToParse ~ '`' }}, '{{ schema }}' ) as xml_parsed_content from {{ relation }}
    
    {% endif %}
    
{%- endmacro -%}
