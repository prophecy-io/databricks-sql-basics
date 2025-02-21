
{%- macro XMLParse(relation, columnToParse, schema) -%}
    select *, from_xml ( {{ '`' ~ columnToParse ~ '`' }}, '{{ schema }}' ) as xml_parsed_content from {{ relation }}
{%- endmacro -%}
