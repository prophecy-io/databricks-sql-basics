
{%- macro XMLParse(relation, columnToParse, schema) -%}
    select *, from_xml ( {{ '`' ~ columnToParse ~ '`' }}, 'a INT, b DOUBLE' ) as xml_parsed_content from {{ relation }}
{%- endmacro -%}
