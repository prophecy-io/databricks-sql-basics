{%- macro ArrangeTool(relation_name, key_fields=[], output_fields=[]) -%}

{%- if output_fields | length == 0 -%}
    select 'ERROR: No output_fields specified. Define output columns with their source fields.' as error_message
{%- else -%}

{%- set first_output = output_fields[0] -%}
{%- set max_fields = first_output.get('source_fields', []) | length -%}
{%- for output_field in output_fields -%}
    {%- set field_count = output_field.get('source_fields', []) | length -%}
    {%- if field_count > max_fields -%}
        {%- set max_fields = field_count -%}
    {%- endif -%}
{%- endfor -%}

{%- for i in range(max_fields) %}
select
    {%- for key_field in key_fields %}
    {{ key_field }},
    {%- endfor %}
    {%- for output_field in output_fields %}
    {%- set column_header = output_field.get('column_header', 'Value') -%}
    {%- set source_fields = output_field.get('source_fields', []) -%}
    {%- set include_description = output_field.get('include_description', false) -%}
    {%- set description_header = output_field.get('description_header', column_header ~ '_Source') -%}
    {%- if include_description %}
    {%- if i < source_fields | length %}
    '{{ source_fields[i] }}' as {{ description_header }},
    {%- else %}
    null as {{ description_header }},
    {%- endif %}
    {%- endif %}
    {%- if i < source_fields | length %}
    {{ source_fields[i] }} as {{ column_header }}
    {%- else %}
    null as {{ column_header }}
    {%- endif %}
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
from {{ relation_name }}
{%- if not loop.last %}

union all

{%- endif %}
{%- endfor %}

{%- if key_fields | length > 0 %}
order by {{ key_fields | join(', ') }}
{%- endif %}

{%- endif -%}

{%- endmacro -%}