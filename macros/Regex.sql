{% macro Regex(
    relation_name,
    currentSchema='',
    columnName='',
    regexExpression='',
    outputMethod='replace',
    caseInsensitive=true,
    allowBlankTokens=false,
    replacementText='',
    copyUnmatchedText=false,
    tokenizeOutputMethod='split_to_columns',
    noOfColumns=3,
    extraColumnsHandling='drop_extra_with_warning',
    outputRootName='regex_col',
    parseColumns='[]',
    matchColumnName='regex_match',
    errorIfNotMatched=false
) %}

{# Input validation #}
{%- if not columnName or columnName == '' -%}
    {{ log("ERROR: columnName parameter is required and cannot be empty", info=True) }}
    select 'ERROR: columnName parameter is required' as error_message
{%- elif not regexExpression or regexExpression == '' -%}
    {{ log("ERROR: regexExpression parameter is required and cannot be empty", info=True) }}
    select 'ERROR: regexExpression parameter is required' as error_message
{%- elif not relation_name or relation_name == '' -%}
    {{ log("ERROR: relation_name parameter is required and cannot be empty", info=True) }}
    select 'ERROR: relation_name parameter is required' as error_message
{%- else -%}

{%- set output_method_lower = outputMethod | lower -%}
{%- set regex_pattern = ('(?i)' if caseInsensitive else '') ~ regexExpression -%}
{%- set source_table = relation_name -%}

{%- if output_method_lower == 'replace' -%}
    select
        *,
        {% if copyUnmatchedText %}
        case
            when {{ columnName }} rlike '{{ regex_pattern }}' then
                regexp_replace({{ columnName }}, '{{ regex_pattern }}', '{{ replacementText }}')
            else {{ columnName }}
        end as {{ columnName }}_replaced
        {% else %}
        regexp_replace({{ columnName }}, '{{ regex_pattern }}', '{{ replacementText }}') as {{ columnName }}_replaced
        {% endif %}
    from {{ source_table }}
    {% if errorIfNotMatched %}
    where {{ columnName }} rlike '{{ regex_pattern }}'
    {% endif %}

{%- elif output_method_lower == 'parse' -%}
    {# Parse mode: Extract capturing groups into separate columns #}
    {%- if parseColumns != '[]' and parseColumns != '' -%}
        {# Extract column information from parseColumns string using string manipulation #}
        {%- set column_configs = [] -%}

        {# Split by columnName to find each configuration block #}
        {%- set parts = parseColumns.split('"columnName":') -%}
        {%- for part in parts -%}
            {%- if '"' in part -%}
                {# Extract column name - between first pair of quotes #}
                {%- set name_start = part.find('"') -%}
                {%- set name_end = part.find('"', name_start + 1) -%}
                {%- if name_start >= 0 and name_end > name_start -%}
                    {%- set col_name = part[name_start+1:name_end] -%}

                    {# Extract data type - look for "dataType": #}
                    {%- set type_pattern = '"dataType":' -%}
                    {%- set type_start = part.find(type_pattern) -%}
                    {%- if type_start >= 0 -%}
                        {%- set type_quote_start = part.find('"', type_start + type_pattern|length) -%}
                        {%- set type_quote_end = part.find('"', type_quote_start + 1) -%}
                        {%- if type_quote_start >= 0 and type_quote_end > type_quote_start -%}
                            {%- set col_type = part[type_quote_start+1:type_quote_end] -%}
                            {%- set _ = column_configs.append({'name': col_name, 'type': col_type}) -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}

        {{ log("Extracted column configs: " ~ column_configs, info=True) }}

        {%- if column_configs|length > 0 -%}
            select
                *,
                {% for config in column_configs %}
                {%- set col_type = config.type -%}
                {% if col_type.lower() in ['string'] %}
                regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ loop.index }}) as {{ config.name }}
                {% elif col_type.lower() in ['int32', 'integer', 'int'] %}
                cast(regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ loop.index }}) as int) as {{ config.name }}
                {% elif col_type.lower() in ['double', 'float'] %}
                cast(regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ loop.index }}) as double) as {{ config.name }}
                {% elif col_type.lower() in ['bool', 'boolean'] %}
                cast(regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ loop.index }}) as boolean) as {{ config.name }}
                {% else %}
                regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ loop.index }}) as {{ config.name }}
                {% endif %}
                {%- if not loop.last -%},{%- endif -%}
                {% endfor %}
            from {{ source_table }}
            {% if errorIfNotMatched %}
            where {{ columnName }} rlike '{{ regex_pattern }}'
            {% endif %}
        {%- else -%}
            {# Fallback if parsing fails #}
            select
                *,
                {% for i in range(1, noOfColumns + 1) %}
                regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
                {%- if not loop.last -%},{%- endif -%}
                {% endfor %}
            from {{ source_table }}
            {% if errorIfNotMatched %}
            where {{ columnName }} rlike '{{ regex_pattern }}'
            {% endif %}
        {%- endif -%}
    {%- else -%}
        {# Use default numbered columns #}
        select
            *,
            {% for i in range(1, noOfColumns + 1) %}
            regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        from {{ source_table }}
        {% if errorIfNotMatched %}
        where {{ columnName }} rlike '{{ regex_pattern }}'
        {% endif %}
    {%- endif -%}

{%- elif output_method_lower == 'tokenize' -%}
    {%- set tokenize_method_lower = tokenizeOutputMethod | lower -%}

    {%- if tokenize_method_lower == 'split_to_columns' -%}
        select
            *,
            {% for i in range(1, noOfColumns + 1) %}
            {% if allowBlankTokens %}
            coalesce(
                regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}),
                ''
            ) as {{ outputRootName }}{{ i }}
            {% else %}
            regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
            {% endif %}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        from {{ source_table }}
        {% if errorIfNotMatched %}
        where {{ columnName }} rlike '{{ regex_pattern }}'
        {% endif %}

    {%- elif tokenize_method_lower == 'split_to_rows' -%}
        with regex_matches as (
            select
                *,
                regexp_extract_all({{ columnName }}, '{{ regex_pattern }}') as tokens
            from {{ source_table }}
        ),
        exploded_tokens as (
            select
                * except(tokens),
                posexplode(if(size(tokens) > 0, tokens, array(''))) as (token_position, token_value)
            from regex_matches
        )
        select
            *,
            token_value as {{ outputRootName }},
            token_position + 1 as token_sequence
        from exploded_tokens
        {% if not allowBlankTokens %}
        where token_value != '' and token_value is not null
        {% endif %}

    {%- else -%}
        select
            *,
            {% for i in range(1, noOfColumns + 1) %}
            regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        from {{ source_table }}
    {%- endif -%}

{%- elif output_method_lower == 'match' -%}
    select
        *,
        case
            when {{ columnName }} rlike '{{ regex_pattern }}' then 1
            else 0
        end as {{ matchColumnName }}
    from {{ source_table }}
    {% if errorIfNotMatched %}
    where {{ columnName }} rlike '{{ regex_pattern }}'
    {% endif %}

{%- else -%}
    {# Default to replace mode #}
    select
        *,
        regexp_replace({{ columnName }}, '{{ regex_pattern }}', '{{ replacementText }}') as {{ columnName }}_replaced
    from {{ source_table }}

{%- endif -%}

{%- endif -%}

{% endmacro %}