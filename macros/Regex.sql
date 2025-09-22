{% macro Regex(
    relation_name,
    currentSchema='',
    selectedColumnName='',
    regexExpression='',
    outputMethod='replace',
    caseInsensitive=true,
    allowBlankTokens=false,
    replacementText='',
    copyUnmatchedText=false,
    tokenizeOutputMethod='splitColumns',
    noOfColumns=3,
    extraColumnsHandling='dropExtraWithWarning',
    outputRootName='regex_col',
    parseColumns=[],
    matchColumnName='regex_match',
    errorIfNotMatched=false
) %}

{# Input validation #}
{%- if not selectedColumnName or selectedColumnName == '' -%}
    {{ log("ERROR: selectedColumnName parameter is required and cannot be empty", info=True) }}
    select 'ERROR: selectedColumnName parameter is required' as error_message
{%- elif not regexExpression or regexExpression == '' -%}
    {{ log("ERROR: regexExpression parameter is required and cannot be empty", info=True) }}
    select 'ERROR: regexExpression parameter is required' as error_message
{%- elif not relation_name or relation_name == '' -%}
    {{ log("ERROR: relation_name parameter is required and cannot be empty", info=True) }}
    select 'ERROR: relation_name parameter is required' as error_message
{%- else -%}

{%- set output_method_lower = outputMethod | lower -%}
{%- set escaped_regex = regexExpression | replace("\\", "\\\\") | replace("'", "''") -%}
{%- set regex_pattern = ('(?i)' if caseInsensitive else '') ~ escaped_regex -%}
{%- set source_table = relation_name -%}
{%- set extra_handling_lower = extraColumnsHandling | lower -%}

{%- if output_method_lower == 'replace' -%}
    select
        *,
        {% if copyUnmatchedText %}
        case
            when {{ selectedColumnName }} rlike '{{ regex_pattern }}' then
                regexp_replace({{ selectedColumnName }}, '{{ regex_pattern }}', '{{ replacementText | replace("'", "''") }}')
            else {{ selectedColumnName }}
        end as {{ selectedColumnName }}_replaced
        {% else %}
        regexp_replace({{ selectedColumnName }}, '{{ regex_pattern }}', '{{ replacementText | replace("'", "''") }}') as {{ selectedColumnName }}_replaced
        {% endif %}
    from {{ source_table }}

{%- elif output_method_lower == 'parse' -%}
    {%- if parseColumns and parseColumns|length > 0 -%}
        select
            *
            {%- for configStr in parseColumns -%}
                {%- set config = fromjson(configStr) -%}
                {%- if config and config.columnName -%}
                    {%- set col_name = config.columnName -%}
                    {%- set col_type = config.dataType | default('string') -%}
                    {%- set group_index = loop.index -%}
            ,
            {%- if col_type|lower == 'string' -%}
            regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as {{ col_name }}
            {%- elif col_type|lower == 'int' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as int) as {{ col_name }}
            {%- elif col_type|lower == 'bigint' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as bigint) as {{ col_name }}
            {%- elif col_type|lower == 'double' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as double) as {{ col_name }}
            {%- elif col_type|lower == 'bool' or col_type|lower == 'boolean' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as boolean) as {{ col_name }}
            {%- elif col_type|lower == 'date' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as date) as {{ col_name }}
            {%- elif col_type|lower == 'datetime' or col_type|lower == 'timestamp' -%}
            cast(nullif(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}), '') as timestamp) as {{ col_name }}
            {%- else -%}
            regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as {{ col_name }}
            {%- endif -%}
                {%- endif -%}
            {%- endfor %}
        from {{ source_table }}
    {%- else -%}
        select 'ERROR: parseColumns array is empty after parsing' as error_message
    {%- endif -%}


{%- elif output_method_lower == 'tokenize' -%}
    {%- set tokenize_method_lower = tokenizeOutputMethod | lower -%}

    {%- if tokenize_method_lower == 'splitcolumns' -%}
        select
            *
            {%- for i in range(1, noOfColumns + 1) -%}
            ,
            {%- if allowBlankTokens -%}
            coalesce(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}), '') as {{ outputRootName }}{{ i }}
            {%- else -%}
            regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
            {%- endif -%}
            {%- endfor %}
            {#- Add a space to ensure separation from the 'from' clause #}
            {% if extra_handling_lower == 'dropextrawithwarning' -%}
                {{ log("WARNING: Extra regex groups beyond noOfColumns (" ~ noOfColumns ~ ") will be dropped", info=True) }}
            {% elif extra_handling_lower == 'erroronextra' -%}
                {{ log("INFO: Checking for extra regex groups beyond noOfColumns (" ~ noOfColumns ~ ")", info=True) }}
                {% for i in range(noOfColumns + 1, noOfColumns + 6) -%}
                ,case
                    when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) != '' then
                        cast('ERROR: Extra regex group {{ i }} found - extraColumnsHandling set to errorOnExtra' as int)
                    else null
                end as _validation_group_{{ i }}
                {%- endfor %}
            {%- endif -%}

         from {{ source_table }}

    {%- elif tokenize_method_lower == 'splitrows' -%}
        with split_data as (
            select
                *,
                split({{ selectedColumnName }}, '{{ regex_pattern }}') as tokens
            from {{ source_table }}
        ),
        exploded_tokens as (
            select
                *,
                explode(tokens) as token_value
            from split_data
        ),
        numbered_tokens as (
            select
                * except (tokens),
                token_value,
                row_number() over (partition by {{ selectedColumnName }} order by monotonically_increasing_id()) as token_position
            from exploded_tokens
        )
        select
            *,
            token_value as {{ outputRootName }},
            token_position as token_sequence
        from numbered_tokens
        {% if not allowBlankTokens %}
        where token_value != '' and token_value is not null
        {% endif %}

    {%- else -%}
        select
            *,
            {% for i in range(1, noOfColumns + 1) %}
            regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}{{ i }}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        from {{ source_table }}
    {%- endif -%}

{%- elif output_method_lower == 'match' -%}
    {# Simplified match mode - no unnecessary CTEs or joins #}
    select
        *,
        case
            when {{ selectedColumnName }} is null then 0
            when {{ selectedColumnName }} rlike '{{ regex_pattern }}' then 1
            else 0
        end as {{ matchColumnName }}
    from {{ source_table }}
    {% if errorIfNotMatched %}
    where {{ selectedColumnName }} rlike '{{ regex_pattern }}'
    {% endif %}

{%- else -%}
    {# Fallback for unknown output method #}
    select 'ERROR: Unknown outputMethod "{{ outputMethod }}"' as error_message

{%- endif -%}

{%- endif -%}

{% endmacro %}