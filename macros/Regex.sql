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
            {%- if col_type|lower == 'string' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }})
            end as {{ col_name }}
            {%- elif col_type|lower == 'int' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as int)
            end as {{ col_name }}
            {%- elif col_type|lower == 'bigint' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as bigint)
            end as {{ col_name }}
            {%- elif col_type|lower == 'double' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as double)
            end as {{ col_name }}
            {%- elif col_type|lower == 'bool' or col_type|lower == 'boolean' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as boolean)
            end as {{ col_name }}
            {%- elif col_type|lower == 'date' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as date)
            end as {{ col_name }}
            {%- elif col_type|lower == 'datetime' or col_type|lower == 'timestamp' %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else cast(regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) as timestamp)
            end as {{ col_name }}
            {%- else %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }}) = '' then null
                else regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ group_index }})
            end as {{ col_name }}
            {%- endif %}
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
            {%- for i in range(1, noOfColumns + 1) %},
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                {% if allowBlankTokens -%}
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) = '' then ''
                {% else -%}
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) = '' then null
                {% endif -%}
                else regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }})
            end as {{ outputRootName }}{{ i }}
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
        with regex_matches as (
            select
                *,
                regexp_extract_all({{ selectedColumnName }}, '{{ regexExpression }}') as split_tokens
            from {{ source_table }}
        ),
        exploded_tokens as (
            select
                * except (split_tokens),
                explode(split_tokens) as token_value_new
            from regex_matches
        ),
        numbered_tokens as (
            select
                *,
                token_value_new,
                row_number() over (partition by {{ selectedColumnName }} order by monotonically_increasing_id()) as token_position
            from exploded_tokens
        )
        select
            * except (token_value_new),
            token_value_new as {{ outputRootName }},
            token_position as token_sequence
        from numbered_tokens
        {% if not allowBlankTokens %}
        where token_value_new != '' and token_value_new is not null
        {% endif %}

    {%- else -%}
        select
            *,
            {% for i in range(1, noOfColumns + 1) %}
            case
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', 0) = '' then null
                when regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }}) = '' then null
                else regexp_extract({{ selectedColumnName }}, '{{ regex_pattern }}', {{ i }})
            end as {{ outputRootName }}{{ i }}
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