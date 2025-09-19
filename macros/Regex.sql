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
    tokenizeOutputMethod='splitColumns',
    noOfColumns=3,
    extraColumnsHandling='dropExtraWithWarning',
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
{%- set extra_handling_lower = extraColumnsHandling | lower -%}

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

{%- elif output_method_lower == 'parse' -%}
    {%- if parseColumns != '[]' and parseColumns != '' -%}
    {# Parse the JSON string into column configurations #}
    {%- set column_configs = fromjson(parseColumns) -%}

    {{ log("Parsed column configs: " ~ column_configs, info=True) }}

    {%- if column_configs|length > 0 -%}
        select
            *,
            {%- for config in column_configs %}
            {%- set col_name = config.columnName -%}
            {%- set col_type = config.dataType -%}
            {%- set regex_expr = config.rgxExpression -%}

            {# Apply regex extraction and cast to appropriate data type #}
            {% if col_type == 'string' %}
            regexp_extract({{ columnName }}, '{{ regex_expr }}', 1) as {{ col_name }}
            {% elif col_type == 'int' %}
            cast(nullif(regexp_extract({{ columnName }}, '{{ regex_expr }}', 1), '') as int) as {{ col_name }}
            {% elif col_type == 'double' %}
            cast(nullif(regexp_extract({{ columnName }}, '{{ regex_expr }}', 1), '') as double) as {{ col_name }}
            {% elif col_type == 'bool' %}
            cast(nullif(regexp_extract({{ columnName }}, '{{ regex_expr }}', 1), '') as boolean) as {{ col_name }}
            {% elif col_type == 'date' %}
            cast(nullif(regexp_extract({{ columnName }}, '{{ regex_expr }}', 1), '') as date) as {{ col_name }}
            {% elif col_type == 'datetime' %}
            cast(nullif(regexp_extract({{ columnName }}, '{{ regex_expr }}', 1), '') as timestamp) as {{ col_name }}
            {% else %}
            {# Default to string if unknown type #}
            regexp_extract({{ columnName }}, '{{ regex_expr }}', 1) as {{ col_name }}
            {% endif %}
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        from {{ source_table }}
    {%- endif -%}
{%- else -%}
    select 'ERROR: No parseColumns provided' as error_message
{%- endif -%}

{%- elif output_method_lower == 'tokenize' -%}
    {%- set tokenize_method_lower = tokenizeOutputMethod | lower -%}

    {%- if tokenize_method_lower == 'splitcolumns' -%}
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

            {# Handle extra columns based on extraColumnsHandling setting #}
            {%- if extra_handling_lower == 'keepextra' -%}
                {# Add extra columns beyond noOfColumns #}
                {%- for i in range(noOfColumns + 1, max_regex_groups + 1) %}
                ,{% if allowBlankTokens %}
                coalesce(
                    regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}),
                    ''
                ) as {{ outputRootName }}_extra_{{ i }}
                {% else %}
                regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) as {{ outputRootName }}_extra_{{ i }}
                {% endif %}
                {%- endfor %}
            {%- elif extra_handling_lower == 'dropextrawithwarning' -%}
                {{ log("WARNING: Extra regex groups beyond noOfColumns (" ~ noOfColumns ~ ") will be dropped", info=True) }}
            {%- elif extra_handling_lower == 'erroronextra' -%}
                {# Validate that no extra regex groups exist beyond noOfColumns #}
                {{ log("INFO: Checking for extra regex groups beyond noOfColumns (" ~ noOfColumns ~ ")", info=True) }}
                {%- for i in range(noOfColumns + 1, noOfColumns + 6) %}
                ,case
                    when regexp_extract({{ columnName }}, '{{ regex_pattern }}', {{ i }}) != '' then
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
                split({{ columnName }}, '{{ regex_pattern }}') as tokens
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
                row_number() over (partition by {{ columnName }} order by monotonically_increasing_id()) as token_position
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

{%- endif -%}

{%- endif -%}

{% endmacro %}