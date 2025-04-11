{% macro TextToColumns(
    relation_name,
    columnName,
    delimiter,
    split_strategy,
    noOfColumns,
    leaveExtraCharLastCol,
    splitColumnPrefix,
    splitColumnSuffix,
    splitRowsColumnName    
    ) %}

{# 
  Build the regex pattern for matching the delimiter.
  Adjust the pattern as needed for your use case.
#}
{%- set pattern = delimiter -%}
{%- if split_strategy == 'splitColumns' -%}
    WITH source AS (
        SELECT *,
            split(
                regexp_replace({{ columnName }}, {{ "'" ~ pattern ~ "'" }}, '%%DELIM%%'),
                '%%DELIM%%'
            ) AS tokens
        FROM {{ relation_name }}
    ),
    all_data AS (
    SELECT *,
        {# Extract tokens positionally (Spark arrays are 0-indexed) #}
        {%- for i in range(1, noOfColumns) %}
            regexp_replace(trim(tokens[{{ i - 1 }}]), '^"|"$', '') AS {{ splitColumnPrefix }}_{{ i }}_{{ splitColumnSuffix }}{% if not loop.last or leaveExtraCharLastCol %}, {% endif %}
        {%- endfor %}
        {%- if leaveExtraCharLastCol %}
            CASE 
                WHEN size(tokens) >= {{ noOfColumns }} 
                    THEN array_join(slice(tokens, {{ noOfColumns }}, greatest(size(tokens) - {{ noOfColumns }} + 1, 0)), '{{ delimiter }}')
                ELSE null 
            END AS {{ splitColumnPrefix }}_{{ noOfColumns }}_{{ splitColumnSuffix }}
        {%- else %}
            tokens[{{ noOfColumns - 1 }}] AS {{ splitColumnPrefix }}_{{ noOfColumns }}_{{ splitColumnSuffix }}
        {%- endif %}
    FROM source
    )
    SELECT * EXCEPT(tokens) FROM all_data

{%- elif split_strategy == 'splitRows' -%}
    SELECT r.*,
        trim(regexp_replace(s.col, '[{}_]', ' ')) AS {{ splitRowsColumnName }}
    FROM {{ relation_name }} r
    LATERAL VIEW explode(
        split(
            if(r.{{ columnName }} IS NULL, '', r.{{ columnName }}),
            '{{ pattern }}'
        )
    ) s AS col

{%- else -%}
SELECT * FROM {{ relation_name }}
{%- endif -%}

{% endmacro %}