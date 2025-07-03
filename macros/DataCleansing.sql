{% macro DataCleansing(
    relation_name,
    schema,
    modifyCase,
    columnNames=[],
    replaceNullTextFields=False,
    replaceNullTextWith="NA",
    replaceNullForNumericFields=False,
    replaceNullNumericWith=0,
    trimWhiteSpace=False,
    removeTabsLineBreaksAndDuplicateWhitespace=False,
    allWhiteSpace=False,
    cleanLetters=False,
    cleanPunctuations=False,
    cleanNumbers=False,
    removeRowNullAllCols=False,
    replaceNullDateFields=False,
    replaceNullDateWith="1970-01-01",
    replaceNullTimeFields=False,
    replaceNullTimeWith="1970-01-01 00:00:00"
) %}

    {# ─── helper: quote an identifier the adapter’s way (Spark ⇒ back-ticks) ─── #}
    {%- set q = adapter.quote_identifier -%}

    {{ log("Applying dataset-specific cleansing operations", info=True) }}
    {%- if removeRowNullAllCols -%}
        {{ log("Removing rows where all columns are NULL", info=True) }}
        {%- set where_clause = schema | map(attribute='name')
                                        | map('string')
                                        | map(q)
                                        | map('~', ' IS NOT NULL') | list -%}
        {%- set cleansed_cte -%}
            WITH cleansed_data AS (
                SELECT *
                FROM {{ relation_name }}
                WHERE {{ where_clause | join(' OR ') }}
            )
        {%- endset -%}
    {%- else -%}
        {%- set cleansed_cte -%}
            WITH cleansed_data AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- endset -%}
    {%- endif -%}

    {{ log("Applying column-specific cleansing operations", info=True) }}
    {%- if columnNames | length > 0 -%}
        {# ---------------- prepare helpers ---------------- #}
        {%- set numeric_types = ["bigint","decimal","double","float",
                                 "integer","smallint","tinyint"] -%}
        {%- set col_type_map = {} -%}
        {%- for c in schema -%}
            {%- set col_type_map = col_type_map.update({ c.name: c.dataType | lower }) -%}
        {%- endfor -%}

        {%- set columns_to_select = [] -%}

        {# ---------------- generate per-column expressions ---------------- #}
        {%- for col_name in columnNames -%}
            {%- set col_expr = q(col_name) -%}

            {# numeric null substitution #}
            {%- if col_type_map.get(col_name) in numeric_types
                   and replaceNullForNumericFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" -%}
            {%- endif -%}

            {# string-type cleansing #}
            {%- if col_type_map.get(col_name) == "string" -%}
                {%- if replaceNullTextFields -%}
                    {%- set col_expr = "COALESCE(" ~ col_expr ~ ", '" ~ replaceNullTextWith ~ "')" -%}
                {%- endif -%}
                {%- if trimWhiteSpace -%}
                    {%- set col_expr = "LTRIM(RTRIM(" ~ col_expr ~ "))" -%}
                {%- endif -%}
                {%- if removeTabsLineBreaksAndDuplicateWhitespace -%}
                    {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', ' ')" -%}
                {%- endif -%}
                {%- if allWhiteSpace -%}
                    {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', '')" -%}
                {%- endif -%}
                {%- if cleanLetters -%}
                    {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[A-Za-z]+', '')" -%}
                {%- endif -%}
                {%- if cleanPunctuations -%}
                    {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^a-zA-Z0-9\\\\s]', '')" -%}
                {%- endif -%}
                {%- if cleanNumbers -%}
                    {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\d+', '')" -%}
                {%- endif -%}

                {%- if modifyCase == "makeLowercase" -%}
                    {%- set col_expr = "LOWER(" ~ col_expr ~ ")" -%}
                {%- elif modifyCase == "makeUppercase" -%}
                    {%- set col_expr = "UPPER(" ~ col_expr ~ ")" -%}
                {%- elif modifyCase == "makeTitlecase" -%}
                    {%- set col_expr = "INITCAP(" ~ col_expr ~ ")" -%}
                {%- endif -%}
            {%- endif -%}

            {# date/timestamp null substitution #}
            {%- if col_type_map.get(col_name) == "date" and replaceNullDateFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" -%}
            {%- endif -%}
            {%- if col_type_map.get(col_name) == "timestamp" and replaceNullTimeFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" -%}
            {%- endif -%}

            {# final cast & alias #}
            {%- set col_expr = "CAST(" ~ col_expr ~ " AS " ~ col_type_map.get(col_name) ~ ")" -%}
            {%- do columns_to_select.append(col_expr ~ ' AS ' ~ q(col_name)) -%}
        {%- endfor -%}

        {# ------------- preserve original column order ------------- #}
        {%- set output_columns = [] -%}
        {%- for col in schema -%}
            {% set overridden = {"flag": false} %}
            {%- for expr in columns_to_select -%}
                {%- set alias = expr.split(' AS ')[-1] | replace('`', '') | upper -%}
                {%- if col.name | replace('`','') | replace('"','') | upper == alias -%}
                    {%- do output_columns.append(expr) -%}
                    {% do overridden.update({"flag": true}) %}
                    {%- break -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if not overridden.flag -%}
                {%- do output_columns.append(q(col.name)) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set final_select = "SELECT " ~ (output_columns | join(', ')) ~ " FROM cleansed_data" -%}
    {%- else -%}
        {%- set final_select = "SELECT * FROM cleansed_data" -%}
    {%- endif -%}

    {{ return(cleansed_cte ~ "\n" ~ final_select) }}

{% endmacro %}