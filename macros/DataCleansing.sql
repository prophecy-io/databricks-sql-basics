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

    {# ----------------------------------------------------------- #
       helper: quote an identifier the way Spark/Databricks expects
       (adapter.quote_identifier already returns the right back-tick
        quoting for the current adapter)                         #}
    {%- set q = adapter.quote_identifier -%}

    {{ log("Applying dataset-specific cleansing operations", info=True) }}
    {%- if removeRowNullAllCols -%}
        {{ log("Removing rows where all columns are null", info=True) }}
        {%- set where_clause = [] -%}
        {%- for col in schema -%}
            {%- do where_clause.append(q(col['name']) ~ ' IS NOT NULL') -%}
        {%- endfor -%}
        {%- set where_clause_sql = where_clause | join(' OR ') -%}

        {%- set cleansed_cte -%}
            WITH cleansed_data AS (
                SELECT *
                FROM {{ relation_name }}
                WHERE {{ where_clause_sql }}
            )
        {%- endset -%}
    {%- else -%}
        {{ log("Returning all rows (no row-level cleansing)", info=True) }}
        {%- set cleansed_cte -%}
            WITH cleansed_data AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- endset -%}
    {%- endif -%}

    {{ log("Applying column-specific cleansing operations", info=True) }}
    {%- if columnNames | length > 0 -%}
        {%- set columns_to_select = [] -%}
        {%- set col_type_map = {} -%}
        {%- for col in schema -%}
            {%- set col_type_map = col_type_map.update({ col.name: col.dataType | lower }) -%}
        {%- endfor -%}
        {%- set numeric_types = ["bigint","decimal","double","float",
                                 "integer","smallint","tinyint"] -%}

        {%- for col_name in columnNames -%}
            {%- set col_expr = q(col_name) -%}

            {# -------- numeric null handling -------- #}
            {%- if col_type_map.get(col_name) in numeric_types
                   and replaceNullForNumericFields -%}
                {%- set col_expr
                     = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" -%}
            {%- endif -%}

            {# -------- string-typed columns -------- #}
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

                {# -------- case conversion -------- #}
                {%- if modifyCase == "makeLowercase" -%}
                    {%- set col_expr = "LOWER(" ~ col_expr ~ ")" -%}
                {%- elif modifyCase == "makeUppercase" -%}
                    {%- set col_expr = "UPPER(" ~ col_expr ~ ")" -%}
                {%- elif modifyCase == "makeTitlecase" -%}
                    {%- set col_expr = "INITCAP(" ~ col_expr ~ ")" -%}
                {%- endif -%}
            {%- endif -%}

            {# -------- date/time null handling -------- #}
            {%- if col_type_map.get(col_name) == "date" and replaceNullDateFields -%}
                {%- set col_expr
                     = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" -%}
            {%- endif -%}

            {%- if col_type_map.get(col_name) == "timestamp"
                   and replaceNullTimeFields -%}
                {%- set col_expr
                     = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" -%}
            {%- endif -%}

            {# -------- final casting & alias -------- #}
            {%- set col_expr = "CAST(" ~ col_expr ~ " AS " ~ col_type_map.get(col_name) ~ ")" -%}
            {%- do columns_to_select.append(col_expr ~ ' AS ' ~ q(col_name)) -%}
        {%- endfor -%}

        {# ----- build final select list (preserve original col order) ----- #}
        {%- set output_columns = [] -%}
        {%- for col in schema -%}
            {%- set override = columns_to_select
                               | selectattr('endswith', q(col['name'])) | list | first -%}
            {%- if override -%}
                {%- do output_columns.append(override) -%}
            {%- else -%}
                {%- do output_columns.append(q(col['name'])) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set final_select -%}
            SELECT {{ output_columns | join(', ') }} FROM cleansed_data
        {%- endset -%}
    {%- else -%}
        {%- set final_select = "SELECT * FROM cleansed_data" -%}
    {%- endif -%}

    {{ return(cleansed_cte ~ "\n" ~ final_select) }}

{% endmacro %}