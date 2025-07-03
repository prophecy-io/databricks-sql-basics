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

    {# ───── helper: quote identifiers ───── #}
    {%- set q = lambda x: '`' ~ x ~ '`' -%}

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

    {%- else  -%}
        {{ log("Returning all columns since dataset-specific cleansing operations are not specified", info=True) }}
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
        {%- set numeric_types = ["bigint", "decimal", "double", "float", "integer", "smallint", "tinyint"] -%}

        {{ log(col_type_map, info=True) }}
        {%- for col_name in columnNames -%}
            {%- set col_expr = q(col_name) -%}

            {%- if col_type_map.get(col_name) in numeric_types and replaceNullForNumericFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" -%}
            {%- endif -%}

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

            {%- if col_type_map.get(col_name) == "date" and replaceNullDateFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" -%}
            {%- endif -%}

            {%- if col_type_map.get(col_name) == "timestamp" and replaceNullTimeFields -%}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" -%}
            {%- endif -%}

            {{ log("Appending transformed column expression", info=True) }}
            {%- set col_expr = "CAST(" ~ col_expr ~ " AS " ~ col_type_map.get(col_name) ~ ")" -%}
            {%- do columns_to_select.append(col_expr ~ ' AS ' ~ q(col_name)) -%}
        {%- endfor -%}

        {# Build the final list of columns to output #}
        {%- set output_columns = [] -%}
        {%- for col_name_val in schema -%}
            {% set has_override = false %}
            {%- for expr in columns_to_select -%}
                {%- set alias = expr.split(' AS ')[-1] | trim | replace('"', '') | replace('`', '') | upper -%}
                {%- if col_name_val['name'] | trim | replace('"', '') | upper == alias -%}
                    {%- do output_columns.append(expr) -%}
                    {% set has_override = true %}
                    {%- break -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if not has_override -%}
                {%- do output_columns.append(q(col_name_val['name'])) -%}
            {%- endif -%}
        {%- endfor -%}

        {{ log("Columns after expression evaluation:" ~ output_columns, info=True) }}
        {%- set final_output_columns = output_columns | unique | join(', ') -%}
        {{ log("Final Output Columns: " ~ final_output_columns, info=True) }}

        {%- set final_select -%}
            {%- if columns_to_select -%}
                SELECT {{ final_output_columns }} FROM cleansed_data
            {%- else -%}
                SELECT * FROM cleansed_data
            {%- endif -%}
        {%- endset -%}

    {%- else -%}
        {%- set final_select -%}
            SELECT * FROM cleansed_data
        {%- endset -%}
    {%- endif -%}

    {%- set final_query = cleansed_cte ~ "\n" ~ final_select -%}
    {{ return(final_query) }}

{% endmacro %}