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

    {# ────────────────────────────────
       helper – quote identifiers the
       right way for the active adapter
    ──────────────────────────────── #}
    {%- set q = adapter.quote_identifier -%}

    {# ────────────────────────────────
       1.  Build the cleansed_data CTE
    ──────────────────────────────── #}
    {% set cleansed_cte %}
        WITH cleansed_data AS (
            SELECT *
            FROM {{ relation_name }}
            {% if removeRowNullAllCols %}
                WHERE
                {%- set conds = [] -%}
                {%- for c in schema %}
                    {%- do conds.append(q(c.name) ~ ' IS NOT NULL') %}
                {%- endfor %}
                {{ conds | join(' OR ') }}
            {% endif %}
        )
    {% endset %}

    {# ────────────────────────────────
       2.  If no columnNames supplied,
           just return everything
    ──────────────────────────────── #}
    {% if columnNames | length == 0 %}
        {{ return( cleansed_cte ~ '\nSELECT * FROM cleansed_data' ) }}
    {% endif %}

    {# ────────────────────────────────
       3.  Prep helpers
    ──────────────────────────────── #}
    {%- set numeric_types = ["bigint","decimal","double","float",
                             "integer","smallint","tinyint"] -%}

    {%- set col_type_map = {} %}
    {%- for c in schema %}
        {% do col_type_map.update({ c.name: c.dataType | lower }) %}
    {%- endfor %}

    {%- set columns_to_select = [] -%}

    {# ────────────────────────────────
       4.  Build transformed expressions
    ──────────────────────────────── #}
    {%- for col_name in columnNames %}
        {%- set col_expr = q(col_name) %}

        {% if col_type_map.get(col_name) in numeric_types
              and replaceNullForNumericFields %}
            {%- set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" %}
        {% endif %}

        {% if col_type_map.get(col_name) == "string" %}
            {% if replaceNullTextFields %}
                {%- set col_expr = "COALESCE(" ~ col_expr ~ ", '" ~ replaceNullTextWith ~ "')" %}
            {% endif %}
            {% if trimWhiteSpace %}
                {%- set col_expr = "LTRIM(RTRIM(" ~ col_expr ~ "))" %}
            {% endif %}
            {% if removeTabsLineBreaksAndDuplicateWhitespace %}
                {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', ' ')" %}
            {% endif %}
            {% if allWhiteSpace %}
                {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', '')" %}
            {% endif %}
            {% if cleanLetters %}
                {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[A-Za-z]+', '')" %}
            {% endif %}
            {% if cleanPunctuations %}
                {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^a-zA-Z0-9\\\\s]', '')" %}
            {% endif %}
            {% if cleanNumbers %}
                {%- set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\d+', '')" %}
            {% endif %}

            {% if modifyCase == 'makeLowercase' %}
                {%- set col_expr = "LOWER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeUppercase' %}
                {%- set col_expr = "UPPER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeTitlecase' %}
                {%- set col_expr = "INITCAP(" ~ col_expr ~ ")" %}
            {% endif %}
        {% endif %}

        {% if col_type_map.get(col_name) == 'date' and replaceNullDateFields %}
            {%- set col_expr = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" %}
        {% endif %}
        {% if col_type_map.get(col_name) == 'timestamp' and replaceNullTimeFields %}
            {%- set col_expr = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" %}
        {% endif %}

        {# final cast & alias #}
        {%- set col_expr = "CAST(" ~ col_expr ~ " AS " ~ col_type_map.get(col_name) ~ ")" %}
        {% do columns_to_select.append(col_expr ~ ' AS ' ~ q(col_name)) %}
    {%- endfor %}

    {# ────────────────────────────────
       5.  Preserve original column order,
           applying overrides where present
    ──────────────────────────────── #}
    {%- set output_columns = [] %}
    {%- for c in schema %}
        {%- set override_found = false %}
        {%- for expr in columns_to_select %}
            {%- set alias = expr.split(' AS ')[1] | replace('`','') | upper %}
            {%- if c.name | replace('`','') | replace('"','') | upper == alias %}
                {% do output_columns.append(expr) %}
                {%- set override_found = true %}
                {%- break %}
            {%- endif %}
        {%- endfor %}
        {%- if not override_found %}
            {% do output_columns.append(q(c.name)) %}
        {%- endif %}
    {%- endfor %}

    {% set final_select %}
        SELECT {{ output_columns | join(', ') }} FROM cleansed_data
    {% endset %}

    {{ return( cleansed_cte ~ '\n' ~ final_select ) }}

{% endmacro %}