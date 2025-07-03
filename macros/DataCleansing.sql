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

    {# a single back-tick we can reuse everywhere #}
    {% set bt = "`" %}

    {# ───────────── 1. cleansed_data CTE (row-level) ───────────── #}
    {% set cleansed_cte %}
        WITH cleansed_data AS (
            SELECT *
            FROM {{ relation_name }}
            {% if removeRowNullAllCols %}
                WHERE
                {%- set conds = [] -%}
                {%- for c in schema %}
                    {%- do conds.append(bt ~ c.name ~ bt ~ ' IS NOT NULL') %}
                {%- endfor %}
                {{ conds | join(' OR ') }}
            {% endif %}
        )
    {% endset %}

    {# no column list? return everything, we’re done #}
    {% if columnNames | length == 0 %}
        {{ return(cleansed_cte ~ '\nSELECT * FROM cleansed_data') }}
    {% endif %}

    {# ───────────── 2. helpers ───────────── #}
    {% set numeric_types = [
        "bigint","decimal","double","float",
        "integer","smallint","tinyint"
    ] %}

    {% set col_type_map = {} %}
    {% for c in schema %}
        {% do col_type_map.update({ c.name: c.dataType | lower }) %}
    {% endfor %}

    {% set columns_to_select = [] %}

    {# ───────────── 3. per-column transforms ───────────── #}
    {% for col_name in columnNames %}
        {% set col_expr = bt ~ col_name ~ bt %}

        {# numeric null replacement #}
        {% if col_type_map.get(col_name) in numeric_types
              and replaceNullForNumericFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" %}
        {% endif %}

        {# string rules #}
        {% if col_type_map.get(col_name) == "string" %}
            {% if replaceNullTextFields %}
                {% set col_expr = "COALESCE(" ~ col_expr ~ ", '" ~ replaceNullTextWith ~ "')" %}
            {% endif %}
            {% if trimWhiteSpace %}
                {% set col_expr = "LTRIM(RTRIM(" ~ col_expr ~ "))" %}
            {% endif %}
            {% if removeTabsLineBreaksAndDuplicateWhitespace %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', ' ')" %}
            {% endif %}
            {% if allWhiteSpace %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', '')" %}
            {% endif %}
            {% if cleanLetters %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[A-Za-z]+', '')" %}
            {% endif %}
            {% if cleanPunctuations %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^a-zA-Z0-9\\\\s]', '')" %}
            {% endif %}
            {% if cleanNumbers %}
                {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\d+', '')" %}
            {% endif %}

            {% if modifyCase == 'makeLowercase' %}
                {% set col_expr = "LOWER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeUppercase' %}
                {% set col_expr = "UPPER(" ~ col_expr ~ ")" %}
            {% elif modifyCase == 'makeTitlecase' %}
                {% set col_expr = "INITCAP(" ~ col_expr ~ ")" %}
            {% endif %}
        {% endif %}

        {# date / timestamp null replacement #}
        {% if col_type_map.get(col_name) == 'date' and replaceNullDateFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", DATE '" ~ replaceNullDateWith ~ "')" %}
        {% endif %}
        {% if col_type_map.get(col_name) == 'timestamp' and replaceNullTimeFields %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", TIMESTAMP '" ~ replaceNullTimeWith ~ "')" %}
        {% endif %}

        {# final cast & alias #}
        {% set col_expr = "CAST(" ~ col_expr ~ " AS " ~ col_type_map.get(col_name) ~ ")" %}
        {% do columns_to_select.append(col_expr ~ ' AS ' ~ bt ~ col_name ~ bt) %}
    {% endfor %}

    {# ───────────── 4. keep original column order ───────────── #}
    {% set output_columns = [] %}
    {% for c in schema %}
        {% set override_expr = None %}
        {% for expr in columns_to_select %}
            {% set alias = expr.split(' AS ')[1] | replace('`','') | upper %}
            {% if c.name | replace('`','') | replace('"','') | upper == alias %}
                {% set override_expr = expr %}
                {% break %}
            {% endif %}
        {% endfor %}
        {% if override_expr %}
            {% do output_columns.append(override_expr) %}
        {% else %}
            {% do output_columns.append(bt ~ c.name ~ bt) %}
        {% endif %}
    {% endfor %}

    {% set final_select %}
        SELECT {{ output_columns | join(', ') }} FROM cleansed_data
    {% endset %}

    {{ return(cleansed_cte ~ '\n' ~ final_select) }}

{% endmacro %}