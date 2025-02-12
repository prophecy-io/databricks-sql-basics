{% macro DataCleansing(
    relation,
    schema,
    null_operation="remove_row_null_all_cols",
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
    makeLowercase=False,
    makeUppercase=False,
    makeTitlecase=False
) %}
    {%- set columns_to_select = [] %}

    -- Step 1: Handle null_operation flag
    {% if null_operation == "remove_row_null_all_cols" %}
        -- Remove rows where all columns are null
        {% set where_clause = [] %}
        {% for col in columnNames %}
            {% do where_clause.append('`' ~ col ~ '` IS NOT NULL') %}
        {% endfor %}
        {%- set where_clause_sql = where_clause | join(' OR ') %}
        
        WITH cleansed_data AS (
            SELECT *
            FROM {{ relation }}
            WHERE {{ where_clause_sql }}
        )
    {% elif null_operation == "remove_col_null_all_rows" %}
        -- Remove columns where all rows are null
        WITH cleansed_data AS (
            SELECT {% for col in columnNames %}
                CASE WHEN COUNT(`{{ col }}`) OVER() > 0 THEN `{{ col }}` ELSE NULL END AS `{{ col }}`{% if not loop.last %}, {% endif %}
            {% endfor %}
            FROM {{ relation }}
        )
    {% elif null_operation == "remove_empty_rows_cols" %}
        -- Remove rows where all columns are null, and columns where all rows are null
        {% set where_clause = [] %}
        {% for col in columnNames %}
            {% do where_clause.append('`' ~ col ~ '` IS NOT NULL') %}
        {% endfor %}
        {%- set where_clause_sql = where_clause | join(' OR ') %}
        
        WITH cleansed_data AS (
            SELECT {% for col in columnNames %}
                CASE WHEN `{{ col }}` IS NOT NULL THEN `{{ col }}` ELSE NULL END AS `{{ col }}`{% if not loop.last %}, {% endif %}
            {% endfor %}
            FROM {{ relation }}
            WHERE {{ where_clause_sql }}
        )
    {% else %}
        -- If no null_operation is specified, just select all columns without filtering
        WITH cleansed_data AS (
            SELECT *
            FROM {{ relation }}
        )
    {% endif %}

    {% set col_type_map = {} %}
    {% for col in schema %}
        {% set col_type_map = col_type_map.update({ col.name: col.dataType | lower }) %}
    {% endfor %}

    {{ log(col_type_map, info = True) }}

    -- Step 2: Apply data cleansing operations
    {% for col_name in columnNames %}
        {% set col_expr = '`' ~ col_name ~ '`' %}

        {% if ( col_type_map.get(col_name) == "integer" and replaceNullForNumericFields ) %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", " ~ replaceNullNumericWith | string ~ ")" %}
        {% endif %}
        {% if ( col_type_map.get(col_name) == "string" and replaceNullTextFields ) %}
            {% set col_expr = "COALESCE(" ~ col_expr ~ ", '" ~ replaceNullTextWith ~ "')" %}
        {% endif %}

        {% if trimWhiteSpace %}
            {% set col_expr = "TRIM(" ~ col_expr ~ ")" %}
        {% endif %}

        {% if removeTabsLineBreaksAndDuplicateWhitespace %}
            {% set col_expr = "REPLACE(REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', ' '), '\\n', '')" %}
        {% endif %}

        {% if allWhiteSpace %}
            {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\s+', '')" %}
        {% endif %}

        {% if cleanLetters %}
            {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^A-Za-z]', '')" %}
        {% endif %}

        {% if cleanPunctuations %}
            {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '[^a-zA-Z0-9\\s]', '')" %}
        {% endif %}

        {% if cleanNumbers %}
            {% set col_expr = "REGEXP_REPLACE(" ~ col_expr ~ ", '\\\\d+', '')" %}
        {% endif %}

        {% if makeLowercase %}
            {% set col_expr = "LOWER(" ~ col_expr ~ ")" %}
        {% endif %}

        {% if makeUppercase %}
            {% set col_expr = "UPPER(" ~ col_expr ~ ")" %}
        {% endif %}

        {% if makeTitlecase %}
            {% set col_expr = "INITCAP(" ~ col_expr ~ ")" %}
        {% endif %}

        -- Append the transformed column expression
        {% set col_expr = col_expr ~ " :: " ~ col_type_map.get(col_name) %}
        {% do columns_to_select.append(col_expr ~ " AS `" ~ col_name ~ "`") %}
    {% endfor %}

    -- Final SQL statement with transformations applied
    SELECT
        {{ columns_to_select | join(',\n    ') }}
    FROM cleansed_data
{% endmacro %}
