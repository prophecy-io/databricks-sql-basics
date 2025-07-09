{%- macro DataMasking(
    relation_name,
    columnNames,
    maskingMethod,
    upperCharSubstitute,
    lowerCharSubstitute,
    digitCharSubstitute,
    otherCharSubstitute,
    sha2BitLength,
    combinedHash
) %}

    {{ log("Applying Masking-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}
    {%- if maskingMethod == "mask" -%}
        {% for column in columnNames %}
            {%- set args = [column] -%}

            {%- if upperCharSubstitute == "NULL" -%}
                {%- do args.append("upperChar => NULL") -%}
            {%- elif upperCharSubstitute != "" -%}
                {%- do args.append("upperChar => '" ~ upperCharSubstitute ~ "'") -%}
            {%- endif -%}

            {%- if lowerCharSubstitute == "NULL" -%}
                {%- do args.append("lowerChar => NULL") -%}
            {%- elif lowerCharSubstitute != "" -%}
                {%- do args.append("lowerChar => '" ~ lowerCharSubstitute ~ "'") -%}
            {%- endif -%}

            {%- if digitCharSubstitute == "NULL" -%}
                {%- do args.append("digitChar => NULL") -%}
            {%- elif digitCharSubstitute != "" -%}
                {%- do args.append("digitChar => '" ~ digitCharSubstitute ~ "'") -%}
            {%- endif -%}

            {%- if otherCharSubstitute == "NULL" -%}
                {%- do args.append("otherChar => NULL") -%}
            {%- elif otherCharSubstitute != "" -%}
                {%- do args.append("otherChar => '" ~ otherCharSubstitute ~ "'") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ column ~ "_mask") -%}
        {% endfor %}

    {%- elif maskingMethod == "hash" -%}
        {%- if combinedHash == "True" -%}
            {%- set arg_string = columnNames | join(', ') -%}
            {%- do withColumn_clause.append("hash(" ~ arg_string ~ ") AS aggregate_hash") -%}
        {%- else -%}
            {% for column in columnNames %}
                {%- do withColumn_clause.append("hash(" ~ column ~ ") AS " ~ column ~ "_hash") -%}
            {% endfor %}
        {%- endif -%}

    {%- elif maskingMethod == "sha2" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("sha2(" ~ column ~ ", " ~ sha2BitLength ~ ") AS " ~ column ~ "_sha2") -%}
        {% endfor %}

    {%- else -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append(maskingMethod ~ "(" ~ column ~ ") AS " ~ column ~ "_" ~ maskingMethod) -%}
        {% endfor %}

    {%- endif -%}

    {%- set select_clause_sql = withColumn_clause | join(', ') -%}

    {%- set select_cte_sql -%}
        {%- if select_clause_sql == "" -%}
            WITH final_cte AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- else -%}
            WITH final_cte AS (
                SELECT *, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_cte_sql ~ "\nSELECT * FROM final_cte" -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}

