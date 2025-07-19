{%- macro DataMasking(
    relation_name,
    columnNames,
    remaining_columns,
    maskingMethod,
    upperCharSubstitute,
    lowerCharSubstitute,
    digitCharSubstitute,
    otherCharSubstitute,
    sha2BitLength,
    maskedColumnAdditionMethod,
    prefix_suffix_opt,
    prefix_suffix_val,
    combinedHashColumnName
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
            {%- if maskedColumnAdditionMethod == "inplace_substitute" -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("mask(" ~ arg_string ~ ") AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- elif maskingMethod == "hash" -%}
        {%- if maskedColumnAdditionMethod == "combinedHash_substitute" -%}
            {%- set arg_string = columnNames | join(', ') -%}
            {%- do withColumn_clause.append("hash(" ~ arg_string ~ ") AS " ~ combinedHashColumnName) -%}
        {%- else  -%}
            {% for column in columnNames %}
                {%- if maskedColumnAdditionMethod == "inplace_substitute" -%}
                    {%- do withColumn_clause.append("hash(" ~ column ~ ") AS " ~ column) -%}
                {%- elif prefix_suffix_opt == "Prefix" -%}
                    {%- do withColumn_clause.append("hash(" ~ column ~ ") AS " ~ prefix_suffix_val ~ column) -%}
                {%- else -%}
                    {%- do withColumn_clause.append("hash(" ~ column ~ ") AS " ~ column ~ prefix_suffix_val) -%}
                {%- endif -%}
            {% endfor %}
        {%- endif -%}

    {%- elif maskingMethod == "sha2" -%}
        {% for column in columnNames %}
            {%- if maskedColumnAdditionMethod == "inplace_substitute" -%}
                {%- do withColumn_clause.append("sha2(" ~ column ~ ", " ~ sha2BitLength ~ ") AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("sha2(" ~ column ~ ", " ~ sha2BitLength ~ ") AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("sha2(" ~ column ~ ", " ~ sha2BitLength ~ ") AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- else -%}
        {% for column in columnNames %}
            {%- if maskedColumnAdditionMethod == "inplace_substitute" -%}
                {%- do withColumn_clause.append(maskingMethod ~ "(" ~ column ~ ") AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append(maskingMethod ~ "(" ~ column ~ ") AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append(maskingMethod ~ "(" ~ column ~ ") AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}

    {%- endif -%}

    {%- set select_clause_sql = withColumn_clause | join(', ') -%}

    {%- set select_cte_sql -%}
        {%- if select_clause_sql == "" -%}
            WITH final_cte AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- elif (maskedColumnAdditionMethod == "prefix_suffix_substitute") or (maskingMethod == "hash" and maskedColumnAdditionMethod == "combinedHash_substitute") -%}
            WITH final_cte AS (
                SELECT *, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- elif remaining_columns == "" -%}
            WITH final_cte AS (
                SELECT {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- else -%}
            WITH final_cte AS (
                SELECT {{ remaining_columns }}, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_cte_sql ~ "\nSELECT * FROM final_cte" -%}

    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro -%}
