{% macro UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}

    {# 1 / Normalise relation list #}
    {%- if relation_names is string -%}
        {%- set relations = relation_names.split(',') | map('trim') | list -%}
    {%- else -%}
        {%- set relations = relation_names | list -%}
    {%- endif -%}

    {# 2 / Extract column names for each relation (upper-case for case-insensitive match) #}
    {%- set columns_per_relation = [] -%}
    {%- for schema_blob in schemas -%}
        {%- if schema_blob is string -%}
            {%- set parsed = fromjson(schema_blob) -%}
        {%- else -%}
            {%- set parsed = schema_blob -%}
        {%- endif -%}

        {%- set col_list = [] -%}
        {%- for f in parsed -%}
            {%- do col_list.append(f.name | upper) -%}
        {%- endfor -%}
        {%- do columns_per_relation.append(col_list) -%}
    {%- endfor -%}

    {# 3 / Determine final column set and validate #}
    {%- set final_columns = columns_per_relation[0] | list -%}

    {%- if missingColumnOps == 'allowMissingColumns' -%}
        {%- for col_list in columns_per_relation[1:] -%}
            {%- for c in col_list if c not in final_columns -%}
                {%- do final_columns.append(c) -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- elif missingColumnOps == 'nameBasedUnionOperation' -%}
        {%- for i in range(1, columns_per_relation | length) -%}
            {%- set cl = columns_per_relation[i] -%}
            {%- set diff1 = [] -%}
            {%- set diff2 = [] -%}

            {%- for c in cl if c not in final_columns -%}
                {%- do diff1.append(c) -%}
            {%- endfor -%}
            {%- for c in final_columns if c not in cl -%}
                {%- do diff2.append(c) -%}
            {%- endfor -%}

            {%- if diff1 | length > 0 or diff2 | length > 0 -%}
                {{ exceptions.raise_compiler_error(
                    "Column mismatch between first relation and relation #" ~ (i + 1) ~
                    ". Extra in that relation: " ~ diff1 | join(', ') ~
                    " | Missing: " ~ diff2 | join(', ')
                ) }}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "Unsupported missingColumnOps value: " ~ missingColumnOps) }}
    {%- endif -%}

    {# 4 / Build SELECT for each relation (pad missing columns with NULL) #}
    {%- set selects = [] -%}
    {%- for idx in range(relations | length) -%}
        {%- set cur_cols = columns_per_relation[idx] -%}
        {%- set parts = [] -%}
        {%- for col in final_columns -%}
            {%- if col in cur_cols -%}
                {%- do parts.append(col) -%}
            {%- else -%}
                {%- do parts.append("null as " ~ col) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do selects.append("select " ~ (parts | join(', ')) ~ " from " ~ relations[idx]) -%}
    {%- endfor -%}

    {# 5 / Union-all them together #}
    with union_query as (
        {{ selects | join('\nunion all\n') }}
    )
    select *
    from union_query

{%- endmacro %}