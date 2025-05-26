{# ------------------------------------------------------------------
   UnionByName
   Parameters
     relation_names   – list OR comma-separated string of relations
     schemas          – list of schema JSON blobs or already-parsed dicts
     missingColumnOps – 'allowMissingColumns'   (default)
                        'nameBasedUnionOperation'
   ------------------------------------------------------------------ #}

{% macro UnionByName(relation_names,
                     schemas,
                     missingColumnOps='allowMissingColumns') -%}

    {# 1. Build a Python list called `relations` #}
    {%- if relation_names is string -%}
        {%- set relations = [] -%}
        {%- for rel in relation_names.split(',') -%}
            {%- do relations.append(rel | trim) -%}
        {%- endfor -%}
    {%- else -%}
        {%- set relations = relation_names | list -%}
    {%- endif -%}

    {# 2. For each schema capture column list #}
    {%- set columns_per_relation = [] -%}
    {%- for schema_blob in schemas -%}
        {%- if schema_blob is string -%}
            {%- set parsed = fromjson(schema_blob) -%}
        {%- else -%}
            {%- set parsed = schema_blob -%}
        {%- endif -%}

        {%- set col_list = [] -%}
        {%- for f in parsed -%}
            {%- do col_list.append(f.name) -%}
        {%- endfor -%}
        {%- do columns_per_relation.append(col_list) -%}
    {%- endfor -%}

    {# 3. Decide the final column set and validate when required #}
    {%- set final_columns = columns_per_relation[0] | list -%}

    {%- if missingColumnOps == 'allowMissingColumns' -%}
        {%- for other_cols in columns_per_relation[1:] -%}
            {%- for c in other_cols if c not in final_columns -%}
                {%- do final_columns.append(c) -%}
            {%- endfor -%}
        {%- endfor -%}

    {%- elif missingColumnOps == 'nameBasedUnionOperation' -%}
        {%- for idx in range(1, relations | length) -%}
            {%- set current = columns_per_relation[idx] -%}
            {%- set extra   = [] -%}
            {%- set missing = [] -%}

            {%- for c in current if c not in final_columns -%}
                {%- do extra.append(c) -%}
            {%- endfor -%}
            {%- for c in final_columns if c not in current -%}
                {%- do missing.append(c) -%}
            {%- endfor -%}

            {%- if extra | length > 0 or missing | length > 0 -%}
                {{ exceptions.raise_compiler_error(
                    "Column mismatch between first relation and relation "
                    ~ (idx + 1) ~ ". Extra: " ~ extra | join(', ')
                    ~ " | Missing: " ~ missing | join(', ')
                ) }}
            {%- endif -%}
        {%- endfor -%}

    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "Unsupported missingColumnOps value: " ~ missingColumnOps) }}
    {%- endif -%}

    {# 4. Build SELECT for each relation (insert NULLs for missing cols) #}
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

        {%- do selects.append(
              "select " ~ parts | join(', ') ~ " from " ~ relations[idx]) -%}
    {%- endfor -%}

    {# 5. Union-all them together #}
    with union_query as (
        {{ selects | join('\nunion all\n') }}
    )
    select *
    from union_query

{%- endmacro %}