{% macro MultiColumnRename(
    relation,
    schema,
    columnNames,
    renameMethod,
    editOperation = '',
    editType = '',
    editWith = '',
    customExpression='') 
%}
    {%- set renamed_columns = [] -%}

    {%- for column in columnNames -%}
        {%- set renamed_column = "" -%}
        {%- if renameMethod == 'editPrefixSuffix' -%}
            {%- if editOperation == 'Add' -%}
                {%- if editType == 'Prefix' -%}
                    {%- set renamed_column = column ~ " AS " ~ editWith ~ column -%}
                {%- else -%}
                    {%- set renamed_column = column ~ " AS " ~ column ~ editWith -%}
                {%- endif -%}
            {%- endif -%}

        {%- elif renameMethod == 'advancedRename' -%}
                {%- set custom_expr_result = DatabricksSqlBasics.evaluate_expression(customExpression | replace('column_name',  "\'" ~ column ~ "\'"),column) -%}
                {%- set custom_expr_result_trimmed = custom_expr_result | trim -%}
                {%- set renamed_column = column ~ " AS " ~ custom_expr_result_trimmed -%}           
        {%- endif -%}
        
        {%- do renamed_columns.append(renamed_column) -%}
    {%- endfor -%}

    {# Get the schema of cleansed data #}
    {%- set output_columns = [] -%}
    {%- for col_name_val in schema -%}
        {% set flag_dict = {"flag": false} %}
        {%- for expr in renamed_columns -%}
            {# Split on 'AS' to get the orig column name; assumes expression contains "AS" #}
            {%- set parts = expr.split(' AS ') -%}
            {%- set orig_col_name = parts[0] | trim | upper -%}
            {%- if (col_name_val | trim | upper) == orig_col_name -%}
                {%- do output_columns.append(expr) -%}
                {% do flag_dict.update({"flag": true}) %}
                {%- break -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if flag_dict.flag == false -%}    
            {%- do output_columns.append(col_name_val) -%}
        {%- endif -%}
    {%- endfor -%}

    select 
        {{ output_columns | join(',\n    ') }}
    from {{ relation }}
{% endmacro %}