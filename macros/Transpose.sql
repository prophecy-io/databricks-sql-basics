
{%- macro Transpose(
    relation_name,
    schema,
    keyColumns,
    dataColumns) -%}

  {%- set available_cols = [] -%}
  {%- for col in dataColumns -%}
    {%- if col in schema -%}
      {%- do available_cols.append(col) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set union_queries = [] -%}
  {%- for data_col in available_cols -%}
    {%- set select_list = [] -%}
    {%- for key in keyColumns -%}
      {%- do select_list.append(key) -%}
    {%- endfor -%}
    {%- do select_list.append("'" ~ data_col ~ "' as Name") -%}
    {%- do select_list.append('CAST(' ~ data_col ~ ' as string) as Value') -%}
    
    {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~ ' FROM ' ~ relation_name -%}
    {%- do union_queries.append(query) -%}
  {%- endfor -%}

  {{ union_queries | join('\nUNION ALL\n') }}

{%- endmacro -%}