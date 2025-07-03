{%- macro Transpose(
    relation_name,
    keyColumns,
    dataColumns,
    nameColumn,
    valueColumn,
    schema=[]) -%}

  {#— simple helper to quote identifiers —#}
  {%- set quote = lambda x: '`' ~ x ~ '`' -%}

  {%- set available_cols = [] -%}
  {%- if dataColumns and nameColumn | length > 0 and valueColumn | length > 0 -%}

    {%- set union_queries = [] -%}
    {%- for data_col in dataColumns -%}
      {%- set select_list = [] -%}

      {%- if keyColumns -%}
        {%- for key in keyColumns -%}
          {# quote each key column #}
          {%- do select_list.append(quote(key)) -%}
        {%- endfor -%}
      {%- endif -%}

      {# literal value for “which column”, plus quoted alias #}
      {%- do select_list.append("'" ~ data_col ~ "' as " ~ quote(nameColumn)) -%}

      {# value of the data column, cast to string, with quoted alias #}
      {%- do select_list.append('CAST(' ~ quote(data_col) ~ ' AS STRING) AS ' ~ quote(valueColumn)) -%}

      {%- set query = 'SELECT ' ~ (select_list | join(', ')) ~ ' FROM ' ~ relation_name -%}
      {%- do union_queries.append(query) -%}
    {%- endfor -%}

    {{ union_queries | join('\nUNION ALL\n') }}
  {%- else -%}
    SELECT * FROM {{ relation_name }}
  {%- endif -%}

{%- endmacro -%}