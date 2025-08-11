{% macro JSONParse(model, json_column, record_id_col, json_parse_method, sampleRecord, sampleSchema, max_depth=1) %}

{%- if json_parse_method != 'parseFromSampleRecord' and json_parse_method != 'parseFromSchema' -%}
      {%- set level_condition = '' -%}
      {%- if json_parse_method == 'output_unnest_json_field' or json_parse_method == 'output_flatten_array' -%}
        {% set level_condition = 'AND depth < ' ~ max_depth %}
      {%- endif -%}

      {%- set array_condition = '' -%}
      {%- if json_parse_method == 'output_flatten_array' -%}
        {% set array_condition = 'AND try_cast(value as array<variant>) is not null ' %}
      {%- endif -%}

      {%- if record_id_col != '' -%}
        WITH RECURSIVE json_flatten_base AS (
          SELECT {{ record_id_col }}, '' as path, try_parse_json({{ json_column }}) AS value, try_parse_json({{ json_column }}) as parsed_variant_js from {{ model }}
        )

      {%- else -%}
        {%- set record_id_col = 'recordId' -%}
        WITH RECURSIVE json_flatten_base AS (
          SELECT row_number() over(order by {{ json_column }}) as {{ record_id_col }}, '' as path, try_parse_json({{ json_column }}) AS value, try_parse_json({{ json_column }}) as parsed_variant_js from {{ model }}
        )
      {%- endif -%}

      , json_flatten_levels AS (
      -- Base case: start from the root struct/array, at depth 0
      SELECT {{ record_id_col }}, path, '' as op_path,
      value, parsed_variant_js,
      0 AS depth
      FROM json_flatten_base
      UNION ALL
      -- Recursive step:
      -- For each struct/array, variant_explode
      -- and recurse into them, increasing the depth by 1
      SELECT {{ record_id_col }},
      case when path = '' and k is NULL then concat('[', pos, ']')
          when path = '' and k is not NULL then k
          when path <> '' and k is NULL then concat(path, '[', pos, ']')
          when path <> '' and k is not NULL then concat(path, '.', k)
          end as path,
      case when op_path = '' and k is NULL then pos
          when op_path = '' and k is not NULL then k
          when op_path <> '' and k is NULL then concat(op_path, '.', pos)
          when op_path <> '' and k is not NULL then concat(op_path, '.', k)
          end as op_path,
      report, parsed_variant_js,
      depth + 1
        FROM json_flatten_levels
        JOIN LATERAL variant_explode(value) AS t (pos, k, report)
      WHERE value IS NOT NULL {{ level_condition }} {{ array_condition }}
      )

      , all_levels_cte AS (
        SELECT
          {{ record_id_col }},
          path,
          op_path,
          value,
          parsed_variant_js,
          depth as level_num,
          schema_of_variant(try_variant_get(parsed_variant_js, CONCAT(case when path RLIKE '^\\[\\d+\\]' then '$' else '$.' end, path))) as inferred_datatype
        FROM json_flatten_levels
        WHERE path IS NOT NULL AND path <> ''
        ORDER BY {{ record_id_col }}
      )

      {%- if json_parse_method == 'output_datatype_specific_field' -%}
        select {{ record_id_col }}, concat('{{ json_column }}', '.', op_path) as JSON_Name,
          case when inferred_datatype = 'STRING' then value::string else null end as JSON_ValueString,
          case when inferred_datatype = 'BIGINT' then value::int else null end as JSON_ValueInt,
          case when inferred_datatype RLIKE '^DECIMAL' then value::float else null end as JSON_ValueFloat,
          case when inferred_datatype = 'BOOLEAN' then value::boolean else null end as JSON_ValueBool
        from all_levels_cte where inferred_datatype in ('STRING', 'BIGINT', 'BOOLEAN') or inferred_datatype RLIKE '^DECIMAL'

      {%- elif json_parse_method == 'output_single_string_field' -%}
        select {{ record_id_col }}, concat('{{ json_column }}', '.', op_path) as JSON_Name, value::string as JSON_ValueString
        from all_levels_cte where inferred_datatype in ('STRING', 'BIGINT', 'BOOLEAN') or inferred_datatype RLIKE '^DECIMAL'

      {%- elif json_parse_method == 'output_unnest_json_field' -%}
        select {{ record_id_col }}, concat('{{ json_column }}', '.', op_path) as JSON_Name, value::string as JSON_ValueString
        from all_levels_cte

      {%- elif json_parse_method == 'output_flatten_array' -%}
        select {{ record_id_col }}, value::string as {{ json_column }}_flatten, substring(path, 2, length(path)-2) as {{ json_column }}_idx from all_levels_cte
          UNION ALL
        select {{ record_id_col }}, null, null from json_flatten_base where try_cast(parsed_variant_js as array<variant>) is null
      {%- endif -%}

{%- else -%}
      {%- if not json_column or json_column | trim == '' -%}
        select * from {{ model }}

      {%- elif json_parse_method == 'parseFromSchema' and (not sampleSchema or sampleSchema | trim == '') -%}
          select * from {{ model }}

      {%- elif json_parse_method == 'parseFromSampleRecord' and (not sampleRecord or sampleRecord | trim == '') -%}
          select * from {{ model }}

      {%- else -%}
          {%- set quoted_col = adapter.quote(json_column) -%}
          {%- set alias_col = adapter.quote(json_column ~ '_parsed') -%}

          {%- if json_parse_method == 'parseFromSchema' -%}
              select
                  *,
                  from_json({{ quoted_col }}, '{{ sampleSchema | replace("\n", " ") }}') as {{ alias_col }}
              from {{ model }}

          {%- elif json_parse_method == 'parseFromSampleRecord' -%}
              select
                  *,
                  from_json({{ quoted_col }}, schema_of_json('{{ sampleRecord | replace("\n", " ") }}')) as {{ alias_col }}
              from {{ model }}

          {%- elif json_parse_method == 'none' or not json_parse_method -%}
              select * from {{ model }}

          {%- else -%}
              {{ exceptions.raise_compiler_error(
                  "Invalid parsingMethod: '" ~ json_parse_method ~ "'. Expected 'parseFromSchema', 'parseFromSampleRecord', or 'none'."
              ) }}
          {%- endif -%}
      {%- endif -%}

{%- endif -%}

{% endmacro %}
