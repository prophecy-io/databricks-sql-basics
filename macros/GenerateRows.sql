{# ---------------------------------------------------------------------------
   generate_sequence_generic
   ---------------------------------------------------------------------------
   Parameters (all passed as strings)
   ----------------------------------
   relation_names : string | list | None   -- optional table/CTE; 0 or 1 allowed
   output_col     : string                 -- name for the generated column
   start_expr     : string (required)      -- SQL literal or column/expression
   end_expr       : string (required)      -- SQL literal or column/expression
   step_expr      : string  (default "1")  -- SQL literal or column/expression
   data_type      : string  (default "int")-- numeric | date | timestamp
   interval_unit  : string  (default "day")-- for date/timestamp sequences

   Examples
   --------
   -- Stand-alone numeric sequence 11,14,17,20,23:
   {{ generate_sequence_generic(None,'number',"11","25","3","int","day") }}

   -- Per-row daily dates between account open/close:
   {{ generate_sequence_generic(
        relation_names = ref('accounts'),
        output_col     = 'txn_date',
        start_expr     = 'accountOpenDt',
        end_expr       = 'accountCloseDt',
        step_expr      = '1',
        data_type      = 'date',
        interval_unit  = 'day'
   ) }}
--------------------------------------------------------------------------- #}
{% macro GenerateRows(
        relation_names = None,
        output_col     = 'value',
        start_expr     = None,
        end_expr       = None,
        step_expr      = "1",
        data_type      = "int",
        interval_unit  = "day"
    ) %}
{%- set numeric_types = ["int","integer","bigint","float","double","decimal"] %}

{#-- Normalise relation_names into a list so we can test its length --#}
{%- if relation_names is none %}
    {%- set relations = [] %}
{%- elif relation_names is string %}
    {%- set relations = [relation_names] %}
{%- else %}
    {%- set relations = relation_names | list %}
{%- endif %}

(
{% if relations | length == 0 %}
    {# ..............................................................
       NO INPUT TABLE — emit only the generated sequence
    ................................................................ #}
    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     CAST({{ step_expr  }} AS {{ data_type }})
                 )
               ) AS {{ output_col }}

    {% elif data_type in ["date","timestamp"] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval CAST({{ step_expr }} AS INT) {{ interval_unit }}
                 )
               ) AS {{ output_col }}

    {% else %}
        SELECT NULL AS {{ output_col }} WHERE FALSE
    {% endif %}

{% elif relations | length == 1 %}
    {# ..............................................................
       ONE INPUT TABLE — cross-join per row with generated sequence
    ................................................................ #}
    {%- set rel = relations[0] %}
    {% if data_type in numeric_types %}
        SELECT
            r.*,
            seq_val AS {{ output_col }}
        FROM {{ rel }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                CAST({{ step_expr  }} AS {{ data_type }})
            )
        ) t AS seq_val

    {% elif data_type in ["date","timestamp"] %}
        SELECT
            r.*,
            seq_val AS {{ output_col }}
        FROM {{ rel }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                interval CAST({{ step_expr }} AS INT) {{ interval_unit }}
            )
        ) t AS seq_val

    {% else %}
        SELECT NULL AS {{ output_col }} WHERE FALSE
    {% endif %}

{% else %}
    {# ..............................................................
       >1 relations supplied — explicit error
    ................................................................ #}
    SELECT NULL AS {{ output_col }} WHERE FALSE
    /* Error: generate_sequence_generic expects 0 or 1 relation,
              received {{ relations | length }} */
{% endif %}
)
{% endmacro %}