{#
    generate_sequence_generic(
        relation_names = None                -- string or list; optional
      , start_expr      =                   -- string (SQL literal or column/expression)
      , end_expr        =                   -- string (SQL literal or column/expression)
      , step_expr       = "1"               -- string (SQL literal or column/expression)
      , data_type       = "int"             -- "int" | "bigint" | "double" | "date" | "timestamp"
      , interval_unit   = "day"             -- for date/timestamp only
    )

    Examples
    --------
    -- 1️⃣  Stand-alone numeric sequence 11,14,17,20,23
    {{ generate_sequence_generic(None,"11","25","3","int") }}

    -- 2️⃣  Per-row daily dates between open/close
    {{ generate_sequence_generic(ref('accounts'),
                                 'accountOpenDt',
                                 'accountCloseDt',
                                 '1',
                                 'date',
                                 'day') }}

    -- 3️⃣  Hourly timestamps without a table
    {{ generate_sequence_generic(None,
                                 "'2025-06-19 00:00:00'",
                                 "'2025-06-19 06:00:00'",
                                 '1',
                                 'timestamp',
                                 'hour') }}
#}
{% macro generate_sequence_generic(
        relation_names   = None,
        start_expr       = none,
        end_expr         = none,
        step_expr        = "1",
        data_type        = "int",
        interval_unit    = "day"
    ) %}

{%- set numeric_types = ["int","integer","bigint","float","double","decimal"] -%}
{%- set step_int      = step_expr | string -%}

{#-- normalise relation_names to a list so we can test its length --#}
{%- if relation_names is none %}
    {%- set relations = [] -%}
{%- elif relation_names is string %}
    {%- set relations = [ relation_names ] -%}
{%- else %}
    {%- set relations = relation_names | list -%}
{%- endif %}

(
{% if relations | length == 0 %}
    {# ------------------------------------------------------------------
       NO INPUT TABLE -- just emit a single column called VALUE
       (still uses sequence so the syntax mirrors the relation branch)
    ------------------------------------------------------------------ #}
    {% if data_type in numeric_types %}
        SELECT explode(
                   sequence(
                       CAST({{ start_expr }} AS {{ data_type }}),
                       CAST({{ end_expr   }} AS {{ data_type }}),
                       CAST({{ step_expr  }} AS {{ data_type }})
                   )
               ) AS value

    {% elif data_type in ["date","timestamp"] %}
        SELECT explode(
                   sequence(
                       CAST({{ start_expr }} AS {{ data_type }}),
                       CAST({{ end_expr   }} AS {{ data_type }}),
                       interval CAST({{ step_int }} AS INT) {{ interval_unit }}
                   )
               ) AS value
    {% else %}
        SELECT NULL AS value WHERE FALSE  -- unsupported type
    {% endif %}

{% elif relations | length == 1 %}
    {# ------------------------------------------------------------------
       ONE INPUT TABLE -- cross join per row with generated sequence
    ------------------------------------------------------------------ #}
    {%- set rel = relations[0] %}
    {% if data_type in numeric_types %}
        SELECT
            r.*,
            seq_val AS value
        FROM {{ rel }}           AS r
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
            seq_val AS value
        FROM {{ rel }}           AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                interval CAST({{ step_int }} AS INT) {{ interval_unit }}
            )
        ) t AS seq_val
    {% else %}
        SELECT NULL AS value WHERE FALSE
    {% endif %}

{% else %}
    {# ------------------------------------------------------------------
       MORE THAN ONE RELATION PROVIDED → give an explicit error
    ------------------------------------------------------------------ #}
    SELECT NULL AS value
    WHERE FALSE
    /* Error: generate_sequence_generic expects 0 or 1 relation, got {{ relations | length }} */
{% endif %}
)
{% endmacro %}