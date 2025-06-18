{# =============================================================================
    GenerateRows
    =============================================================================
    Parameters (all values come in as strings)
    -----------------------------------------------------------------------------
      1  relation_names : '' | 'schema.table' | 'tbl1,tbl2' | list | None
           – '', None, or all-blank ⇒ no table (stand-alone sequence)
           – exactly one non-blank  ⇒ cross-join per row
           – >1                      ⇒ explicit error

      2  new_field_name : string         -- name of the generated column
      3  start_expr     : string         -- literal or column/expression
      4  end_expr       : string         -- literal or column/expression
      5  step_expr      : string         -- literal or column/expression (default "1")
      6  data_type      : string         -- numeric | date | timestamp    (default "int")
      7  interval_unit  : string         -- Spark interval unit for temporal types
                                           (default "day")
============================================================================= #}
{% macro GenerateRows(
        relation_names = None,
        new_field_name = 'value',
        start_expr     = None,
        end_expr       = None,
        step_expr      = "1",
        data_type      = "int",
        interval_unit  = "day"
    ) %}

{%- set numeric_types = ["int","integer","bigint","float","double","decimal"] -%}

{#----------------------------------------------------------------------------
   0️⃣  Normalise relation_names  →  list `relations`
----------------------------------------------------------------------------#}
{% set tmp = [] %}
{% if relation_names is none %}
    {# nothing to do #}
{% elif relation_names is string %}
    {% for part in relation_names.split(',') %}
        {% if part | trim != '' %}
            {% do tmp.append(part | trim) %}
        {% endif %}
    {% endfor %}
{% else %}
    {% for item in relation_names | list %}
        {% if item is string %}
            {% for part in item.split(',') %}
                {% if part | trim != '' %}
                    {% do tmp.append(part | trim) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}
{% set relations = tmp %}

(   {# ------------- OPEN PAREN so caller can SELECT * FROM (…) AS t ------------- #}

{#----------------------------------------------------------------------------
   1️⃣  NO INPUT RELATION  ------------------------------------------------------
----------------------------------------------------------------------------#}
{% if relations | length == 0 %}

    {# ---------- numeric ----------------------------------------------------- #}
    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     /* ensure step sign matches direction */
                     (CASE
                          WHEN (CAST({{ step_expr }} AS {{ data_type }}) = 0)
                               THEN 1    -- safety: avoid 0 step
                          WHEN (CAST({{ start_expr }} AS {{ data_type }})
                                >   CAST({{ end_expr }}   AS {{ data_type }}))
                               THEN -ABS(CAST({{ step_expr }} AS {{ data_type }}))
                          ELSE  ABS(CAST({{ step_expr }} AS {{ data_type }}))
                      END)
                 )
               ) AS {{ new_field_name }}

    {# ---------- dates / timestamps ----------------------------------------- #}
    {% elif data_type in ["date","timestamp"] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval
                     (CASE
                          WHEN (CAST({{ start_expr }} AS {{ data_type }}) >
                                CAST({{ end_expr   }} AS {{ data_type }}))
                               THEN -ABS(CAST({{ step_expr }} AS INT))
                          ELSE  ABS(CAST({{ step_expr }} AS INT))
                      END) {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   2️⃣  EXACTLY ONE RELATION  ---------------------------------------------------
----------------------------------------------------------------------------#}
{% elif relations | length == 1 %}
    {%- set rel = relations[0] -%}

    {% if data_type in numeric_types %}
        SELECT
            r.*,
            seq_val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                (CASE
                     WHEN (CAST({{ step_expr }} AS {{ data_type }}) = 0)
                          THEN 1
                     WHEN (CAST({{ start_expr }} AS {{ data_type }})
                           >   CAST({{ end_expr }}   AS {{ data_type }}))
                          THEN -ABS(CAST({{ step_expr }} AS {{ data_type }}))
                     ELSE  ABS(CAST({{ step_expr }} AS {{ data_type }}))
                 END)
            )
        ) t AS seq_val

    {% elif data_type in ["date","timestamp"] %}
        SELECT
            r.*,
            seq_val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                interval
                (CASE
                     WHEN (CAST({{ start_expr }} AS {{ data_type }}) >
                           CAST({{ end_expr   }} AS {{ data_type }}))
                          THEN -ABS(CAST({{ step_expr }} AS INT))
                     ELSE  ABS(CAST({{ step_expr }} AS INT))
                 END) {{ interval_unit }}
            )
        ) t AS seq_val

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   3️⃣  >1 RELATIONS  -----------------------------------------------------------
----------------------------------------------------------------------------#}
{% else %}
    SELECT NULL AS {{ new_field_name }} WHERE FALSE
    /* Error: GenerateRows expects 0 or 1 relation,
              received {{ relations | length }} ({{ relations | join(', ') }}) */
{% endif %}

)   {# ------------- CLOSE PAREN --------------------------------------------- #}
{% endmacro %}