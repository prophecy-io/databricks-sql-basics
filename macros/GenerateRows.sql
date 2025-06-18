{# =============================================================================
    GenerateRows
    =============================================================================
    Args (all strings, supplied in this order)
    ---------------------------------------------------------------------------
      1  relation_names   :  '' | 'schema.table' | 'tbl1,tbl2' | list | None
                            → '', None, or whitespace ⇒ no table
                            → exactly 1 ⇒ cross-join per row
                            → >1        ⇒ explicit error
      2  new_field_name   :  output column name
      3  start_expr       :  literal or column/expression
      4  end_expr         :  literal or column/expression
      5  step_expr        :  literal or column/expression  (default "1")
      6  data_type        :  int / bigint / double / decimal / date / timestamp
      7  interval_unit    :  Spark interval unit (default "day") – for temporal
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
   STEP 0  – Normalise relation_names → relations (clean, non-blank list)
----------------------------------------------------------------------------#}
{% set _tmp = [] %}
{% if relation_names is none %}
    {# nothing supplied #}
{% elif relation_names is string %}
    {% for p in relation_names.split(',') %}
        {% if p | trim != '' %}
            {% do _tmp.append(p | trim) %}
        {% endif %}
    {% endfor %}
{% else %}
    {% for itm in relation_names | list %}
        {% if itm is string %}
            {% for p in itm.split(',') %}
                {% if p | trim != '' %}
                    {% do _tmp.append(p | trim) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}
{% set relations = _tmp %}

(   {# ------------- OPEN PAREN so callers can SELECT * FROM (…) t ------------- #}

{#----------------------------------------------------------------------------
   BRANCH A : 0 table  → stand-alone generator
----------------------------------------------------------------------------#}
{% if relations | length == 0 %}

    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     -- flip sign automatically for descending ranges
                     CASE
                         WHEN CAST({{ step_expr }} AS {{ data_type }}) = 0
                              THEN 1
                         WHEN CAST({{ start_expr }} AS {{ data_type }})
                              >   CAST({{ end_expr }}   AS {{ data_type }})
                              THEN -ABS(CAST({{ step_expr }} AS {{ data_type }}))
                         ELSE  ABS(CAST({{ step_expr }} AS {{ data_type }}))
                     END
                 )
               ) AS {{ new_field_name }}

    {% elif data_type in ["date","timestamp"] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval
                     CASE
                         WHEN CAST({{ start_expr }} AS {{ data_type }})
                              >   CAST({{ end_expr }}   AS {{ data_type }})
                              THEN -ABS(CAST({{ step_expr }} AS INT))
                         ELSE  ABS(CAST({{ step_expr }} AS INT))
                     END {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   BRANCH B : 1 table  → keep r.* and add generated column
----------------------------------------------------------------------------#}
{% elif relations | length == 1 %}
    {%- set rel = relations[0] -%}

    {% if data_type in numeric_types %}
        SELECT
            r.*,
            s.seq_val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        CROSS JOIN LATERAL (
            SELECT explode(
                     sequence(
                         CAST({{ start_expr }} AS {{ data_type }}),
                         CAST({{ end_expr   }} AS {{ data_type }}),
                         CASE
                             WHEN CAST({{ step_expr }} AS {{ data_type }}) = 0
                                  THEN 1
                             WHEN CAST({{ start_expr }} AS {{ data_type }})
                                  >   CAST({{ end_expr }}   AS {{ data_type }})
                                  THEN -ABS(CAST({{ step_expr }} AS {{ data_type }}))
                             ELSE  ABS(CAST({{ step_expr }} AS {{ data_type }}))
                         END
                     )
                   ) AS seq_val
        ) s

    {% elif data_type in ["date","timestamp"] %}
        SELECT
            r.*,
            s.seq_val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        CROSS JOIN LATERAL (
            SELECT explode(
                     sequence(
                         CAST({{ start_expr }} AS {{ data_type }}),
                         CAST({{ end_expr   }} AS {{ data_type }}),
                         interval
                         CASE
                             WHEN CAST({{ start_expr }} AS {{ data_type }})
                                  >   CAST({{ end_expr }}   AS {{ data_type }})
                                  THEN -ABS(CAST({{ step_expr }} AS INT))
                             ELSE  ABS(CAST({{ step_expr }} AS INT))
                         END {{ interval_unit }}
                     )
                   ) AS seq_val
        ) s

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   BRANCH C : >1 tables  → explicit error
----------------------------------------------------------------------------#}
{% else %}
    SELECT NULL AS {{ new_field_name }} WHERE FALSE
    /* Error: GenerateRows expects 0 or 1 relation,
              received {{ relations | length }}  ({{ relations | join(', ') }}) */
{% endif %}

)   {# ---------------------- CLOSE PAREN ------------------------------------ #}
{% endmacro %}