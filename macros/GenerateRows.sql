{#
   generate_sequence_generic(
       relation_names   = None | "" | "table" | ['schema.table']   -- optional
     , new_field_name   = 'seq_val'                               -- output col
     , start_expr       =                                          -- required
     , end_expr         =                                          -- required
     , step_expr        = "1"                                      -- default
     , data_type        = "int"                                    -- numeric|date|timestamp
     , interval_unit    = "day"                                    -- for date/timestamp
   )
#}
{% macro generate_sequence_generic(
        relation_names   = None,
        new_field_name   = 'seq_val',
        start_expr       = None,
        end_expr         = None,
        step_expr        = "1",
        data_type        = "int",
        interval_unit    = "day"
    ) %}
{%- set numeric_types = ["int","integer","bigint","float","double","decimal"] %}

{#----------------------------------------------------------------------------
   Normalise relation_names -> relations (list with real, non-blank names)
----------------------------------------------------------------------------#}
{% set relations = [] %}
{% if relation_names is none %}
    {# nothing supplied #}

{% elif relation_names is string %}
    {% if relation_names | trim != '' %}
        {% set relations = [ relation_names | trim ] %}
    {% endif %}

{% else %}
    {# iterable; filter out blanks #}
    {% for rel in relation_names | list %}
        {% if rel | trim != '' %}
            {% do relations.append(rel | trim) %}
        {% endif %}
    {% endfor %}
{% endif %}

(   {#-- OPEN PAREN so caller can SELECT * FROM ( … ) t  if desired --#}

{#----------------------------------------------------------------------------
   1️⃣  NO INPUT TABLE  --------------------------------------------------------
----------------------------------------------------------------------------#}
{% if relations | length == 0 %}

    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     CAST({{ step_expr  }} AS {{ data_type }})
                 )
               ) AS {{ new_field_name }}

    {% elif data_type in ["date","timestamp"] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval CAST({{ step_expr }} AS INT) {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   2️⃣  EXACTLY ONE INPUT TABLE  ----------------------------------------------
----------------------------------------------------------------------------#}
{% elif relations | length == 1 %}
    {%- set rel = relations[0] %}

    {% if data_type in numeric_types %}
        SELECT
            r.*,
            seq_val AS {{ new_field_name }}
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
            seq_val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        LATERAL VIEW explode(
            sequence(
                CAST({{ start_expr }} AS {{ data_type }}),
                CAST({{ end_expr   }} AS {{ data_type }}),
                interval CAST({{ step_expr }} AS INT) {{ interval_unit }}
            )
        ) t AS seq_val

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{#----------------------------------------------------------------------------
   3️⃣  MORE THAN ONE RELATION  -----------------------------------------------
----------------------------------------------------------------------------#}
{% else %}
    SELECT NULL AS {{ new_field_name }} WHERE FALSE
    /* Error: generate_sequence_generic expects 0 or 1 relation,
              received {{ relations | length }} */
{% endif %}
)   {#-- CLOSE PAREN --#}
{% endmacro %}