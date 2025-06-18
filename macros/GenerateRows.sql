{# =============================================================================
    GenerateRows
    =============================================================================
    Parameters (all strings)
    -----------------------------------------------------------------------------
    1  relation_names : string | list | None
         • '', '   ', or None  →  no input table
         • 'schema.table'      →  single input table
         • 'tbl1,tbl2'         →  comma-separated (⇢ error if >1 after cleaning)

    2  new_field_name : string          -- output column name

    3  start_expr     : string          -- literal or column/expression
    4  end_expr       : string          -- literal or column/expression
    5  step_expr      : string (default "1")

    6  data_type      : string (default "int")
         numeric types: int | integer | bigint | float | double | decimal
         temporal    : date | timestamp

    7  interval_unit  : string (default "day")
         valid Spark interval units when data_type is date/timestamp

    ---------------------------------------------------------------------------
    Example (stand-alone ints 11,14,17,20,23)
    -----------------------------------------
    {{ GenerateRows(
         relation_names = '',           -- no table
         new_field_name = 'num',
         start_expr     = '11',
         end_expr       = '25',
         step_expr      = '2',
         data_type      = 'int'
    ) }}
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
{%- set numeric_types = ["int","integer","bigint","float","double","decimal"] %}

{#----------------------------------------------------------------------------
   NORMALISE relation_names  →  list `relations`
   • Accepts None, string, or list/tuple
   • Splits comma-separated strings
   • Removes blanks & whitespace
----------------------------------------------------------------------------#}
{% set temp_list = [] %}
{% if relation_names is none %}
    {# leave empty #}

{% elif relation_names is string %}
    {% set temp_list = relation_names.split(',') %}

{% else %}
    {# iterable: flatten & split comma-separated parts #}
    {% for item in relation_names | list %}
        {% if item is string %}
            {% for part in item.split(',') %}
                {% do temp_list.append(part) %}
            {% endfor %}
        {% else %}
            {% do temp_list.append(item) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% set relations = [] %}
{% for rel in temp_list %}
    {% if rel | trim != '' %}
        {% do relations.append(rel | trim) %}
    {% endif %}
{% endfor %}

(   {# ----------------------------- open parenthesis ------------------------ #}

{#----------------------------------------------------------------------------
   BRANCH 1 : NO TABLE  --------------------------------------------------------
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
   BRANCH 2 : ONE TABLE  -------------------------------------------------------
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
   BRANCH 3 : >1 TABLES  -------------------------------------------------------
----------------------------------------------------------------------------#}
{% else %}
    SELECT NULL AS {{ new_field_name }} WHERE FALSE
    /* Error: GenerateRows expects 0 or 1 relation,
              received {{ relations | length }} ({{ relations | join(', ') }}) */
{% endif %}
)   {# ----------------------------- close parenthesis ----------------------- #}
{% endmacro %}