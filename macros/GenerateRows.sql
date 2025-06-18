{% macro GenerateRows(
        relation_name  = '',
        new_field_name = 'value',
        start_expr     = None,
        end_expr       = None,
        step_expr      = '1',
        data_type      = 'int',
        interval_unit  = 'day'
    ) %}
{%- set numeric_types = ['int','integer','bigint','float','double','decimal'] -%}
{%- set rel = relation_name | trim %}
{%- set is_table = rel != '' %}

{# ---------------- helper snippets ---------------------------------------- #}
{%- macro _step_numeric() %}
    CASE
        WHEN CAST({{ step_expr }} AS {{ data_type }}) = 0
             THEN 1
        WHEN CAST({{ start_expr }} AS {{ data_type }})
             >   CAST({{ end_expr }}   AS {{ data_type }})
             THEN -ABS(CAST({{ step_expr }} AS {{ data_type }}))
        ELSE  ABS(CAST({{ step_expr }} AS {{ data_type }}))
    END
{%- endmacro %}

{%- macro _step_temporal() %}
    CASE
        WHEN CAST({{ start_expr }} AS {{ data_type }})
             >   CAST({{ end_expr }}   AS {{ data_type }})
             THEN -ABS(CAST({{ step_expr }} AS INT))
        ELSE  ABS(CAST({{ step_expr }} AS INT))
    END
{%- endmacro %}

(   -- open bracket so caller can SELECT * FROM ( … ) t
{% if not is_table %}

    {# ─────────────────── stand-alone generator ─────────────────────────── #}
    {% if data_type in numeric_types %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     {{ _step_numeric() }}
                 )
               ) AS {{ new_field_name }}

    {% elif data_type in ['date','timestamp'] %}
        SELECT explode(
                 sequence(
                     CAST({{ start_expr }} AS {{ data_type }}),
                     CAST({{ end_expr   }} AS {{ data_type }}),
                     interval {{ _step_temporal() }} {{ interval_unit }}
                 )
               ) AS {{ new_field_name }}

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{% else %}

    {# ─────────────────── join to the input table ───────────────────────── #}
    {% if data_type in numeric_types %}
        SELECT
            r.* ,
            gen.val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        CROSS JOIN (
            SELECT explode(
                     sequence(
                         CAST({{ start_expr }} AS {{ data_type }}),
                         CAST({{ end_expr   }} AS {{ data_type }}),
                         {{ _step_numeric() }}
                     )
                 ) AS val
        ) gen

    {% elif data_type in ['date','timestamp'] %}
        SELECT
            r.* ,
            gen.val AS {{ new_field_name }}
        FROM {{ rel }} AS r
        CROSS JOIN (
            SELECT explode(
                     sequence(
                         CAST({{ start_expr }} AS {{ data_type }}),
                         CAST({{ end_expr   }} AS {{ data_type }}),
                         interval {{ _step_temporal() }} {{ interval_unit }}
                     )
                 ) AS val
        ) gen

    {% else %}
        SELECT NULL AS {{ new_field_name }} WHERE FALSE
    {% endif %}

{% endif %}
)   -- close bracket
{% endmacro %}