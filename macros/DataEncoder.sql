{% macro DataEncoder(
    relation_name,
    columnNames,
    enc_dec_method,
    enc_dec_charSet,
    aes_enc_dec_key,
    aes_enc_dec_mode,
    aes_encrypt_aad,
    aes_encrypt_iv
) %}

    {{ log("Applying encoding-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}

    {%- if enc_dec_method == "aes_encrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                column,
                "'" ~ aes_enc_dec_key ~ "'",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_encrypt_aad != "" -%}
                {%- do args.append("'" ~ aes_encrypt_aad ~ "'") -%}
            {%- endif -%}
            {%- if aes_encrypt_iv != "" -%}
                {%- do args.append("'" ~ aes_encrypt_iv ~ "'") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- do withColumn_clause.append("base64(aes_encrypt(" ~ arg_string ~ ")) AS " ~ column ~ "_aes_encrypt") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "aes_decrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                "unbase64(" ~ column ~ ")",
                "'" ~ aes_enc_dec_key ~ "'",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_encrypt_aad != "" -%}
                {%- do args.append("'" ~ aes_encrypt_aad ~ "'") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- do withColumn_clause.append("CAST(" ~ "aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column ~ "_aes_decrypt") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "try_aes_decrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                "unbase64(" ~ column ~ ")",
                "'" ~ aes_enc_dec_key ~ "'",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_encrypt_aad != "" -%}
                {%- do args.append("'" ~ aes_encrypt_aad ~ "'") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- do withColumn_clause.append("CAST(" ~ "try_aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column ~ "_try_aes_decrypt") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "base64" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("base64(" ~ column ~ ") AS " ~ column ~ "_base64") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "unbase64" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("CAST(" ~ "unbase64(" ~ column ~ ") AS STRING) AS " ~ column ~ "_unbase64") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "hex" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("hex(" ~ column ~ ") AS " ~ column ~ "_hex") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "unhex" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("decode(" ~ "unhex(" ~ column ~ "), 'UTF-8') AS " ~ column ~ "_unhex") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "encode" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("hex(encode(" ~ column ~ ", '" ~ enc_dec_charSet ~ "')) AS " ~ column ~ "_encode") -%}
        {% endfor %}
    {%- endif -%}

    {%- if enc_dec_method == "decode" -%}
        {% for column in columnNames %}
            {%- do withColumn_clause.append("decode(unhex(" ~ column ~ "), '" ~ enc_dec_charSet ~ "') AS " ~ column ~ "_decode") -%}
        {% endfor %}
    {%- endif -%}

    {%- set select_clause_sql = withColumn_clause | join(', ') -%}


    {%- set select_cte_sql -%}
        {%- if select_clause_sql == "" -%}
            WITH final_cte AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- else -%}
            WITH final_cte AS (
                SELECT *, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- endif -%}
    {%- endset -%}

    {%- set final_select_query = select_cte_sql ~ "\nSELECT * FROM final_cte" -%}
    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}

    {{ return(final_select_query) }}

{%- endmacro %}
