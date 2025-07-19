{% macro DataEncoderDecoder(
    relation_name,
    columnNames,
    remaining_columns,
    enc_dec_method,
    enc_dec_charSet,
    aes_enc_dec_secretScope_key,
    aes_enc_dec_secretKey_key,
    aes_enc_dec_mode,
    aes_enc_dec_secretScope_aad,
    aes_enc_dec_secretKey_aad,
    aes_enc_dec_secretScope_iv,
    aes_enc_dec_secretKey_iv,
    prefix_suffix_opt,
    change_col_name,
    prefix_suffix_val
) %}
    {{ log("Applying encoding-specific column operations", info=True) }}
    {%- set withColumn_clause = [] -%}
    {%- if enc_dec_method == "aes_encrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                column,
                "secret('" ~ aes_enc_dec_secretScope_key ~ "','" ~ aes_enc_dec_secretKey_key ~ "')",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_enc_dec_secretScope_iv != "" and aes_enc_dec_secretKey_iv != "" -%}
                {%- do args.append("secret('" ~ aes_enc_dec_secretScope_iv ~ "','" ~ aes_enc_dec_secretKey_iv ~ "')") -%}
            {%- else -%}
                {%- do args.append('""') -%}
            {%- endif -%}

            {%- if aes_enc_dec_secretScope_aad != "" and aes_enc_dec_secretKey_aad != "" -%}
                {%- do args.append("secret('" ~ aes_enc_dec_secretScope_aad ~ "','" ~ aes_enc_dec_secretKey_aad ~ "')") -%}
            {%- endif -%}

            {%- set arg_string = args | join(', ') -%}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("base64(aes_encrypt(" ~ arg_string ~ ")) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("base64(aes_encrypt(" ~ arg_string ~ ")) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("base64(aes_encrypt(" ~ arg_string ~ ")) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "aes_decrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                "unbase64(" ~ column ~ ")",
                "secret('" ~ aes_enc_dec_secretScope_key ~ "','" ~ aes_enc_dec_secretKey_key ~ "')",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_enc_dec_secretScope_aad != "" and aes_enc_dec_secretKey_aad != "" -%}
                {%- do args.append("secret('" ~ aes_enc_dec_secretScope_aad ~ "','" ~ aes_enc_dec_secretKey_aad ~ "')") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("CAST(" ~ "aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "try_aes_decrypt" -%}
        {% for column in columnNames %}
            {%- set args = [
                "unbase64(" ~ column ~ ")",
                "secret('" ~ aes_enc_dec_secretScope_key ~ "','" ~ aes_enc_dec_secretKey_key ~ "')",
                "'" ~ aes_enc_dec_mode ~ "'",
                "'DEFAULT'"
            ] -%}
            {%- if aes_enc_dec_secretScope_aad != "" and aes_enc_dec_secretKey_aad != "" -%}
                {%- do args.append("secret('" ~ aes_enc_dec_secretScope_aad ~ "','" ~ aes_enc_dec_secretKey_aad ~ "')") -%}
            {%- endif -%}
            {%- set arg_string = args | join(', ') -%}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("CAST(" ~ "try_aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "try_aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "try_aes_decrypt(" ~ arg_string ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "base64" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("base64(" ~ column ~ ") AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("base64(" ~ column ~ ") AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("base64(" ~ column ~ ") AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "unbase64" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("CAST(" ~ "unbase64(" ~ column ~ ") AS STRING) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("CAST(" ~ "unbase64(" ~ column ~ ") AS STRING) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("CAST(" ~ "unbase64(" ~ column ~ ") AS STRING) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "hex" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("hex(" ~ column ~ ") AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("hex(" ~ column ~ ") AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("hex(" ~ column ~ ") AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "unhex" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("decode(" ~ "unhex(" ~ column ~ "), 'UTF-8') AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("decode(" ~ "unhex(" ~ column ~ "), 'UTF-8') AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("decode(" ~ "unhex(" ~ column ~ "), 'UTF-8') AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "encode" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("hex(encode(" ~ column ~ ", '" ~ enc_dec_charSet ~ "')) AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("hex(encode(" ~ column ~ ", '" ~ enc_dec_charSet ~ "')) AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("hex(encode(" ~ column ~ ", '" ~ enc_dec_charSet ~ "')) AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- if enc_dec_method == "decode" -%}
        {% for column in columnNames %}
            {%- if change_col_name == "False" -%}
                {%- do withColumn_clause.append("decode(unhex(" ~ column ~ "), '" ~ enc_dec_charSet ~ "') AS " ~ column) -%}
            {%- elif prefix_suffix_opt == "Prefix" -%}
                {%- do withColumn_clause.append("decode(unhex(" ~ column ~ "), '" ~ enc_dec_charSet ~ "') AS " ~ prefix_suffix_val ~ column) -%}
            {%- else -%}
                {%- do withColumn_clause.append("decode(unhex(" ~ column ~ "), '" ~ enc_dec_charSet ~ "') AS " ~ column ~ prefix_suffix_val) -%}
            {%- endif -%}
        {% endfor %}
    {%- endif -%}
    {%- set select_clause_sql = withColumn_clause | join(', ') -%}
    {%- set select_cte_sql -%}
        {%- if select_clause_sql == "" -%}
            WITH final_cte AS (
                SELECT *
                FROM {{ relation_name }}
            )
        {%- elif change_col_name == "True" -%}
            WITH final_cte AS (
                SELECT *, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- elif remaining_columns == "" -%}
            WITH final_cte AS (
                SELECT {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- else -%}
            WITH final_cte AS (
                SELECT {{ remaining_columns }}, {{ select_clause_sql }}
                FROM {{ relation_name }}
            )
        {%- endif -%}
    {%- endset -%}
    {%- set final_select_query = select_cte_sql ~ "\nSELECT * FROM final_cte" -%}
    {{ log("final select query is -> ", info=True) }}
    {{ log(final_select_query, info=True) }}
    {{ return(final_select_query) }}
{%- endmacro %}