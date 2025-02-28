WITH xyz AS (

  SELECT * 
  
  FROM xyz

),

bulk_column_expressions AS (

  {#Processes and transforms specific columns for enhanced data analysis.#}
  {{
    DatabricksSqlBasics.BulkColumnExpressions(
      'xyz', 
      ['name', 'age'], 
      'upper(column_value)', 
      'VA', 
      'Select output type', 
      true, 
      ['email'], 
      'Suffix'
    )
  }}

)

SELECT *

FROM bulk_column_expressions
