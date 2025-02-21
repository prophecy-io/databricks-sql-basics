WITH c1 AS (

  SELECT * 
  
  FROM {{ ref('c1')}}

),

xml_parsed_data AS (

  {#Improves the usability and analysis of XML data for better insights.#}
  {{ DatabricksSqlBasics.XMLParse('c1', 'c1', '') }}

)

SELECT *

FROM xml_parsed_data
