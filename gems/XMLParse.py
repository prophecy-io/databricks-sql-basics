
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from prophecy.cb.server.base.ComponentBuilderBase import ComponentCode, Diagnostic, SeverityLevelEnum, SubstituteDisabled
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from prophecy.cb.server.base.datatypes import SString, SFloat
from prophecy.cb.ui.uispec import *
from prophecy.cb.util.NumberUtils import parseFloat
from prophecy.cb.server.base import WorkflowContext
import dataclasses
from prophecy.cb.migration import PropertyMigrationObj
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, DoubleType, BooleanType
import re


class ColumnParser(MacroSpec):
    name: str = "XMLParse"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"
    import logging
    logger = logging.getLogger("aa")

    def parse_ddl_to_struct(self, ddl: str) -> StructType:
        """
        Parses a complex DDL string into a PySpark StructType schema.
        Supports nested STRUCT and ARRAY types.
        """
        self.logger.info("parse_ddl_to_struct  " + ddl)
        DDL_TYPE_MAPPING = {
            "STRING": StringType(),
            "INT": IntegerType(),
            "INTEGER": IntegerType(),
            "DOUBLE": DoubleType(),
            "BOOLEAN": BooleanType()
        }

        def parse_field(field_def: str):
            """Recursively parses a single column definition."""
            field_def = field_def.strip()
            
            # Match STRUCT<...> type
            struct_match = re.match(r"(\w+)\s*:\s*STRUCT<(.+)>", field_def, re.IGNORECASE)
            if struct_match:
                col_name = struct_match.group(1)
                col_schema = self.parse_ddl_to_struct(struct_match.group(2))  # Recursively parse STRUCT
                return StructField(col_name, col_schema, True)
            
            # Match ARRAY<...> type
            array_match = re.match(r"(\w+)\s*:\s*ARRAY<(.+)>", field_def, re.IGNORECASE)
            if array_match:
                col_name = array_match.group(1)
                element_schema = parse_field(f"dummy:{array_match.group(2)}").dataType  # Get element type
                return StructField(col_name, ArrayType(element_schema), True)
            
            # Match basic types
            col_parts = field_def.split(":")
            if len(col_parts) < 2:
                raise ValueError(f"Invalid column definition: {field_def}")
            
            col_name, col_type = col_parts[0].strip(), col_parts[1].strip().upper()
            spark_type = DDL_TYPE_MAPPING.get(col_type)
            
            if spark_type is None:
                raise ValueError(f"Unsupported type: {col_type}")
            
            return StructField(col_name, spark_type, True)

        # Extract fields correctly even with nested structures
        fields = []
        depth = 0
        current_field = []
        
        for char in ddl:
            if char == "<":
                depth += 1
            elif char == ">":
                depth -= 1
            
            if char == "," and depth == 0:
                fields.append("".join(current_field).strip())
                current_field = []
            else:
                current_field.append(char)
        
        if current_field:
            fields.append("".join(current_field).strip())

        return StructType([parse_field(field) for field in fields])


    @dataclass(frozen=True)
    class ColumnParserProperties(MacroProperties):
        # properties for the component with default values
        relation: str = "in0"
        columnToParse: str = ""
        schema: str = ""
        tableSchema: Optional[StructType] = StructType([StructField("teacher", StringType(), True),StructField("student", ArrayType(StructType([StructField("name", StringType(), True),StructField("rank", IntegerType(), True)])), True)])

    def dialog(self) -> Dialog:
        print("aa")
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        # schemaTable = SchemaTable("").bindProperty("tableSchema")  # Uncommented this line
        columnSelector = SchemaColumnsDropdown("Source Column Name").withSearchEnabled().bindSchema("component.ports.inputs[0].schema").bindProperty("columnToParse").showErrorsFor("columnToParse")

        return Dialog("ColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowInputAddOrDelete=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(relationTextBox)
                .addElement(ColumnsLayout("1rem").addColumn(columnSelector,"0.4fr")),
                "1fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        self.logger.info("validate")
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        print("HELLO!")
        return newState
        # schema = newState.properties.schema
        # return newState.bindProperties(dataclasses.replace(newState.properties, tableSchema=schema))

    def apply(self, props: ColumnParserProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        schemaString = ""
        if props.tableSchema is not None:
            schemaString = props.tableSchema.simpleString()
        arguments = [
            "'" + props.relation + "'",
            "'" + props.columnToParse + "'",
            "'" + props.schema + "'",
            "'" + schemaString + "'"
            ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        structSchema = self.parse_ddl_to_struct(parametersMap.get('tableSchema')[1:-1][7:-1])
        return ColumnParser.ColumnParserProperties(
            relation=parametersMap.get('relation')[1:-1],
            columnToParse=parametersMap.get('columnToParse')[1:-1],
            schema=parametersMap.get('schema')[1:-1],
            tableSchema=structSchema
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        schemaString = ""
        if properties.tableSchema is not None:
            schemaString = properties.tableSchema.simpleString()
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("columnToParse", properties.columnToParse),
                MacroParameter("schema", properties.schema),
                MacroParameter("tableSchema", schemaString)
            ],
        )
