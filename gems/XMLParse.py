
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


class ColumnParser(MacroSpec):
    name: str = "XMLParse"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class ColumnParserProperties(MacroProperties):
        # properties for the component with default values
        relation: str = "in0"
        columnToParse: str = ""
        schema: str = ""
        tableSchema: Optional[StructType] = None

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        schemaTable = SchemaTable("").bindProperty("tableSchema")
        columnSelector = SchemaColumnsDropdown("Source Column Name").withSearchEnabled().bindSchema("component.ports.inputs[0].schema").bindProperty("columnToParse").showErrorsFor("columnToParse")

        return Dialog("ColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowInputAddOrDelete=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(relationTextBox)
                .addElement(ColumnsLayout("1rem").addColumn(columnSelector,"0.4fr"))
                .addElement(schemaTable),
                "1fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        return newState

    def apply(self, props: ColumnParserProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            "'" + props.columnToParse + "'",
            "'" + props.schema + "'"
            ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return ColumnParser.ColumnParserProperties(
            relation=parametersMap.get('relation')[1:-1],
            columnToParse=parametersMap.get('columnToParse')[1:-1],
            schema=parametersMap.get('schema')[1:-1]
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("columnToParse", properties.columnToParse),
                MacroParameter("schema", properties.schema)
            ],
        )


