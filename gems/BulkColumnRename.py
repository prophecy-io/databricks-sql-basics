
from dataclasses import dataclass
import dataclasses

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *

from prophecy.cb.server.base import WorkflowContext
from prophecy.cb.server.base.datatypes import SInt, SString
from prophecy.cb.ui.uispec import *
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


class BulkColumnRename(MacroSpec):
    name: str = "BulkColumnRename"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class BulkColumnRenameProperties(MacroProperties):
        # properties for the component with default values
        schema: Optional[StructType] = StructType([])
        columnNames: List[str] = field(default_factory=list)
        renameMethod: str = ""
        prefix: str = ""
        suffix: str = ""
        customExpression: str = ""
        relation: str = ""

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        renameMethod = SelectBox("Select a method to rename columns").addOption("Add Prefix", "Add Prefix").addOption("Add Suffix", "Add Suffix").addOption("Custom Expression", "Custom Expression").bindProperty("renameMethod")
        dialog = Dialog("BulkColumnRename").addElement(ColumnsLayout(gap="1rem", height="100%").addColumn(Ports(allowInputAddOrDelete=True),"content").addColumn(StackLayout(height="100%").addElement(relationTextBox).addElement(renameMethod).addElement(SchemaColumnsDropdown("Select columns to rename").withMultipleSelection().bindSchema("schema").bindProperty("columnNames")).addElement(Condition().ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Add Prefix")).then(StackLayout().addElement(TextBox("Enter Prefix").bindPlaceholder("""Example: new_""").bindProperty("prefix")))).addElement(Condition().ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Add Suffix")).then(StackLayout().addElement(TextBox("Enter Suffix").bindPlaceholder("""Example: _new""").bindProperty("suffix")))).addElement(Condition().ifEqual(PropExpr("component.properties.renameMethod"), StringExpr("Custom Expression")).then(StackLayout().addElement(TextBox("Enter Custom Expression").bindPlaceholder("""Example: concat(column_name, 'xyz')""").bindProperty("customExpression"))))))
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        portSchema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in portSchema["fields"]]
        struct_fields = [StructField(field["name"], StructType(), True) for field in fields_array]

        newProperties = dataclasses.replace(
            newState.properties, 
            schema = StructType(struct_fields)
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: BulkColumnRenameProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            str(props.columnNames),
            "'" + str(props.renameMethod) + "'",
            "'" + str(props.prefix) + "'",
            "'" + str(props.suffix) + "'",
            '"' + str(props.customExpression) + '"'
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return BulkColumnRename.BulkColumnRenameProperties(
            relation=parametersMap.get('relation')[1:-1],
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            renameMethod=parametersMap.get('renameMethod')[1:-1],
            prefix=parametersMap.get('prefix')[1:-1],
            suffix=parametersMap.get('suffix')[1:-1],
            customExpression=parametersMap.get('customExpression')[1:-1]
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("renameMethod", properties.renameMethod),
                MacroParameter("prefix", properties.prefix),
                MacroParameter("suffix", properties.suffix),
                MacroParameter("customExpression", properties.customExpression)
            ],
        )


