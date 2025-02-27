
from dataclasses import dataclass


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
from pyspark.sql.types import StringType, BinaryType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, TimestampType, DateType, StructField
from pyspark.sql.types import StructType
import dataclasses
import json


class BulkColumnExpressions(MacroSpec):
    name: str = "BulkColumnExpressions"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class BulkColumnExpressionsProperties(MacroProperties):
        # properties for the component with default values
        relation: str = "in0"
        columnNames: List[str] = field(default_factory=list)
        remainingColumns: List[str] = field(default_factory=list)
        schemaColDropdownSchema: Optional[StructType] = StructType([])
        prefixSuffixOption: str = "Prefix"
        prefixSuffixToBeAdded: str = ""
        castOutputTypeName: str = "Select output type"
        copyOriginalColumns: bool = False
        expressionToBeApplied: str = ""
        

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        prefixSuffixDropDown = SelectBox("").addOption("Prefix", "Prefix").addOption("Suffix", "Suffix").bindProperty("prefixSuffixOption")
        copyOriginalColumns = Checkbox("").bindProperty("copyOriginalColumns")
        maintainOriginalColumns = NativeText("Maintain the original columns and add")
        prefixSuffixBox = TextBox("",ignoreTitle=True).bindPlaceholder("Orig_").bindProperty("prefixSuffixToBeAdded")
        toTheNewColumns = NativeText("to the new columns")
        changeOutputColumnNames = ColumnsLayout(gap="1rem").addColumn(copyOriginalColumns, "0.1fr").addColumn(maintainOriginalColumns).addColumn(prefixSuffixDropDown).addColumn(prefixSuffixBox).addColumn(toTheNewColumns)
        dialog = Dialog("BulkColumnExpressions").addElement(ColumnsLayout(gap="1rem", height="100%") \
        .addColumn(Ports(allowInputAddOrDelete=True), "content") \
        .addColumn(StackLayout(height="100%").addElement(relationTextBox) \
        .addElement(SchemaColumnsDropdown("Selected Columns").withMultipleSelection().bindSchema("schemaColDropdownSchema").bindProperty("columnNames")) \
        .addElement(changeOutputColumnNames) \
        .addElement(ExpressionBox("Output Expression").bindProperty("expressionToBeApplied").bindPlaceholder("Write spark sql expression considering `column_value` as column value and `column_name` as column name string literal. Example:\nFor column value: column_value * 100\nFor column name: upper(column_name)").bindLanguage("plaintext"))))
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        struct_fields = [StructField(field["name"], StringType(), True) for field in fields_array]
        remainingColumns = []
        for field in fields_array:
            if field["name"] not in newState.properties.columnNames:
                remainingColumns.append(field["name"])
        newProperties = dataclasses.replace(
            newState.properties, 
            schemaColDropdownSchema = StructType(struct_fields),
            remainingColumns = remainingColumns
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: BulkColumnExpressionsProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            str(props.columnNames),
            "'" + props.expressionToBeApplied + "'",
            "'" + props.prefixSuffixToBeAdded + "'",
            "'" + props.castOutputTypeName + "'",
            str(props.copyOriginalColumns).lower(),
            str(props.remainingColumns),
            "'" + props.prefixSuffixOption + "'",
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return BulkColumnExpressions.BulkColumnExpressionsProperties(
            relation=parametersMap.get('relation')[1:-1],
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            expressionToBeApplied=parametersMap.get('expressionToBeApplied')[1:-1],
            prefixSuffixToBeAdded=parametersMap.get('prefixSuffixToBeAdded')[1:-1],
            castOutputTypeName=parametersMap.get('castOutputTypeName')[1:-1],
            copyOriginalColumns=parametersMap.get('copyOriginalColumns').lower() == "true",
            remainingColumns=json.loads(parametersMap.get('remainingColumns').replace("'", '"')),
            prefixSuffixOption=parametersMap.get('prefixSuffixOption')[1:-1],
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert the component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("expressionToBeApplied", properties.expressionToBeApplied),
                MacroParameter("prefixSuffixToBeAdded", properties.prefixSuffixToBeAdded),
                MacroParameter("castOutputTypeName", properties.castOutputTypeName),
                MacroParameter("copyOriginalColumns", str(properties.copyOriginalColumns).lower()),
                MacroParameter("remainingColumns", json.dumps(properties.remainingColumns)),
                MacroParameter("prefixSuffixOption", properties.prefixSuffixOption),
            ],
        )


