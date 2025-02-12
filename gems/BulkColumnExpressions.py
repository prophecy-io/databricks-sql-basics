
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
        dataType: str = ""
        prefixSuffixOption: str = "Prefix / Suffix to be added"
        prefixSuffixToBeAdded: str = ""
        castOutputTypeName: str = "Select output type"
        changeOutputFieldName: bool = False
        changeOutputFieldType: bool = False
        copyOriginalColumns: bool = False
        expressionToBeApplied: str = ""
        isPrefix: bool = False
        

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        typeNames = ["String","Binary","Boolean","Byte","Short","Integer","Long","Float","Double", "Date", "Timestamp"]
        dataTypeSelectBox = SelectBox("Data Type of the columns to do operations on").addOption("String Type", "String").addOption("Numeric Type", "Numeric").addOption("Date/Timestamp Type", "Date").addOption("All Types", "All").bindProperty("dataType")
        prefixSuffixDropDown = SelectBox("Add Prefix / Suffix").addOption("Prefix", "Prefix").addOption("Suffix", "Suffix").bindProperty("prefixSuffixOption")
        sparkDataTypeList = SelectBox("Cast output column as")
        for typeName in typeNames:
            sparkDataTypeList = sparkDataTypeList.addOption(typeName, typeName)
        sparkDataTypeList = sparkDataTypeList.bindProperty("castOutputTypeName")


        dialog = Dialog("BulkColumnExpressions").addElement(ColumnsLayout(gap="1rem", height="100%") \
        .addColumn(Ports(allowInputAddOrDelete=True), "content") \
        .addColumn(StackLayout(height="100%").addElement(relationTextBox).addElement(dataTypeSelectBox) \
        .addElement(SchemaColumnsDropdown("Selected Columns").withMultipleSelection().bindSchema("schemaColDropdownSchema").bindProperty("columnNames")) \
        .addElement(Checkbox("Change output column name").bindProperty("changeOutputFieldName")) \
        .addElement(Condition().ifEqual(PropExpr("component.properties.changeOutputFieldName"), BooleanExpr(True)).then(StackLayout(gap="1rem").addElement(prefixSuffixDropDown).addElement(TextBox("Value").bindPlaceholder("Example: new_").bindProperty("prefixSuffixToBeAdded")).addElement(Checkbox("Copy incoming columns to output").bindProperty("copyOriginalColumns")))) \
        .addElement(Checkbox("Change output column type").bindProperty("changeOutputFieldType")) \
        .addElement(Condition().ifEqual(PropExpr("component.properties.changeOutputFieldType"), BooleanExpr(True)).then(ColumnsLayout().addColumn(sparkDataTypeList))) \
        .addElement(ExpressionBox("Output Expression").bindProperty("expressionToBeApplied").bindPlaceholder("Write spark sql expression considering `column_value` as column value and `column_name` as column name string literal. Example:\nFor column value: column_value * 100\nFor column name: upper(column_name)").bindLanguage("plaintext"))))
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        dataTypeMapping = {
            "String": {"String"},
            "Numeric": {"Byte", "Short", "Integer", "Long", "Float", "Double"},
            "Date": {"Date", "Timestamp"},
            "All": {"String", "Byte", "Short", "Integer", "Long", "Float", "Double", "Date", "Timestamp"}
        }
        # Handle changes in the component's state and return the new state
        if newState.properties.dataType in dataTypeMapping:
            allowedSet = set(dataTypeMapping[newState.properties.dataType])
        else:
            allowedSet = set()
        schemaString = str(newState.ports.inputs[0].schema).replace("'", '"')
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"] if field["dataType"]["type"] in allowedSet]
        struct_fields = [StructField(field["name"], StringType(), True) for field in fields_array]
        prefix = newState.properties.prefixSuffixOption == "Prefix"
        newProperties = dataclasses.replace(
            newState.properties, 
            schemaColDropdownSchema = StructType(struct_fields),
            isPrefix=prefix
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: BulkColumnExpressionsProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        # isPrefix=true, castOutputTypeName='', copyOriginalColumns=false, remainingColumns=[]
        arguments = [
            "'" + props.relation + "'",
            str(props.columnNames),
            "'" + props.expressionToBeApplied + "'",
            "'" + props.prefixSuffixToBeAdded + "'",
            str(props.changeOutputFieldName).lower(),
            str(props.isPrefix).lower(),
            str(props.changeOutputFieldType).lower(),
            "'" + props.castOutputTypeName + "'",
            str(props.copyOriginalColumns).lower(),
            str(props.remainingColumns),
            "'" + props.prefixSuffixOption + "'",
            "'" + props.dataType + "'"
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
            changeOutputFieldName=parametersMap.get('changeOutputFieldName').lower() == "true",
            isPrefix=parametersMap.get('isPrefix').lower() == "true",
            changeOutputFieldType=parametersMap.get('changeOutputFieldType').lower() == "true",
            castOutputTypeName=parametersMap.get('castOutputTypeName')[1:-1],
            copyOriginalColumns=parametersMap.get('copyOriginalColumns').lower() == "true",
            remainingColumns=json.loads(parametersMap.get('remainingColumns').replace("'", '"')),
            prefixSuffixOption=parametersMap.get('prefixSuffixOption')[1:-1],
            dataType=parametersMap.get('dataType')[1:-1],
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
                MacroParameter("changeOutputFieldType", properties.changeOutputFieldType),
                MacroParameter("changeOutputFieldName", str(properties.changeOutputFieldName).lower()),
                MacroParameter("isPrefix", str(properties.isPrefix).lower()),
                MacroParameter("castOutputTypeName", properties.castOutputTypeName),
                MacroParameter("copyOriginalColumns", str(properties.copyOriginalColumns).lower()),
                MacroParameter("remainingColumns", json.dumps(properties.remainingColumns)),
                MacroParameter("prefixSuffixOption", properties.prefixSuffixOption),
                MacroParameter("dataType", properties.dataType)
            ],
        )


