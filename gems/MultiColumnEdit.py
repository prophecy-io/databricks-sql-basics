from dataclasses import dataclass
import dataclasses
import json

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


class MultiColumnEdit(MacroSpec):
    name: str = "MultiColumnEdit"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Prepare"


    @dataclass(frozen=True)
    class MultiColumnEditProperties(MacroProperties):
        # properties for the component with default values
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
        relation_name: List[str] = field(default_factory=list)

    def get_relation_names(self, component: Component, context: SqlContext):
        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)

        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.slug is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.slug)

        return relation_name

    def dialog(self) -> Dialog:
        typeNames = ["STRING", "BINARY", "BOOLEAN", "NUMBER", "FLOAT", "DATE", "TIMESTAMP"]
        dataTypeSelectBox = SelectBox("Data Type of the columns to do operations on").addOption("String Type", "String").addOption("Numeric Type", "Numeric").addOption("Date/Timestamp Type", "Date").addOption("All Types", "All").bindProperty("dataType")
        prefixSuffixDropDown = SelectBox("Add Prefix / Suffix").addOption("Prefix", "Prefix").addOption("Suffix", "Suffix").bindProperty("prefixSuffixOption")
        sparkDataTypeList = SelectBox("Cast output column as")
        for typeName in typeNames:
            sparkDataTypeList = sparkDataTypeList.addOption(typeName, typeName)
        sparkDataTypeList = sparkDataTypeList.bindProperty("castOutputTypeName")


        dialog = Dialog("MultiColumnEdit").addElement(ColumnsLayout(gap="1rem", height="100%") \
        .addColumn(Ports(allowInputAddOrDelete=True), "content") \
        .addColumn(StackLayout(height="100%").addElement(dataTypeSelectBox) \
        .addElement(SchemaColumnsDropdown("Selected Columns").withMultipleSelection().bindSchema("component.ports.inputs[0].schema").bindProperty("columnNames")) \
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
            "String": {"VARCHAR", "CHAR", "STRING", "TEXT"},
            "Numeric": {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "FLOAT", "DOUBLE", "REAL"},
            "Date": {"DATE", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"},
            "All": {"VARCHAR", "CHAR", "STRING", "TEXT", "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "FLOAT", "DOUBLE", "REAL", "DATE", "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ"}
        }

        # Handle changes in the component's state and return the new state
        if newState.properties.dataType in dataTypeMapping:
            allowedSet = set(dataTypeMapping[newState.properties.dataType])
        else:
            allowedSet = set()

        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"] if field["dataType"]["type"].upper() in allowedSet]
        struct_fields = [StructField(field["name"], StringType(), True) for field in fields_array]
        prefix = newState.properties.prefixSuffixOption == "Prefix"
        relation_name = self.get_relation_names(newState, context)
        newProperties = dataclasses.replace(
            newState.properties,
            schemaColDropdownSchema = StructType(struct_fields),
            isPrefix=prefix,
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: MultiColumnEditProperties) -> str:
        # Get the table name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        # isPrefix=true, castOutputTypeName='', copyOriginalColumns=false, remainingColumns=[]
        arguments = [
            "'" + table_name + "'",
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
        return MultiColumnEdit.MultiColumnEditProperties(
            relation_name=parametersMap.get('relation_name'),
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
                MacroParameter("relation_name", properties.relation_name),
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

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component,context)
        newProperties = dataclasses.replace(
            component.properties,
            relation_name = relation_name
        )
        return component.bindProperties(newProperties)
