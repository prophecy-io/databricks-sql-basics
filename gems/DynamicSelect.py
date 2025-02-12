
from dataclasses import dataclass
import dataclasses

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql import *
import json

class DynamicSelect(MacroSpec):
    name: str = "DynamicSelect"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class DynamicSelectProperties(MacroProperties):
        # properties for the component with default values
        relation: str = 'in0'
        selectUsing: str = "SELECT_FIELD_TYPES"
        # DATA TYPES
        boolTypeChecked: bool = False
        strTypeChecked: bool = False
        intTypeChecked: bool = False
        shortTypeChecked: bool = False
        byteTypeChecked: bool = False
        longTypeChecked: bool = False
        floatTypeChecked: bool = False
        doubleTypeChecked: bool = False
        decimalTypeChecked: bool = False
        binaryTypeChecked: bool = False
        dateTypeChecked: bool = False
        timestampTypeChecked: bool = False
        structTypeChecked: bool = False
        schema: str = ''
        targetTypes: str = ''
        # custom expression
        customExpression: str = ''

    def dialog(self) -> Dialog:
        return Dialog("DynamicSelect").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(VerticalDivider(), width="content")
            .addColumn(
                StackLayout(gap=("1rem"), width="50%", height=("100bh"))
                .addElement(TitleElement("Configuration"))
                .addElement(
                    TextBox("Table name")
                    .bindPlaceholder("in0")
                    .bindProperty("relation")
                )
                .addElement(
                    SelectBox("")
                    .addOption("Select field types", "SELECT_FIELD_TYPES")
                    .addOption("Select via expression", "SELECT_EXPR")
                    .bindProperty("selectUsing")
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.selectUsing"),
                        StringExpr("SELECT_FIELD_TYPES"),
                    )
                    .then(
                        StackLayout(gap=("1rem"), width="50%", height=("100bh"))
                        .addElement(TitleElement("Select Field types"))
                        .addElement(Checkbox("Boolean", "boolTypeChecked"))
                        .addElement(Checkbox("String", "strTypeChecked"))
                        .addElement(Checkbox("Integer", "intTypeChecked"))
                        .addElement(Checkbox("Short", "shortTypeChecked"))
                        .addElement(Checkbox("Byte", "byteTypeChecked"))
                        .addElement(Checkbox("Long", "longTypeChecked"))
                        .addElement(Checkbox("Float", "floatTypeChecked"))
                        .addElement(Checkbox("Double", "doubleTypeChecked"))
                        .addElement(Checkbox("Decimal", "decimalTypeChecked"))
                        .addElement(Checkbox("Binary", "binaryTypeChecked"))
                        .addElement(Checkbox("Date", "dateTypeChecked"))
                        .addElement(Checkbox("Timestamp", "timestampTypeChecked"))
                        .addElement(Checkbox("Struct", "structTypeChecked"))
                    )
                    .otherwise(
                        StackLayout()
                        .addElement(
                            TextBox("Enter Custom SQL Expression")
                            .bindPlaceholder(
                                """contains(column_name, 'user')""")
                            .bindProperty("customExpression")
                        )
                        .addElement(
                            AlertBox(
                                variant="success",
                                _children=[
                                    Markdown(
                                        "We can use following metadata columns in our expressions"
                                        "\n"
                                        "* **column_name** - Name of column, eg. name, country\n"
                                        "* **column_type** - Type of column, eg. string, int\n"
                                        "* **column_size** - Size of column in bytes, eg. int -> 4, boolean -> 1 (-1 "
                                        "for undefined)\n"
                                        "* **field_number** - Index of column in dataframe, eg. 0 for first column\n"
                                    )
                                ]
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        target_types = []
        if newState.properties.boolTypeChecked:
            target_types.append("Boolean")
        if newState.properties.strTypeChecked:
            target_types.append("String")
        if newState.properties.intTypeChecked:
            target_types.append("Integer")
        if newState.properties.shortTypeChecked:
            target_types.append("Short")
        if newState.properties.byteTypeChecked:
            target_types.append("Byte")
        if newState.properties.longTypeChecked:
            target_types.append("Long")
        if newState.properties.floatTypeChecked:
            target_types.append("Float")
        if newState.properties.doubleTypeChecked:
            target_types.append("Double")
        if newState.properties.decimalTypeChecked:
            target_types.append("Decimal")
        if newState.properties.binaryTypeChecked:
            target_types.append("Binary")
        if newState.properties.dateTypeChecked:
            target_types.append("Date")
        if newState.properties.timestampTypeChecked:
            target_types.append("Timestamp")
        if newState.properties.structTypeChecked:
            target_types.append("Struct")
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        newProperties = dataclasses.replace(newState.properties, schema=json.dumps(fields_array), targetTypes=json.dumps(target_types))
        return newState.bindProperties(newProperties)

    def apply(self, props: DynamicSelectProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        relation = "'" + props.relation + "'"
        schema = props.schema
        targetTypes = props.targetTypes
        selectUsing = f"'{props.selectUsing}'"
        customExpression = props.customExpression
        non_empty_param = ",".join(x for x in [relation, schema, targetTypes, selectUsing, customExpression] if x is not None and len(x) != 0)
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        targetTypesList = json.loads(parametersMap.get('targetTypes').replace("'", '"'))  # Parse targetTypes once
        return DynamicSelect.DynamicSelectProperties(
            relation=parametersMap.get('relation')[1:-1],
            schema=parametersMap.get('schema'),
            targetTypes=parametersMap.get('targetTypes'),
            customExpression=parametersMap.get('customExpression'),
            selectUsing=parametersMap.get('selectUsing')[1:-1],
            boolTypeChecked="Boolean" in targetTypesList,
            strTypeChecked="String" in targetTypesList,
            intTypeChecked="Integer" in targetTypesList,
            shortTypeChecked="Short" in targetTypesList,
            byteTypeChecked="Byte" in targetTypesList,
            longTypeChecked="Long" in targetTypesList,
            floatTypeChecked="Float" in targetTypesList,
            doubleTypeChecked="Double" in targetTypesList,
            decimalTypeChecked="Decimal" in targetTypesList,
            binaryTypeChecked="Binary" in targetTypesList,
            dateTypeChecked="Date" in targetTypesList,
            timestampTypeChecked="Timestamp" in targetTypesList,
            structTypeChecked="Struct" in targetTypesList
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("schema", properties.schema),
                MacroParameter("targetTypes", properties.targetTypes),
                MacroParameter("customExpression", properties.customExpression),
                MacroParameter("selectUsing", properties.selectUsing),
            ],
        )


