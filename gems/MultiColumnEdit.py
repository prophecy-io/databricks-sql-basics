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
        schema: Optional[StructType] = StructType([])
        prefixSuffixOption: str = "Prefix"
        prefixSuffixToBeAdded: str = ""
        changeOutputFieldName: bool = False
        expressionToBeApplied: str = ""
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
        dialog = Dialog("MultiColumnEdit")\
        .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowInputAddOrDelete=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer()
                        .addElement(
                            Step()
                                .addElement(
                                    StackLayout(height="100%")
                                        .addElement(
                                            SchemaColumnsDropdown("Selected columns to edit")
                                            .withMultipleSelection().bindSchema("component.ports.inputs[0].schema").bindProperty("columnNames")
                                        )
                                        .addElement(
                                            ColumnsLayout(gap="1rem", height="100%")
                                            .addColumn(
                                                Checkbox("").bindProperty("changeOutputFieldName"), "0.05fr"
                                            )
                                            .addColumn(
                                                NativeText("Maintain the original columns and add "), "0.75fr"
                                            )
                                            .addColumn(
                                                SelectBox("").addOption("Prefix", "Prefix").addOption("Suffix", "Suffix").bindProperty("prefixSuffixOption"), "0.5fr"
                                            )
                                            .addColumn(
                                                TextBox("").bindPlaceholder("Example: new_").bindProperty("prefixSuffixToBeAdded"), "0.5fr"
                                            )
                                            .addColumn(
                                                NativeText("to the new columns"), "3fr"
                                            )
                                        )
                                    )
                        )
                )
                .addElement(
                    StepContainer()
                        .addElement(
                            Step()
                                .addElement(
                                    ExpressionBox("Output Expression").bindProperty("expressionToBeApplied").bindPlaceholder("Write spark sql expression considering `column_value` as column value and `column_name` as column name string literal. Example:\nFor column value: column_value * 100\nFor column name: upper(column_name)").bindLanguage("plaintext")
                                )
                        )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(MultiColumnEdit, self).validate(context,component)
        props = component.properties

        if len(component.properties.columnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.columnNames", "Please select a column for the operation", SeverityLevelEnum.Error))

        if len(component.properties.expressionToBeApplied) == 0:
            diagnostics.append(
                Diagnostic("component.properties.expressionToBeApplied", "Please give an expression to apply", SeverityLevelEnum.Error))

        if component.properties.changeOutputFieldName:
            if len(component.properties.prefixSuffixOption) > 0:
                if len(component.properties.prefixSuffixToBeAdded) == 0:
                    diagnostics.append(
                        Diagnostic("component.properties.prefixSuffixToBeAdded", "Please add prefix or suffix", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        portSchema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in portSchema["fields"]]
        struct_fields = [StructField(field["name"], StructType(), True) for field in fields_array]
        relation_name = self.get_relation_names(newState,context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema = StructType(struct_fields),
            relation_name = relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: MultiColumnEditProperties) -> str:
        # Get the table name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # Get existing column names
        allColumnNames = [field.name for field in props.schema.fields]

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"


        arguments = [
            "'" + table_name + "'",
            str(allColumnNames),
            str(props.columnNames),
            "'" + props.expressionToBeApplied + "'",
            str(props.changeOutputFieldName).lower(),
            "'" + props.prefixSuffixOption + "'",
            "'" + props.prefixSuffixToBeAdded + "'"
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return MultiColumnEdit.MultiColumnEditProperties(
            relation_name=parametersMap.get('relation_name'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            expressionToBeApplied=parametersMap.get('expressionToBeApplied'),
            changeOutputFieldName=parametersMap.get('changeOutputFieldName').lower() == "true",
            prefixSuffixOption=parametersMap.get('prefixSuffixOption'),
            prefixSuffixToBeAdded=parametersMap.get('prefixSuffixToBeAdded')
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
                MacroParameter("changeOutputFieldName", str(properties.changeOutputFieldName).lower()),
                MacroParameter("prefixSuffixOption", properties.prefixSuffixOption),
                MacroParameter("prefixSuffixToBeAdded", properties.prefixSuffixToBeAdded)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        portSchema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in portSchema["fields"]]
        struct_fields = [StructField(field["name"], StructType(), True) for field in fields_array]
        relation_name = self.get_relation_names(component,context)

        newProperties = dataclasses.replace(
            component.properties,
            schema = StructType(struct_fields),
            relation_name = relation_name
        )
        return component.bindProperties(newProperties)

