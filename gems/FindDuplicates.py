
from dataclasses import dataclass

import dataclasses
import json
from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class FindDuplicates(MacroSpec):
    name: str = "FindDuplicates"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class FindDuplicatesProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ''
        columnNames: List[str] = field(default_factory=list)
        column_group_condition: str = ""
        grouped_count: str = ""
        lower_limit: str = ""
        upper_limit: str = ""
        outputType: str = "U_output"

    def dialog(self) -> Dialog:
        between_condition = Condition().ifEqual(
            PropExpr("component.properties.column_group_condition"), StringExpr("between")
        )

        selectBox = (RadioGroup("")
                     .addOption("Unique", "U_output",
                                description=("Returns the unique records from the dataset based on the selected columns combination"))
                     .addOption("Duplicate", "D_output",
                                description="Returns the duplicate records from the dataset based on the selected columns combination"
                                )
                     .addOption("Custom", "Custom_output",
                                description="Returns the records with grouped column count as per below selected options"
                                )
                     .setOptionType("button")
                     .setVariant("medium")
                     .setButtonStyle("solid")
                     .bindProperty("outputType")
                     )

        return Dialog("FindDuplicates").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    StepContainer()
                    .addElement(
                        Step()
                        .addElement(
                            StackLayout(height="100%")
                            .addElement(TitleElement("Select columns"))
                            .addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("columnNames")
                            )
                        )
                    )
                )
                .addElement(selectBox)
                .addElement(
                    Condition().ifEqual(PropExpr("component.properties.outputType"), StringExpr("Custom_output")).then(
                        StepContainer()
                        .addElement(
                            Step()
                            .addElement(
                                StackLayout(height="100%")
                                .addElement(TitleElement("Select the below options to apply custom grouping"))
                                .addElement(
                                    StackLayout(height="100%")
                                    .addElement(
                                        SelectBox("Select the Filter type for column group count")
                                        .bindProperty("column_group_condition")
                                        .withStyle({"width": "100%"})
                                        .withDefault("")
                                        .addOption("Group count equal to", "equal_to")
                                        .addOption("Group count less than", "less_than")
                                        .addOption("Group count greater than", "greater_than")
                                        .addOption("Group count not equal to", "not_equal_to")
                                        .addOption("Group count between", "between")
                                    )
                                    .addElement(
                                        Condition().ifEqual(PropExpr("component.properties.column_group_condition"), StringExpr("between")).then(
                                            TextBox("Lower limit (inclusive)").bindProperty("lower_limit").bindPlaceholder("")
                                        ).otherwise(TextBox("Grouped count").bindProperty("grouped_count").bindPlaceholder(""))
                                    )
                                    .addElement(between_condition.then(
                                        TextBox("Upper limit (inclusive)").bindProperty("upper_limit").bindPlaceholder(""))
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(FindDuplicates, self).validate(context, component)

        if len(component.properties.columnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.columnNames", f"Select atleast one column to apply masking on", SeverityLevelEnum.Error)
            )
        if len(component.properties.columnNames) > 0 :
            missingKeyColumns = [col for col in component.properties.columnNames if
                                 col not in component.properties.schema]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic("component.properties.columnNames", f"Selected columns {missingKeyColumns} are not present in input schema.", SeverityLevelEnum.Error)
                )
        if component.properties.outputType == "Custom_output":
            if component.properties.column_group_condition == "":
                diagnostics.append(
                    Diagnostic("component.properties.column_group_condition", f"Select one group condition from the given dropdown.", SeverityLevelEnum.Error)
                )
            if component.properties.column_group_condition == "between" and component.properties.lower_limit == "":
                diagnostics.append(
                    Diagnostic("component.properties.lower_limit", f"Specify the lower limit value for grouped column count.", SeverityLevelEnum.Error)
                )
            if component.properties.column_group_condition == "between" and component.properties.upper_limit == "":
                diagnostics.append(
                    Diagnostic("component.properties.upper_limit", f"Specify the upper limit value for grouped column count.", SeverityLevelEnum.Error)
                )

        return diagnostics

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
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: FindDuplicatesProperties) -> str:
        # generate the actual macro call given the component's state
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        resolved_macro_name = f"{self.projectName}.{self.name}"

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        arguments = [
            "'" + table_name + "'",
            safe_str(props.columnNames),
            safe_str(props.column_group_condition),
            safe_str(props.outputType),
            safe_str(props.grouped_count),
            safe_str(props.lower_limit),
            safe_str(props.upper_limit)
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return FindDuplicates.FindDuplicatesProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            column_group_condition=parametersMap.get('column_group_condition'),
            grouped_count=parametersMap.get('grouped_count'),
            lower_limit=parametersMap.get('lower_limit'),
            upper_limit=parametersMap.get('upper_limit'),
            outputType=parametersMap.get('outputType')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("column_group_condition", str(properties.column_group_condition)),
                MacroParameter("grouped_count", str(properties.grouped_count)),
                MacroParameter("lower_limit", str(properties.lower_limit)),
                MacroParameter("upper_limit", str(properties.upper_limit)),
                MacroParameter("outputType", str(properties.outputType))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
