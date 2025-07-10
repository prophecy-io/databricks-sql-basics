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
    category: str = "Custom"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class FindDuplicatesProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ''
        columnNames: List[str] = field(default_factory=list)
        column_group_condition: str = None
        count_value: str = None
        left_limit: str = None
        right_limit: str = None

    def dialog(self) -> Dialog:
        between_condition = Condition().ifEqual(
            PropExpr("component.properties.column_group_condition"), StringExpr("between")
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
                            .addElement(TitleElement("Select columns to group by"))
                            .addElement(
                                SchemaColumnsDropdown("", appearance="minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("columnNames")
                            )
                        )
                    )
                )
                .addElement(
                    SelectBox("Select your Filter type for column group count")
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
                    ColumnsLayout(gap="1rem", height="100%")
                    .addColumn(
                        between_condition.then(
                            TextBox("Left limit").bindPlaceholder("0").bindProperty("left_limit")
                        ).otherwise(TextBox("count").bindPlaceholder("1").bindProperty("count_value"))
                    )
                    .addColumn(
                        StackLayout(height="100%")
                        .addElement(between_condition.then(
                            TextBox("Right limit").bindPlaceholder("1").bindProperty("right_limit")
                        ))
                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(FindDuplicates, self).validate(context, component)
        condition = component.properties.column_group_condition
        count_value = component.properties.count_value
        left_limit = component.properties.left_limit
        right_limit = component.properties.right_limit

        if condition is None:
            return diagnostics
        if len(component.properties.columnNames) > 0 :
            missingKeyColumns = [col for col in component.properties.columnNames if
                                 col not in component.properties.schema]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic("component.properties.columnNames", f"Selected columns {missingKeyColumns} are not present in input schema.", SeverityLevelEnum.Error)
                )
        if condition != "between" and ((count_value is None) or (not count_value.isdigit())):
            diagnostics.append(
                Diagnostic("component.properties.count_value", f"count should be a whole number. Right now, count is {count_value or 'None'}", SeverityLevelEnum.Error)
            )
        if condition == "between" and ((left_limit is None) or (not left_limit.isdigit())):
            diagnostics.append(
                Diagnostic("component.properties.left_limit", f"left_limit should be a whole number. Right now, left_limit is {left_limit or 'None'}", SeverityLevelEnum.Error)
            )
        if condition == "between" and ((right_limit is None) or (not right_limit.isdigit())):
            diagnostics.append(
                Diagnostic("component.properties.right_limit", f"right_limit should be a whole number. Right now, right_limit is {right_limit or 'None'}", SeverityLevelEnum.Error)
            )
        if condition == "between":
            if (left_limit is None) or (right_limit is None) or (int(left_limit)>int(right_limit)):
                diagnostics.append(
                    Diagnostic("component.properties.between", f"left_limit should be a less than or equal to right_limit number. Right now, left_limit is {component.properties.left_limit}, right_limit is {component.properties.right_limit}", SeverityLevelEnum.Error)
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
            safe_str(props.count_value),
            safe_str(props.left_limit),
            safe_str(props.right_limit)
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
            count_value=parametersMap.get('count_value'),
            left_limit=parametersMap.get('left_limit'),
            right_limit=parametersMap.get('right_limit')
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
                MacroParameter("count_value", str(properties.count_value)),
                MacroParameter("left_limit", str(properties.left_limit)),
                MacroParameter("right_limit", str(properties.right_limit))
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
