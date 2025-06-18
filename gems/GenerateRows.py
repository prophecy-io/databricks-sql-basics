import dataclasses
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class GenerateRows(MacroSpec):
    name: str = "GenerateRows"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Prepare"

    @dataclass(frozen=True)
    class GenerateRowsProperties(MacroProperties):
        relation_name: List[str] = field(default_factory=list)
        new_field_name: Optional[str] = None
        start_expr: Optional[str] = None
        end_expr: Optional[str] = None
        step_expr: Optional[str] = None
        data_type: Optional[str] = None
        interval_unit: Optional[str] = None


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

    def dialog(self) -> Dialog:
        return Dialog("GenerateRows").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                Ports(allowInputAddOrDelete=True, allowCustomOutputSchema=True),
                "content"
            )
                .addColumn(
                StackLayout(height="100%")
                    .addElement(TextBox("new_field_name").bindPlaceholder("""abc""").bindProperty("new_field_name"))
                    .addElement(TextBox("start_expr").bindPlaceholder("""abc""").bindProperty("start_expr"))
                    .addElement(TextBox("end_expr").bindPlaceholder("""abc""").bindProperty("end_expr"))
                    .addElement(TextBox("step_expr").bindPlaceholder("""abc""").bindProperty("step_expr"))
                    .addElement(TextBox("data_type").bindPlaceholder("""abc""").bindProperty("data_type"))
                    .addElement(TextBox("interval_unit").bindPlaceholder("""abc""").bindProperty("interval_unit"))
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super().validate(context, component)
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)


    def apply(self, props: GenerateRowsProperties) -> str:
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + table_name + "'",
            "'" + str(props.new_field_name) + "'",
            "'" + str(props.start_expr) + "'",
            "'" + str(props.end_expr) + "'",
            "'" + str(props.step_expr) + "'",
            "'" + str(props.data_type) + "'",
            "'" + str(props.interval_unit) + "'"
        ]

        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return GenerateRows.GenerateRowsProperties(
            relation_name=parametersMap.get('relation_name'),
            new_field_name=parametersMap.get('new_field_name'),
            start_expr=parametersMap.get('start_expr'),
            end_expr=parametersMap.get('end_expr'),
            step_expr=parametersMap.get('step_expr'),
            data_type=parametersMap.get('data_type'),
            interval_unit=parametersMap.get('interval_unit')

        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("new_field_name", properties.new_field_name),
                MacroParameter("start_expr", properties.start_expr),
                MacroParameter("end_expr", properties.end_expr),
                MacroParameter("step_expr", properties.step_expr),
                MacroParameter("data_type", properties.diag_message),
                MacroParameter("interval_unit", properties.interval_unit)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
