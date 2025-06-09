import dataclasses
import datetime as dt
import json

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class ToDo(MacroSpec):
    name: str = "ToDo"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Custom"


    @dataclass(frozen=True)
    class ToDoProperties(MacroProperties):
        schema: str = ''
        relation_name: List[str] = field(default_factory=list)
        error_string: str = ''
        code_string: str = ''
        diag_message: str = ''

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
        return Dialog("ToDo").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(
                Ports(allowInputAddOrDelete=True, allowCustomOutputSchema=True, defaultCustomOutputSchema=True),
                "content"
            )
                .addColumn(
                StackLayout(height="100%").addElement(
                    StackLayout()
                        .addElement(TitleElement("Error"))
                        .addElement(Editor(height="10bh").bindProperty("error_string"))
                ).addElement(
                    StackLayout()
                        .addElement(TitleElement("Code"))
                        .addElement(Editor(height="70bh").bindProperty("code_string"))
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics =  super().validate(context,component)
        if component.properties.diag_message != '':
            diagnostics.append(
                Diagnostic("component.properties.columnName", component.properties.diag_message,
                           SeverityLevelEnum.Error))
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: ToDoProperties) -> str:
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + table_name + "'",
            props.schema,
            "'" + props.error_string + "'",
            "'" + props.code_string + "'"
        ]

        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return DataCleansing.DataCleansingProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            error_string=parametersMap.get('error_string'),
            code_string=parametersMap.get('code_string'),
            diag_message=parametersMap.get('diag_message')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("error_string", properties.error_string),
                MacroParameter("code_string", properties.code_string),
                MacroParameter("diag_message", properties.diag_message)
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


