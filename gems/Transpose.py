from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class Transpose(MacroSpec):
    name: str = "Transpose"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class TransposeProperties(MacroProperties):
        # properties for the component with default values
        schema: str = ''
        relation_name: List[str] = field(default_factory=list)
        keyColumns: Optional[List[str]] = field(default_factory=list)
        dataColumns: Optional[List[str]] = field(default_factory=list)

    def get_relation_names(self,component: Component, context: SqlContext):
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
        # Define the UI dialog structure for the component
        return Dialog("Transpose").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height=("100%"))
                .addElement(
                    SchemaColumnsDropdown("Key Columns")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("keyColumns")
                        .showErrorsFor("keyColumns")
                )
                .addElement(
                    SchemaColumnsDropdown("Data Columns")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("dataColumns")
                        .showErrorsFor("dataColumns")
                ).addElement(
                        AlertBox(
                            variant="success",
                            _children=[
                                Markdown(
                                    "* **Key Columns** : Columns that act as **identifiers** for each row. These remain as-is during the transpose. Think of them like primary keys or grouping fields (e.g., `id`, `country`, `date`).\n"
                                    "* **Data Columns** : Columns that you want to **pivot into Name/Value pairs**. Each of these becomes a row in the transposed output.\n\n"
                                    "Let's understand from a simple example.\n\n"
                                    "**Input:**\n\n"
                                    "| id | country | sales | cost |\n"
                                    "|----|---------|-------|------|\n"
                                    "| 1  | USA     | 100   | 50   |\n\n"
                                    "**Transposed:** (with key columns = `id`, `country` and data columns = `sales`, `cost`)\n\n"
                                    "| id | country | Name  | Value |\n"
                                    "|----|---------|-------|-------|\n"
                                    "| 1  | USA     | sales | 100   |\n"
                                    "| 1  | USA     | cost  | 50    |"
                                )
                            ]
                        )
                ),
            "5fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(Transpose, self).validate(context,component)
        if not component.properties.keyColumns:
            diagnostics.append(
                Diagnostic("properties.keyColumns", f"Key columns can't be empty.",SeverityLevelEnum.Error)
            )

        if not component.properties.dataColumns:
            diagnostics.append(
                Diagnostic("properties.dataColumns", f"Data columns can't be empty.",SeverityLevelEnum.Error)
            )
        return diagnostics

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

    def apply(self, props: TransposeProperties) -> str:
        # Get the table name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        allColumnNames = [field["name"] for field in json.loads(props.schema)]

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + table_name + "'",
            str(props.keyColumns),
            str(props.dataColumns),
            str(allColumnNames)
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:

        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return Transpose.TransposeProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            keyColumns=json.loads(parametersMap.get('keyColumns').replace("'", '"')),
            dataColumns=json.loads(parametersMap.get('dataColumns').replace("'", '"'))
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("keyColumns", json.dumps(properties.keyColumns)),
                MacroParameter("dataColumns", json.dumps(properties.dataColumns))
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