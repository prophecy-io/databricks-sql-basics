from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class JSONParse(MacroSpec):
    name: str = "JSONParse"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Parse"

    @dataclass(frozen=True)
    class JSONParseProperties(MacroProperties):
        # properties for the component with default values
        columnNames: List[str] = field(default_factory=list)
        relation_name: List[str] = field(default_factory=list)

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
        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                    .addElement(
                        StackLayout(height="100%")
                        .addElement(
                                StackLayout()
                                            .addElement(
                                                SchemaColumnsDropdown("Source Column Name(s)")
                                                .withSearchEnabled()
                                                .withMultipleSelection()
                                                .bindSchema("component.ports.inputs[0].schema")
                                                .bindProperty("columnNames")
                                                .showErrorsFor("columnNames")
                                            )
                                )                   
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(JSONParse, self).validate(context, component)
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the newState's state and return the new state
        relation_name = self.get_relation_names(component,context)
        return (replace(newState, properties=replace(newState.properties,relation_name=relation_name)))

    def apply(self, props: JSONParseProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"
        
        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        arguments = [
            "'" + table_name + "'",
            str(props.columnNames)
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return JSONParse.JSONParseProperties(
            relation_name=parametersMap.get('relation_name'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"'))
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", properties.relation_name),
                MacroParameter("columnNames", json.dumps(properties.columnNames))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component,context)
        return (replace(component, properties=replace(component.properties,relation_name=relation_name)))