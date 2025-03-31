from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

class TextToColumns(MacroSpec):
    name: str = "UnionByName"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Join/Split"

    @dataclass(frozen=True)
    class TextToColumnsProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list) 
        firstSchema: str = ''
        secondSchema: str = ''
        missingColumnOps: str = ""

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
                        RadioGroup("")                        
                        .addOption(
                            "Union By Name (Allow Missing Columns)",
                            "allowMissingColumns",
                            ("UnionAll"),
                            ("Lets you safely combine two Dataframes by matching column names and filling in missing ones with nulls")
                        )
                        .addOption(
                            "Union By Name (No Missing Column)",
                            "nameBasedUnionOperation",
                            ("UnionAll"),
                            ("Union of two DataFrames with same columns in different order")
                        )                                   
                        .setOptionType("button")
                        .setVariant("large")
                        .setButtonStyle("solid")
                        .bindProperty("missingColumnOps")                        
                    )
                )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(TextToColumns, self).validate(context, component)
        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the newState's state and return the new state
        relation_name = self.get_relation_names(newState,context)
        firstSchema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        secondSchema = json.loads(str(newState.ports.inputs[1].schema).replace("'", '"'))

        firstSchemafieldsArray = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in firstSchema["fields"]]
        secondSchemafieldsArray = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in secondSchema["fields"]]

        newProperties = dataclasses.replace(
            newState.properties, 
            firstSchema=json.dumps(firstSchemafieldsArray),
            secondSchema=json.dumps(secondSchemafieldsArray),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)        

    def apply(self, props: TextToColumnsProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"
        
        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        arguments = [
            "'" + table_name + "'",
            props.firstSchema,
            props.secondSchema,
            "'" + props.missingColumnOps + "'"
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return TextToColumns.TextToColumnsProperties(
            relation_name=parametersMap.get('relation_name'),
            firstSchema=parametersMap.get('firstSchema'),
            secondSchema=parametersMap.get('secondSchema'),
            missingColumnOps=parametersMap.get('missingColumnOps')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", properties.relation_name),
                MacroParameter("firstSchema", properties.firstSchema),
                MacroParameter("secondSchema", properties.secondSchema),
                MacroParameter("missingColumnOps", properties.missingColumnOps)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):

        relation_name = self.get_relation_names(component,context)
        firstSchema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        secondSchema = json.loads(str(component.ports.inputs[1].schema).replace("'", '"'))

        firstSchemafieldsArray = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in firstSchema["fields"]]
        secondSchemafieldsArray = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in secondSchema["fields"]]

        newProperties = dataclasses.replace(
            component.properties, 
            firstSchema=json.dumps(firstSchemafieldsArray),
            secondSchema=json.dumps(secondSchemafieldsArray),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)