
import dataclasses

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
import json

@dataclass(frozen=True)
class OutputField:
    columnHeader: str
    sourceFields: List[str]
    includeDescription: bool
    descriptionExpression: str

class ArrangeTool(MacroSpec):
    name: str = "ArrangeTool"
    projectName: str = "Gem_creator"
    category: str = "Transform"
    minNumOfInputPorts: int = 1

    @dataclass(frozen=True)
    class ArrangeToolProperties(MacroProperties):
        # properties for the component with default values
        schema: str = ''
        relation_name: List[str] = field(default_factory=list)
        keyColumns: List[str] = field(default_factory=list)
        outputFields: List[OutputField] = field(default_factory=lambda: [OutputField("Column Name", [], False, "Default Description")])

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
        keyFieldDropdown = SchemaColumnsDropdown("keyFields", appearance = "minimal")\
                        .withMultipleSelection()\
                        .bindSchema("component.ports.inputs[0].schema")\
                        .bindProperty("keyColumns")
        outputFieldTable = BasicTable("OutputFieldTable", columns=[
                Column("Output Field Name", "columnHeader",TextBox("").bindPlaceholder("column_name").bindProperty("record.columnHeader")),
                Column("Source Fields", "sourceFields",
                    SchemaColumnsDropdown("Inner Fields", appearance = "minimal")
                                .withMultipleSelection()
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("record.sourceFields"),
                    width="25%"
                ),
                Column("Include Description", "includeDescription",
                    (Checkbox("Include Description").bindProperty("record.includeDescription")), width="20%"
                ),
                Column("Description", "descriptionHeader",
                    (
                    ExpressionBox(language="sql")
                        .bindPlaceholder(
                            "Write sql expression considering `column_name` as column name string literal. Example:\n For column name: upper(column_name)")
                        .withGroupBuilder(GroupBuilderType.EXPRESSION)
                        .withUnsupportedExpressionBuilderTypes(
                            [ExpressionBuilderType.INCREMENTAL_EXPRESSION])
                        .bindProperty("record.descriptionExpression")
                    ), width="20%"
                ),
                ],
                delete=True,
                appendNewRow=True,
                targetColumnKey="columnHeader"
            ).bindProperty("outputFields")


        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=False),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    keyFieldDropdown
                ).addElement(
                    outputFieldTable
                )
           )
       )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

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

    def apply(self, props: ArrangeToolProperties) -> str:
        # Get the table name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # Get existing column names
        # allColumnNames = [field["name"] for field in json.loads(props.schema)]

        outputFields = json.dumps(
            [
                {
                    "column_header": fld.columnHeader,
                    "source_fields": fld.sourceFields,
                    "include_description": fld.columnHeader,
                    "description_header": fld.descriptionExpression,
                }
                for fld in props.outputFields
            ]
        )

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + table_name + "'",
            str(props.keyColumns),
            str(outputFields)
        ]
        params = ",".join([param for param in arguments])
        # Arrange(relation_name, key_fields=[], output_fields=[]
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        outputFields = [
                OutputField(
                    columnHeader = fld.get("column_header"),
                    sourceFields = fld.get("source_fields"),
                    includeDescription = fld.get("include_description"),
                    descriptionExpression = fld.get("description_header")
                )
                for fld in json.loads(parametersMap.get('outputFields').replace("'", '"'))
            ]

        return ArrangeTool.ArrangeToolProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            keyColumns=json.loads(parametersMap.get('keyColumns').replace("'", '"')),
            outputFields=outputFields
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        outputFields = json.dumps([{
                    "column_header": fld.columnHeader,
                    "source_fields": fld.sourceFields,
                    "include_description": fld.includeDescription,
                    "description_header": fld.descriptionExpression,
                }
                for fld in properties.outputFields
            ]
        )
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("keyColumns", json.dumps(properties.keyColumns)),
                MacroParameter("outputFields", outputFields)
            ],
        )


