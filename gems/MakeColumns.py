
from dataclasses import dataclass
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class MakeColumns(MacroSpec):
    name: str = "MakeColumns"
    projectName: str = "Gem_creator"
    category: str = "Transform"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class MakeColumnsProperties(MacroProperties):
        # properties for the component with default values
        schema: str = ''
        relation_name: List[str] = field(default_factory=list)
        numCols: int = 2
        orientation: str = "horizontal"
        groupbyCols: Optional[List[str]] = field(default_factory=list)
        allFieldsNames: Optional[List[str]] = field(default_factory=list)

    def dialog(self) -> Dialog:
        return Dialog("MakeColumns").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    NumberBox("Number of Columns")
                        .bindPlaceholder("Number of Columns")
                        .bindProperty("numCols")
                )
                .addElement(
                    SelectBox("Arrangement Orientation")
                        .addOption("Horizontally", "horizontal")
                        .addOption("Vertically", "vertical")
                        .bindProperty("orientation")
                )
                .addElement(
                    SchemaColumnsDropdown("Grouping Fields", appearance="minimal")
                        .withMultipleSelection()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("groupbyCols")
                        .showErrorsFor("groupbyCols")
                )
            )
        )

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

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        # TODO: Check why allFieldsNames is not populating
        allFieldsNames = [field["name"] for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
            allFieldsNames = allFieldsNames
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: MakeColumnsProperties) -> str:
        import json
        # generate the actual macro call given the component's state
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        resolved_macro_name = f"{self.projectName}.{self.name}"
        non_empty_param = ",".join([
            "'" + table_name + "'",
            str(props.schema),
            str(props.numCols),
            str(props.allFieldsNames),
            f"'{props.orientation}'",
            str(props.groupbyCols)])
        # MakeColumns(relation_name, num_columns, arrangement='horizontal', grouping_fields=[], data_fields=[])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'


    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return MakeColumns.MakeColumnsProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            numCols=int(parametersMap.get('numCols')),
            allFieldsNames=json.loads(parametersMap.get('allFieldsNames').replace("'", '"')),
            orientation = parametersMap.get('orientation'),
            groupByCols=json.loads(parametersMap.get('groupByCols').replace("'", '"'))
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("numCols", str(properties.numCols)),
                MacroParameter("allFieldsNames", json.dumps(properties.allFieldsNames)),
                MacroParameter("orientation", str(properties.orientation)),
                MacroParameter("groupByCols", json.dumps(properties.groupByCols))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)
        return (replace(component, properties=replace(component.properties, relation_name=relation_name)))

