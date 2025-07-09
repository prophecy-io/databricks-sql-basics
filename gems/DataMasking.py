from dataclasses import dataclass

import dataclasses
import json
from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

class DataMasking(MacroSpec):
    name: str = "DataMasking"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class DataMaskingProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ''
        columnNames: List[str] = field(default_factory=list)
        maskingMethod: str = ""
        upperCharSubstitute: str = ""
        lowerCharSubstitute: str = ""
        digitCharSubstitute: str = ""
        otherCharSubstitute: str = ""
        sha2BitLength: str = ""
        combinedHash: bool = False

    def dialog(self) -> Dialog:
        mask_condition = Condition().ifEqual(
            PropExpr("component.properties.maskingMethod"), StringExpr("mask")
        )

        hash_condition = Condition().ifEqual(
            PropExpr("component.properties.maskingMethod"), StringExpr("hash")
        )

        sha2_condition = Condition().ifEqual(
            PropExpr("component.properties.maskingMethod"), StringExpr("sha2")
        )

        mask_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("upperChar substitute key").bindProperty("upperCharSubstitute").bindPlaceholder("default is 'X'. If set to NULL, upper case chars remain unmasked"))
            .addElement(TextBox("lowerChar substitute key").bindProperty("lowerCharSubstitute").bindPlaceholder("default is 'x'. If set to NULL, lower case chars remain unmasked"))
            .addElement(TextBox("digitChar substitute key").bindProperty("digitCharSubstitute").bindPlaceholder("default is 'n'. If set to NULL, digit chars remain unmasked"))
            .addElement(TextBox("otherChar substitute key").bindProperty("otherCharSubstitute").bindPlaceholder(""))
        )

        hash_params_ui = (
            Checkbox("Apply a single hash to all the selected columns at once").bindProperty("combinedHash")
        )

        sha2_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(SelectBox("select the bit length").bindProperty("sha2BitLength").withDefault("")
                        .addOption("224", "224")
                        .addOption("256", "256")
                        .addOption("384", "384")
                        .addOption("512", "512")
                        )
        )

        dialog = Dialog("masking_dialog_box").addElement(
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
                            .addElement(TitleElement("Select columns to applying mask on"))
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
                    SelectBox("Choose your masking method")
                    .bindProperty("maskingMethod")
                    .withStyle({"width": "100%"})
                    .withDefault("")
                    .addOption("mask", "mask")
                    .addOption("crc32", "crc32")
                    .addOption("hash", "hash")
                    .addOption("sha", "sha")
                    .addOption("sha2", "sha2")
                    .addOption("md5", "md5")
                )
                .addElement(
                    mask_condition.then(
                        mask_params_ui
                    )
                )
                .addElement(
                    hash_condition.then(
                        hash_params_ui
                    )
                )
                .addElement(
                    sha2_condition.then(
                        sha2_params_ui
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(DataMasking, self).validate(context, component)
        if len(component.properties.columnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.columnNames", f"Select atleast one column to apply masking on", SeverityLevelEnum.Error)
            )
        elif len(component.properties.columnNames) > 0 :
            missingKeyColumns = [col for col in component.properties.columnNames if
                                 col not in component.properties.schema]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic("component.properties.columnNames", f"Selected columns {missingKeyColumns} are not present in input schema.", SeverityLevelEnum.Error)
                )
        if component.properties.maskingMethod == "sha2" and component.properties.sha2BitLength == "":
            diagnostics.append(
                Diagnostic("component.properties.maskingMethod", f"bit length for sha2 masking cannot be empty.", SeverityLevelEnum.Error)
            )
        if component.properties.maskingMethod == "mask" and ((component.properties.upperCharSubstitute).upper() != "NULL" and
                                                             len(component.properties.upperCharSubstitute)>1):
            diagnostics.append(
                Diagnostic("component.properties.upperCharSubstitute", f"length for upperChar substitute key cannot be greater than 1", SeverityLevelEnum.Error)
            )
        if component.properties.maskingMethod == "mask" and ((component.properties.lowerCharSubstitute).upper() != "NULL" and
                                                             len(component.properties.lowerCharSubstitute)>1):
            diagnostics.append(
                Diagnostic("component.properties.lowerCharSubstitute", f"length for lowerChar substitute key cannot be greater than 1", SeverityLevelEnum.Error)
            )
        if component.properties.maskingMethod == "mask" and ((component.properties.digitCharSubstitute).upper() != "NULL" and
                                                             len(component.properties.digitCharSubstitute)>1):
            diagnostics.append(
                Diagnostic("component.properties.digitCharSubstitute", f"length for digitChar substitute key cannot be greater than 1", SeverityLevelEnum.Error)
            )
        if component.properties.maskingMethod == "mask" and ((component.properties.otherCharSubstitute).upper() != "NULL" and
                                                             len(component.properties.otherCharSubstitute)>1):
            diagnostics.append(
                Diagnostic("component.properties.otherCharSubstitute", f"length for otherChar substitute key cannot be greater than 1", SeverityLevelEnum.Error)
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

    def apply(self, props: DataMaskingProperties) -> str:
        # Generate the actual macro call given the component's state
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        resolved_macro_name = f"{self.projectName}.{self.name}"

        def safe_str(val):
            if val is None or val == "":
                return "''"
            if isinstance(val, list):
                return str(val)
            return f"'{val}'"

        arguments = [
            safe_str(table_name),
            safe_str(props.columnNames),
            safe_str(props.maskingMethod),
            safe_str(props.upperCharSubstitute if props.upperCharSubstitute.upper() != "NULL" else props.upperCharSubstitute.upper()),
            safe_str(props.lowerCharSubstitute if props.lowerCharSubstitute.upper() != "NULL" else props.lowerCharSubstitute.upper()),
            safe_str(props.digitCharSubstitute if props.digitCharSubstitute.upper() != "NULL" else props.digitCharSubstitute.upper()),
            safe_str(props.otherCharSubstitute if props.otherCharSubstitute.upper() != "NULL" else props.otherCharSubstitute.upper()),
            safe_str(props.sha2BitLength),
            safe_str(props.combinedHash)
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return DataMasking.DataMaskingProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            maskingMethod=parametersMap.get('maskingMethod'),
            upperCharSubstitute=parametersMap.get('upperCharSubstitute'),
            lowerCharSubstitute=parametersMap.get('lowerCharSubstitute'),
            digitCharSubstitute=parametersMap.get('digitCharSubstitute'),
            otherCharSubstitute=parametersMap.get('otherCharSubstitute'),
            sha2BitLength=parametersMap.get('sha2BitLength'),
            combinedHash=parametersMap.get('combinedHash')
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
                MacroParameter("maskingMethod", str(properties.maskingMethod)),
                MacroParameter("upperCharSubstitute", str(properties.upperCharSubstitute)),
                MacroParameter("lowerCharSubstitute", str(properties.lowerCharSubstitute)),
                MacroParameter("digitCharSubstitute", str(properties.digitCharSubstitute)),
                MacroParameter("otherCharSubstitute", str(properties.otherCharSubstitute)),
                MacroParameter("sha2BitLength", str(properties.sha2BitLength)),
                MacroParameter("combinedHash", str(properties.combinedHash)),
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


