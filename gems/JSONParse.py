from dataclasses import dataclass
import dataclasses
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from typing import Optional
import json

class JSONParse(MacroSpec):
    name: str = "JSONParse"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Parse"
    minNumOfInputPorts: int = 1

    @dataclass(frozen=True)
    class JSONParseProperties(MacroProperties):
        # properties for the component with default values
        jsonColumnName: str = ""
        uniqueColumnName: str = ""
        relation_name: List[str] = field(default_factory=list)
        jsonParsingMethod: str = "output_single_string_field"
        sampleRecord: Optional[str] = None
        sampleSchema: Optional[str] = None

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
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def dialog(self) -> Dialog:
        sampleRecordTextJSON = TextArea("Sample JSON record to parse schema from", 20).bindProperty("sampleRecord").bindPlaceholder("""{
"root": {
    "person": {
    "id": 1,
    "name": {
        "first": "John",
        "last": "Doe"
    },
    "address": {
        "street": "Main St",
        "city": "Springfield",
        "zip": 12345
    }
    }
}
}""")

        structSchemaText = TextArea("Schema struct to parse the column", 20).bindProperty("sampleSchema").bindPlaceholder("""STRUCT<
root: STRUCT<
    person: STRUCT<
    id: INT,
    name: STRUCT<
        first: STRING,
        last: STRING
    >,
    address: STRUCT<
        street: STRING,
        city: STRING,
        zip: INT
    >
    >
>
>""")

        jsonParsingRadioGroup = (
            RadioGroup("")
            .addOption("Output Values into Single String Field", "output_single_string_field", description=
            "This option outputs 2 fields: JSON_Name(json key) and JSON_ValueString(json value)")
            .addOption("Output Values into Data Type Specific Fields", "output_datatype_specific_field",
                       description="This option outputs corresponding datatype 'value' of the JSON Object (key:value pair)")
            .addOption("Unnest JSON Field", "output_unnest_json_field",
                       description="This option allows to un-nest JSON objects into columns. It goes only one level deeper into the JSON object")
            .addOption("Flatten Array", "output_flatten_array",
                       description="This option is applicable for columns that have array values only. It allows you to expand a JSON array column by removing the square brackets. It creates a separate row for each element separated by a comma and assigns an ID for each row")
            .addOption("Parse from sample record", "parseFromSampleRecord", description="Provide a sample record to parse the schema from")
            .addOption("Parse from schema", "parseFromSchema", description="Provide sample schema in SQL struct format to parse the data with")
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("jsonParsingMethod")
        )

        return Dialog("JsonColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(
                    TitleElement("Select Column to Parse")
                )
                .addElement(StepContainer()
                .addElement(
                    Step()
                    .addElement(
                        ColumnsLayout(gap="1rem", height="100%").addColumn(
                            StackLayout(height="100%")
                            .addElement(
                                SchemaColumnsDropdown("Select json column", appearance = "minimal")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("jsonColumnName")
                                .showErrorsFor("jsonColumnName")
                            )
                        ).addColumn(
                            StackLayout(height="100%")
                            .addElement(
                                SchemaColumnsDropdown("Select primary key column", appearance = "minimal")
                                .bindSchema("component.ports.inputs[0].schema")
                                .bindProperty("uniqueColumnName")
                                .showErrorsFor("uniqueColumnName")
                            )
                        )

                    )
                )
                )
                .addElement(StepContainer()
                .addElement(
                    Step()
                    .addElement(
                        StackLayout(height="100%")
                        .addElement(TitleElement("Select JSON parsing method"))
                        .addElement(jsonParsingRadioGroup)
                    )
                )
                )
                .addElement(
                    Condition().ifEqual(PropExpr("component.properties.jsonParsingMethod"), StringExpr("parseFromSampleRecord")).then(sampleRecordTextJSON)
                )
                .addElement(
                    Condition().ifEqual(PropExpr("component.properties.jsonParsingMethod"), StringExpr("parseFromSchema")).then(structSchemaText)
                )
            )
        )


    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(JSONParse, self).validate(context, component)
        field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]

        if component.properties.jsonColumnName is None or component.properties.jsonColumnName == '':
            diagnostics.append(
                Diagnostic("component.properties.jsonColumnName", "Select a json column for the operation",
                           SeverityLevelEnum.Error))
        else:
            # Extract all column names from the schema
            if component.properties.jsonColumnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.jsonColumnName", f"Selected column {component.properties.jsonColumnName} is not present in input schema.", SeverityLevelEnum.Error)
                )

        # Extract all column names from the schema
        if component.properties.uniqueColumnName != "" and component.properties.uniqueColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.uniqueColumnName", f"Selected column {component.properties.uniqueColumnName} is not present in input schema.", SeverityLevelEnum.Error)
            )

        if component.properties.jsonParsingMethod == 'parseFromSchema':
            if component.properties.sampleSchema is None or component.properties.sampleSchema == "":
                diagnostics.append(
                    Diagnostic("component.properties.sampleSchema", "Please provide a valid SQL struct schema",
                               SeverityLevelEnum.Error))

        elif component.properties.jsonParsingMethod == 'parseFromSampleRecord':
            if component.properties.sampleRecord is None or component.properties.sampleRecord == "":
                diagnostics.append(
                    Diagnostic("component.properties.sampleRecord", "Please provide a valid sample json record",
                               SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the newState's state and return the new state
        relation_name = self.get_relation_names(newState, context)
        return (replace(newState, properties=replace(newState.properties,relation_name=relation_name)))

    def apply(self, props: JSONParseProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        sampleRecord: str = props.sampleRecord if props.sampleRecord is not None else ""
        sampleSchema: str = props.sampleSchema if props.sampleSchema is not None else ""

        if props.jsonParsingMethod not in ('output_unnest_json_field', 'output_flatten_array'):
            arguments = [
                "'" + table_name + "'",
                "'" + props.jsonColumnName + "'",
                "'" + props.uniqueColumnName + "'",
                "'" + props.jsonParsingMethod + "'",
                "'" + sampleRecord + "'",
                "'" + sampleSchema + "'"
            ]
        else:
            arguments = [
                "'" + table_name + "'",
                "'" + props.jsonColumnName + "'",
                "'" + props.uniqueColumnName + "'",
                "'" + props.jsonParsingMethod + "'",
                "'" + sampleRecord + "'",
                "'" + sampleSchema + "'",
                "1"
            ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return JSONParse.JSONParseProperties(
            relation_name=parametersMap.get('relation_name'),
            jsonColumnName=parametersMap.get('jsonColumnName'),
            uniqueColumnName=parametersMap.get('uniqueColumnName'),
            jsonParsingMethod=parametersMap.get('jsonParsingMethod'),
            sampleRecord=parametersMap.get('sampleRecord'),
            sampleSchema=parametersMap.get('sampleSchema')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("jsonColumnName", properties.jsonColumnName),
                MacroParameter("uniqueColumnName", properties.uniqueColumnName),
                MacroParameter("jsonParsingMethod", properties.jsonParsingMethod),
                MacroParameter("sampleRecord", properties.sampleRecord),
                MacroParameter("sampleSchema", properties.sampleSchema)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component,context)
        return (replace(component, properties=replace(component.properties,relation_name=relation_name)))

