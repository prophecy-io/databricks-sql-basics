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
        columnName: str = ""
        schema: str = ''
        verticalColumnName: str = ""
        uniqueColumnName: str = ""
        relation_name: List[str] = field(default_factory=list)
        parsingMethod: str = "parseFromSampleRecord"
        verticalparsingMethod: str = "output_single_string_field"
        sampleRecord: Optional[str] = None
        sampleSchema: Optional[str] = None
        flatten_method: str = "horizontal_flatten"
        max_recurse_limit: str = ""

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

        json_flatten_box = (
            SelectBox("Choose your flatten method")
            .bindProperty("flatten_method")
            .withStyle({"width": "100%"})
            .withDefault("")
            .addOption("Convert to struct JSON", "horizontal_flatten")
            .addOption("Flatten nested fields vertically ", "vertical_flatten")
        )

        recurse_limit_box = (
            TextBox("Enter the max recurse limit for vertical json flattening").bindPlaceholder("Default value is 100").bindProperty("max_recurse_limit")
        )

        verticalJsonParsingRadioGroup = (
            RadioGroup("")
            .addOption("Output Values into Single String Field", "output_single_string_field", description=
            "This option outputs 2 fields: JSON_Name(json key) and JSON_ValueString(json value)")
            .addOption("Output Values into Data Type Specific Fields", "output_datatype_specific_field",
                       description="This option outputs corresponding datatype 'value' of the JSON Object (key:value pair)")
            .addOption("Unnest JSON Field", "output_unnest_json_field",
                       description="This option allows to un-nest JSON objects. It goes only one level deeper into the JSON object")
            .addOption("Flatten Array", "output_flatten_array",
                       description="This option is applicable for columns that have array values only. It allows you to expand a JSON array column by removing the square brackets. It creates a separate row for each element separated by a comma and assigns an ID for each row")
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("verticalparsingMethod")
        )

        horizontalJsonParsingRadioGroup = (
            RadioGroup("")
            .addOption("Parse from sample record", "parseFromSampleRecord", description="Provide a sample record to parse the schema from")
            .addOption("Parse from schema", "parseFromSchema", description="Provide sample schema in SQL struct format to parse the data with")
            .setOptionType("button")
            .setVariant("medium")
            .setButtonStyle("solid")
            .bindProperty("parsingMethod")
        )


        return Dialog("JsonColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(json_flatten_box)
                .addElement(Condition().ifEqual(PropExpr("component.properties.flatten_method"), StringExpr("vertical_flatten")).then(
                    StepContainer()
                    .addElement(
                        Step()
                        .addElement(
                            StackLayout(height="100%")
                            .addElement(
                                StepContainer()
                                .addElement(
                                    Step()
                                    .addElement(
                                        ColumnsLayout(gap="1rem", height="100%").addColumn(
                                            StackLayout(height="100%")
                                            .addElement(
                                                SchemaColumnsDropdown("Select JSON column", appearance = "default")
                                                .bindSchema("component.ports.inputs[0].schema")
                                                .bindProperty("verticalColumnName")
                                                .showErrorsFor("verticalColumnName")
                                            )
                                        ).addColumn(
                                            StackLayout(height="100%")
                                            .addElement(
                                                SchemaColumnsDropdown("Select primary key column", appearance = "default")
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
                                    .addElement(verticalJsonParsingRadioGroup)
                                )
                            )
                            )
                            .addElement(Condition().ifEqual(PropExpr("component.properties.verticalparsingMethod"), StringExpr("output_single_string_field")).then(recurse_limit_box))
                            .addElement(Condition().ifEqual(PropExpr("component.properties.verticalparsingMethod"), StringExpr("output_datatype_specific_field")).then(recurse_limit_box))
                        )
                    )
                ).otherwise(
                    StepContainer()
                    .addElement(
                        Step()
                        .addElement(
                            StackLayout(height="100%")
                            .addElement(
                                StepContainer()
                                .addElement(
                                    Step()
                                    .addElement(
                                        SchemaColumnsDropdown("Select JSON column", appearance = "default")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("columnName")
                                        .showErrorsFor("columnName")
                                    )
                                )
                            )
                            .addElement(
                                horizontalJsonParsingRadioGroup
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSampleRecord")).then(sampleRecordTextJSON)
                            )
                            .addElement(
                                Condition().ifEqual(PropExpr("component.properties.parsingMethod"), StringExpr("parseFromSchema")).then(structSchemaText)
                            )
                        )

                    )
                )
                )
            )
        )


    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = super(JSONParse, self).validate(context, component)
        field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]

        if component.properties.flatten_method == 'horizontal_flatten':
            if component.properties.columnName is None or component.properties.columnName == '':
                diagnostics.append(
                    Diagnostic("component.properties.columnName", "Select a json column for the operation",
                               SeverityLevelEnum.Error))

            elif component.properties.columnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.columnName", f"Selected column {component.properties.columnName} is not present in input schema.", SeverityLevelEnum.Error)
                )

            if component.properties.parsingMethod == 'parseFromSchema':
                if component.properties.sampleSchema is None or component.properties.sampleSchema == "":
                    diagnostics.append(
                        Diagnostic("component.properties.sampleSchema", "Please provide a valid SQL struct schema",
                                   SeverityLevelEnum.Error))

            elif component.properties.parsingMethod == 'parseFromSampleRecord':
                if component.properties.sampleRecord is None or component.properties.sampleRecord == "":
                    diagnostics.append(
                        Diagnostic("component.properties.sampleRecord", "Please provide a valid sample json record",
                                   SeverityLevelEnum.Error))

        else:
            if component.properties.verticalColumnName is None or component.properties.verticalColumnName == '':
                diagnostics.append(
                    Diagnostic("component.properties.verticalColumnName", "Select a json column for the operation",
                               SeverityLevelEnum.Error))

            elif component.properties.verticalColumnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.verticalColumnName", f"Selected column {component.properties.verticalColumnName} is not present in input schema.", SeverityLevelEnum.Error)
                )

            if component.properties.uniqueColumnName != "" and component.properties.uniqueColumnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.uniqueColumnName", f"Selected column {component.properties.uniqueColumnName} is not present in input schema.", SeverityLevelEnum.Error)
                )

            if component.properties.verticalparsingMethod in ('output_single_string_field', 'output_datatype_specific_field'):
                if not (component.properties.max_recurse_limit.isdigit() and int(component.properties.max_recurse_limit) >= 101) and component.properties.max_recurse_limit != '':
                    diagnostics.append(
                        Diagnostic("component.properties.max_recurse_limit", f"Max recurse limit should be an integer greater than 100", SeverityLevelEnum.Error)
                    )

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the newState's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)
        return (replace(newState, properties=replace(newState.properties,relation_name=relation_name,schema=json.dumps(fields_array))))

    def apply(self, props: JSONParseProperties) -> str:
        # You can now access self.relation_name here
        resolved_macro_name = f"{self.projectName}.{self.name}"
        schema_columns = [js['name'] for js in json.loads(props.schema)]

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        sampleRecord: str = props.sampleRecord if props.sampleRecord is not None else ""
        sampleSchema: str = props.sampleSchema if props.sampleSchema is not None else ""
        parse_method = props.verticalparsingMethod if props.flatten_method == 'vertical_flatten' else props.parsingMethod
        js_col_name = props.verticalColumnName if props.flatten_method == 'vertical_flatten' else props.columnName

        schema_columns.remove(js_col_name)
        remaining_cols = ", ".join(schema_columns)

        if parse_method not in ('output_unnest_json_field', 'output_flatten_array'):
            arguments = [
                "'" + table_name + "'",
                "'" + js_col_name + "'",
                "'" + props.uniqueColumnName + "'",
                "'" + parse_method + "'",
                "'" + sampleRecord + "'",
                "'" + sampleSchema + "'",
                "'" + props.max_recurse_limit + "'",
                "'" + remaining_cols + "'"
            ]
        else:
            arguments = [
                "'" + table_name + "'",
                "'" + js_col_name + "'",
                "'" + props.uniqueColumnName + "'",
                "'" + parse_method + "'",
                "'" + sampleRecord + "'",
                "'" + sampleSchema + "'",
                "'" + props.max_recurse_limit + "'",
                "'" + remaining_cols + "'",
                "1"
            ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        parametersMap = self.convertToParameterMap(properties.parameters)
        print(f"The name of the parametersMap is {parametersMap}")
        return JSONParse.JSONParseProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            columnName=parametersMap.get('columnName'),
            uniqueColumnName=parametersMap.get('uniqueColumnName'),
            parsingMethod=parametersMap.get('parsingMethod'),
            sampleRecord=parametersMap.get('sampleRecord'),
            sampleSchema=parametersMap.get('sampleSchema'),
            flatten_method=parametersMap.get('flatten_method'),
            verticalparsingMethod=parametersMap.get('verticalparsingMethod'),
            verticalColumnName=parametersMap.get('verticalColumnName'),
            max_recurse_limit=parametersMap.get('max_recurse_limit')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("columnName", properties.columnName),
                MacroParameter("uniqueColumnName", properties.uniqueColumnName),
                MacroParameter("parsingMethod", properties.parsingMethod),
                MacroParameter("sampleRecord", properties.sampleRecord),
                MacroParameter("sampleSchema", properties.sampleSchema),
                MacroParameter("flatten_method", properties.flatten_method),
                MacroParameter("verticalparsingMethod", properties.verticalparsingMethod),
                MacroParameter("verticalColumnName", properties.verticalColumnName),
                MacroParameter("max_recurse_limit", properties.max_recurse_limit)
            ]
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component,context)
        return (replace(component, properties=replace(component.properties,relation_name=relation_name,schema=json.dumps(fields_array))))

