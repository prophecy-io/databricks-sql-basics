
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class ColumnParser(MacroSpec):
    name: str = "ColumnParser"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class ColumnParserProperties(MacroProperties):
        # properties for the component with default values
        relation: str = "in0"
        columnToParse: str = ""
        parserType: str = "JSON"
        schema: str = ""
        schemaInferCount: int = 40

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")

        sampleSchemaForXML = """
STRUCT<
  person: STRUCT<
    id: INT,
    name: STRUCT<
      first: STRING,
      last: STRING
    >,
    address: STRUCT<
      street: STRING,
      city: STRING,
      zip: STRING
    >
  >
>"""

        return Dialog("ColumnParser").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Ports(allowInputAddOrDelete=True), "content")
            .addColumn(
                StackLayout(height="100%")
                .addElement(relationTextBox)
                .addElement(
                    ColumnsLayout("1rem")
                    .addColumn(
                        SchemaColumnsDropdown("Source Column Name")
                        .withSearchEnabled()
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("columnToParse")
                        .showErrorsFor("columnToParse"),
                        "0.4fr"
                    )
                )
                .addElement(TitleElement("Parser Type"))
                .addElement(
                    ColumnsLayout("1rem")
                    .addColumn(
                        SelectBox("Select parser type")
                        .addOption("Json Parser", "JSON")
                        .addOption("XML Parser (works on Databricks runtime 14.1+ or Spark Opensource 4.0+) ", "XML")
                        .bindProperty("parserType"),
                        "0.4fr"
                    )
                )
                .addElement(
                    Condition()
                    .ifEqual(PropExpr("component.properties.parserType"), StringExpr("XML"))
                    .then(
                        TextArea("Schema struct to parse the column", 20).bindProperty("schema").bindPlaceholder(sampleSchemaForXML))),
                "1fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        return newState

    def apply(self, props: ColumnParserProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            "'" + props.parserType + "'",
            "'" + props.columnToParse + "'",
            "'" + props.schema + "'"
            ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return ColumnParser.ColumnParserProperties(
            relation=parametersMap.get('relation')[1:-1],
            parserType=parametersMap.get('parserType')[1:-1],
            columnToParse=parametersMap.get('columnToParse')[1:-1],
            schema=parametersMap.get('schema')[1:-1]
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("parserType", properties.parserType),
                MacroParameter("columnToParse", properties.columnToParse),
                MacroParameter("schema", properties.schema)
            ],
        )


