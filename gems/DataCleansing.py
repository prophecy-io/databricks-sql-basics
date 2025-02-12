
from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType



class DataCleansing(MacroSpec):
    name: str = "DataCleansing"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class DataCleansingProperties(MacroProperties):
        # properties for the component with default values
        relation: str = "in0"
        schema: str = ''

        # clean checks
        null_operation: str = "remove_row_null_all_cols"
        columnNames: List[str] = field(default_factory=list)
        replaceNullTextFields: bool = False
        replaceNullTextWith: str = "NA"
        replaceNullForNumericFields: bool = False
        replaceNullNumericWith: int = 0
        trimWhiteSpace: bool = False
        removeTabsLineBreaksAndDuplicateWhitespace: bool = False
        allWhiteSpace: bool = False
        cleanLetters: bool = False
        cleanPunctuations: bool = False
        cleanNumbers: bool = False
        makeLowercase: bool = False
        makeUppercase: bool = False
        makeTitlecase: bool = False

        # sql argument strings
        columnNamesSql: str = ''
        replaceNullTextFieldsSql: str = ''
        replaceNullTextWithSql: str = ''
        replaceNullForNumericFieldsSql: str = ''
        replaceNullNumericWithSql: str = ''
        trimWhiteSpaceSql: str = ''
        removeTabsLineBreaksAndDuplicateWhitespaceSql: str = ''
        allWhiteSpaceSql: str = ''
        cleanLettersSql: str = ''
        cleanPunctuationsSql: str = ''
        cleanNumbersSql: str = ''
        makeLowercaseSql: str = ''
        makeUppercaseSql: str = ''
        makeTitlecaseSql: str = ''

        # for schema column dropdown
        schemaColDropdownSchema: Optional[StructType] = StructType([])

    def dialog(self) -> Dialog:
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        nullOpSelectBox = (SelectBox("")
                           .addOption("1. Remove rows with null in every column", "remove_row_null_all_cols")
                           .addOption("2. Remove columns with null in every row", "remove_col_null_all_rows")
                           .addOption("3. Remove both empty rows and columns (both 1 and 2)",
                                      "remove_empty_rows_cols")
                           .bindProperty("null_operation"))

        selectCol = (SchemaColumnsDropdown("Select columns you want to clean").withMultipleSelection().bindSchema("schemaColDropdownSchema").bindProperty("columnNames"))
        options = (StackLayout(gap="2em")
        .addElement(TitleElement("Clean data"))
        .addElement(
            AlertBox(
                variant="warning",
                _children=[
                    Markdown(
                        "Following operations are only applicable on string columns\n"
                        "Examples - \n"
                        "* **Trim whitespaces** - ' hello prophecy ' => 'hello prophecy'\n"
                        "* **Remove numbers** - 'hello 123 prophecy' => 'hello prophecy'\n"
                        "* **Make titlecase** - 'hello prophecy' => 'Hello Prophecy'\n"
                    )
                ]
            )
        )
        .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Checkbox("Trim whitespaces").bindProperty("trimWhiteSpace"), "1fr")
            .addColumn(Checkbox("Remove tabs, line breaks and duplicate whitespace").bindProperty(
                "removeTabsLineBreaksAndDuplicateWhitespace"), "2fr")
            .addColumn(Checkbox("Remove all whitespaces").bindProperty("allWhiteSpace"), "1fr")
        )
        .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Checkbox("Remove letters").bindProperty("cleanLetters"), "1fr")
            .addColumn(Checkbox("Remove punctuations").bindProperty("cleanPunctuations"), "2fr")
            .addColumn(Checkbox("Remove numbers").bindProperty("cleanNumbers"), "1fr")
        )
        .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(Checkbox("Make lowercase").bindProperty("makeLowercase"), "1fr")
            .addColumn(Checkbox("Make uppercase").bindProperty("makeUppercase"), "2fr")
            .addColumn(Checkbox("Make titlecase").bindProperty("makeTitlecase"), "1fr")
        ))
        return Dialog("DataCleansing") \
            .addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(relationTextBox)
                .addElement(TitleElement("Remove Null Data"))
                .addElement(nullOpSelectBox)
                .addElement(selectCol)
                .addElement(TitleElement("Replace Null values in Column"))
                .addElement(
                    Checkbox("Replace Null for String/Text fields").bindProperty("replaceNullTextFields")
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.replaceNullTextFields"),
                        BooleanExpr(True),
                    )
                    .then(
                        TextBox("Value to replace String/Text field", placeholder="NA")
                        .bindProperty("replaceNullTextWith"),
                    )
                )
                .addElement(
                    Checkbox("Replace Null for Numeric fields").bindProperty("replaceNullForNumericFields")
                )
                .addElement(
                    Condition()
                    .ifEqual(
                        PropExpr("component.properties.replaceNullForNumericFields"),
                        BooleanExpr(True),
                    )
                    .then(
                        NumberBox("Value to replace Numeric field", placeholder="0")
                        .bindProperty("replaceNullNumericWith"),
                    )
                )
                .addElement(options)
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schemaString = str(newState.ports.inputs[0].schema).replace("'", '"')
        print("trying to convert")
        print(schemaString)
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        struct_fields = [StructField(field["name"], StringType(), True) for field in fields_array]

        newProperties = dataclasses.replace(
            newState.properties, 
            schema=json.dumps(fields_array), 
            schemaColDropdownSchema = StructType(struct_fields)
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: DataCleansingProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            props.schema,
            "'" + props.null_operation + "'",
            str(props.columnNames),
            str(props.replaceNullTextFields).lower(),
            "'" + str(props.replaceNullTextWith) + "'",
            str(props.replaceNullForNumericFields).lower(),
            str(props.replaceNullNumericWith),
            str(props.trimWhiteSpace).lower(),
            str(props.removeTabsLineBreaksAndDuplicateWhitespace).lower(),
            str(props.allWhiteSpace).lower(),
            str(props.cleanLetters).lower(),
            str(props.cleanPunctuations).lower(),
            str(props.cleanNumbers).lower(),
            str(props.makeLowercase).lower(),
            str(props.makeUppercase).lower(),
            str(props.makeTitlecase).lower()
        ]


        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # Load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        print("parametersMapisHere")
        print(parametersMap)
        return DataCleansing.DataCleansingProperties(
            relation=parametersMap.get('relation')[1:-1],
            schema=parametersMap.get('schema'),
            null_operation=parametersMap.get('null_operation')[1:-1],
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            replaceNullTextFields=parametersMap.get('replaceNullTextFields').lower() == 'true',
            replaceNullTextWith=parametersMap.get('replaceNullTextWith')[1:-1],
            replaceNullForNumericFields=parametersMap.get('replaceNullForNumericFields').lower() == 'true',
            replaceNullNumericWith=float(parametersMap.get('replaceNullNumericWith')),
            trimWhiteSpace=parametersMap.get('trimWhiteSpace').lower() == 'true',
            removeTabsLineBreaksAndDuplicateWhitespace=parametersMap.get('removeTabsLineBreaksAndDuplicateWhitespace').lower() == 'true',
            allWhiteSpace=parametersMap.get('allWhiteSpace').lower() == 'true',
            cleanLetters=parametersMap.get('cleanLetters').lower() == 'true',
            cleanPunctuations=parametersMap.get('cleanPunctuations').lower() == 'true',
            cleanNumbers=parametersMap.get('cleanNumbers').lower() == 'true',
            makeLowercase=parametersMap.get('makeLowercase').lower() == 'true',
            makeUppercase=parametersMap.get('makeUppercase').lower() == 'true',
            makeTitlecase=parametersMap.get('makeTitlecase').lower() == 'true'
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # Convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("schema", properties.schema),
                MacroParameter("null_operation", properties.null_operation),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("replaceNullTextFields", str(properties.replaceNullTextFields).lower()),
                MacroParameter("replaceNullTextWith", properties.replaceNullTextWith),
                MacroParameter("replaceNullForNumericFields", str(properties.replaceNullForNumericFields).lower()),
                MacroParameter("replaceNullNumericWith", str(properties.replaceNullNumericWith)),
                MacroParameter("trimWhiteSpace", str(properties.trimWhiteSpace).lower()),
                MacroParameter("removeTabsLineBreaksAndDuplicateWhitespace", str(properties.removeTabsLineBreaksAndDuplicateWhitespace).lower()),
                MacroParameter("allWhiteSpace", str(properties.allWhiteSpace).lower()),
                MacroParameter("cleanLetters", str(properties.cleanLetters).lower()),
                MacroParameter("cleanPunctuations", str(properties.cleanPunctuations).lower()),
                MacroParameter("cleanNumbers", str(properties.cleanNumbers).lower()),
                MacroParameter("makeLowercase", str(properties.makeLowercase).lower()),
                MacroParameter("makeUppercase", str(properties.makeUppercase).lower()),
                MacroParameter("makeTitlecase", str(properties.makeTitlecase).lower())
            ],
        )


