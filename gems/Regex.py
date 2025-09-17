
import dataclasses
from dataclasses import dataclass, field
import json
import re

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


@dataclass(frozen=True)
class ColumnParse:
    columnName: str
    dataType: str
    rgxExpression: str


class Regex(MacroSpec):
    name: str = "Regex"
    projectName: str = "Gem_creator"
    category: str = "Transform"
    minNumOfInputPorts: int = 1

    @dataclass(frozen=True)
    class RegexProperties(MacroProperties):
        # properties for the component with default values
        selectedColumnName: str = ""
        schema: str = ''
        relation_name: List[str] = field(default_factory=list)
        regexExpression: str = ""
        caseInsensitive: bool = False
        allowBlankTokens: bool = False
        outputMethod: str = "replace"
        # Replace
        replacementText: Optional[str] = ""
        copyUnmatchedText: bool = False
        # Tokenize
        tokenizeOutputMethod: str = "splitColumns"
        noOfColumns: int = 1
        extraColumnsHandling: str = "dropExtraWithWarning"
        splitRowsColumnName: str = "generated_column"
        outputRootName: str = "generated"
        # Parse
        parseColumns: List[ColumnParse] = field(default_factory=list)
        # Match
        matchColumnName: str = "regex_match"
        errorIfNotMatched: bool = False


    def dialog(self) -> Dialog:
        return Dialog("MacroRegex").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    StackLayout(height="100%")
                    .addElement(
                        TitleElement("Select Column to Split")
                    )
                    .addElement(
                        StepContainer()
                        .addElement(
                            Step()
                                .addElement(
                                    StackLayout(height="100%")
                                        .addElement(
                                            SchemaColumnsDropdown("", appearance = "minimal")
                                            .bindSchema("component.ports.inputs[0].schema")
                                            .bindProperty("selectedColumnName")
                                            .showErrorsFor("selectedColumnName")
                                        )
                                )
                        )
                    )
                    .addElement(
                        StepContainer()
                            .addElement(
                                Step()
                                    .addElement(
                                        StackLayout(height="100%")
                                            .addElement(TitleElement("Regex"))
                                            .addElement(
                                                TextBox("").bindPlaceholder("Regex Expression").bindProperty("regexExpression")
                                            )
                                            .addElement(
                                                Checkbox("Case Insensitive Matching").bindProperty("caseInsensitive")
                                            )
                                            .addElement(
                                                AlertBox(
                                                    variant="success",
                                                    _children=[
                                                        Markdown(
                                                            "**Common Regex Pattern Examples:**"
                                                            "\n"
                                                            "- **Email extraction:**"
                                                            "\n"
                                                            "**Pattern:** `([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})`"
                                                            "\n"
                                                            "Example: Extract user and domain from john.doe@company.com"
                                                            "\n"
                                                            "- **Phone number parsing:**"
                                                            "\n"
                                                            "**Pattern:** `(\d{3})-(\d{3})-(\d{4})`"
                                                            "\n"
                                                            "Example: Parse (555)-123-4567 into area code, exchange, number"
                                                            "\n"
                                                            "- **Date extraction (MM/DD/YYYY):**"
                                                            "\n"
                                                            "**Pattern:** `(\d{1,2})/(\d{1,2})/(\d{4})`"
                                                            "\n"
                                                            "Example: Extract 12/25/2023 into month, day, year"
                                                            "\n"
                                                            "- **Word tokenization:**"
                                                            "\n"
                                                            "**Pattern:** `([^\s]+)`"
                                                            "\n"
                                                            "Example: Split \"Hello World Example\" into individual words"
                                                            "\n"
                                                            "- **Comma-separated values:**"
                                                            "\n"
                                                            "**Pattern:** `([^,]+)`"
                                                            "\n"
                                                            "Example: Split \"Value1,Value2,Value3\" into separate values"
                                                            "\n"
                                                            "- **Extract numbers only:**"
                                                            "\n"
                                                            "**Pattern:** `(\d+)`"
                                                            "\n"
                                                            "Example: Extract 123 from \"Price: $123.45\""
                                                            "\n"
                                                            "- **URL components:**"
                                                            "\n"
                                                            "**Pattern:** `https?://([^/]+)(/.*)?`"
                                                            "\n"
                                                            "Example: Extract domain and path from URLs"
                                                            "\n"
                                                            "- **Remove special characters:**"
                                                            "\n"
                                                            "**Pattern:** `[^a-zA-Z0-9\s]`"
                                                            "\n"
                                                            "Example: Remove punctuation, keep letters, numbers, spaces"
                                                            "\n"
                                                            "- **Match uppercase words:**"
                                                            "\n"
                                                            "**Pattern:** `\b[A-Z]{2,}\b`"
                                                            "\n"
                                                            "Example: Find acronyms like \"USA\", \"API\", \"SQL\""
                                                            "\n"
                                                            "- **Extract text between quotes:**"
                                                            "\n"
                                                            "**Pattern:** `([^\"]*)`"
                                                            "\n"
                                                            "Example: Extract content from \"quoted text\""
                                                        )
                                                    ]
                                                )
                                            )
                                    )
                            )
                            .addElement(
                                RadioGroup("Output Method")
                                .addOption(
                                    "Replace",
                                    "replace",
                                    ("Replace Matched Text with Replacement Text")
                                )
                                .addOption(
                                    "Tokenize",
                                    "tokenize",
                                    ("Split the incoming data using a regular expression")
                                )
                                .addOption(
                                    "Parse",
                                    "parse",
                                    ("Separate the expression into new columns, and set the Name, Type, and Size of the new columns")
                                )
                                .addOption(
                                    "Match",
                                    "match",
                                    ("Append a column containing a number: 1 if the expression matched, 0 if it did not.")
                                )
                                .bindProperty("outputMethod")
                            )
                    )
                    .addElement(
                        StepContainer()
                            .addElement(
                                # Replace Method Configuration
                                Condition()
                                .ifEqual(PropExpr("component.properties.outputMethod"), StringExpr("replace"))
                                .then(
                                    Step()
                                        .addElement(
                                            StackLayout(height="100%")
                                                .addElement(TitleElement("Replace Configuration"))
                                                .addElement(
                                                    TextBox("Replacement Text")
                                                    .bindPlaceholder("Enter replacement expression")
                                                    .bindProperty("replacementText")
                                                )
                                                .addElement(
                                                    Checkbox("Copy Unmatched Text to Output")
                                                    .bindProperty("copyUnmatchedText")
                                                )
                                        )
                                ).otherwise(
                                    # Tokenize Method Configuration
                                    Condition()
                                    .ifEqual(PropExpr("component.properties.outputMethod"), StringExpr("tokenize"))
                                    .then(
                                        Step()
                                            .addElement(
                                                StackLayout(height="100%")
                                                    .addElement(TitleElement("Tokenize Configuration"))
                                                    .addElement(
                                                        RadioGroup("Select Split Strategy")
                                                        .addOption("Split to columns", "splitColumns")
                                                        .addOption("Split to rows", "splitRows")
                                                        .bindProperty("tokenizeOutputMethod")
                                                    ).addElement(
                                                        Checkbox("Allow Blank Tokens").bindProperty("allowBlankTokens")
                                                    )
                                                    .addElement(
                                                        Condition()
                                                        .ifEqual(PropExpr("component.properties.tokenizeOutputMethod"), StringExpr("splitColumns"))
                                                        .then(
                                                            StackLayout(height="100%")
                                                            .addElement(
                                                                ColumnsLayout(gap="1rem", height="100%")
                                                                .addColumn(
                                                                    NumberBox("Number of columns", placeholder=1, requiredMin=1)
                                                                    .bindProperty("noOfColumns")
                                                                )
                                                                .addColumn(
                                                                    SelectBox(titleVar="For Extra Columns")
                                                                    .addOption("Drop Extra with Warning", "dropExtraWithWarning")
                                                                    .addOption("Drop Extra without Warning", "dropExtraWithoutWarning")
                                                                    .addOption("Error", "error")
                                                                    .bindProperty("extraColumnsHandling")
                                                                )
                                                            )
                                                            .addElement(
                                                                TextBox("Output Root Name")
                                                                .bindPlaceholder("Enter Generated Column Suffix")
                                                                .bindProperty("outputRootName")
                                                            )
                                                        )
                                                    )
                                            )
                                    ).otherwise(
                                        # Parse Method Configuration
                                        Condition()
                                        .ifEqual(PropExpr("component.properties.outputMethod"), StringExpr("parse"))
                                        .then(
                                            Step()
                                                .addElement(
                                                    StackLayout(height="100%")
                                                        .addElement(TitleElement("Parse Configuration"))
                                                        .addElement(
                                                            StackLayout(height="100%")
                                                            .addElement(
                                                                AlertBox(
                                                                    variant="info",
                                                                    _children=[
                                                                        Markdown("Configure the output columns for parsed groups. Each capture group in your regex will create a new column.")
                                                                    ]
                                                                )
                                                            )
                                                            .addElement(
                                                                StepContainer()
                                                                    .addElement(
                                                                        Step().addElement(
                                                                            BasicTable("Parse Columns Table", height="200px", columns=[
                                                                                Column(
                                                                                    "New Column Name",
                                                                                    "columnName",
                                                                                    TextBox("")
                                                                                        .bindPlaceholder("Column Name")
                                                                                ),
                                                                                Column(
                                                                                    "Select Data Type",
                                                                                    "dataType",
                                                                                    SelectBox("")
                                                                                        .addOption("String", "string")
                                                                                        .addOption("Integer", "int")
                                                                                        .addOption("Double", "double")
                                                                                        .addOption("Boolean", "bool")
                                                                                        .addOption("Date", "date")
                                                                                        .addOption("DateTime", "datetime")
                                                                                        .bindProperty("record.dataType"),
                                                                                    width="20%"
                                                                                ),
                                                                                Column(
                                                                                    "Regex Expression",
                                                                                    "rgxExpression",
                                                                                    TextBox("", disabledView=True)
                                                                                        .bindPlaceholder("Auto-generated from regex groups"),
                                                                                    width="35%"
                                                                                )
                                                                            ])
                                                                            .bindProperty("parseColumns")
                                                                        )
                                                                    )
                                                            )
                                                        )
                                                )
                                        ).otherwise(
                                            # Match Method Configuration
                                            Condition()
                                            .ifEqual(PropExpr("component.properties.outputMethod"), StringExpr("match"))
                                            .then(
                                                Step()
                                                    .addElement(
                                                        StackLayout(height="100%")
                                                            .addElement(TitleElement("Match Configuration"))
                                                            .addElement(
                                                                TextBox("Column name for match status")
                                                                .bindPlaceholder("Enter name for match result column")
                                                                .bindProperty("matchColumnName")
                                                            )
                                                            .addElement(
                                                                Checkbox("Error if not Matched")
                                                                .bindProperty("errorIfNotMatched")
                                                            )
                                                            .addElement(
                                                                AlertBox(
                                                                    variant="info",
                                                                    _children=[
                                                                        Markdown("This will add a new column containing 1 if the regex matched, 0 if it did not match.")
                                                                    ]
                                                                )
                                                            )
                                                    )
                                            )
                                        )
                                    )
                                )
                            )
                    )
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
        diagnostics = super().validate(context, component)
        props = component.properties

        # Check if columnName is provided
        if not hasattr(props, 'selectedColumnName') or not props.selectedColumnName or len(props.selectedColumnName.strip()) == 0:
            diagnostics.append(
                Diagnostic("component.properties.selectedColumnName", "Column Name is required and cannot be empty", SeverityLevelEnum.Error))

        # Check if regexExpression is provided
        if not hasattr(props, 'regexExpression') or not props.regexExpression or len(props.regexExpression.strip()) == 0:
            diagnostics.append(
                Diagnostic("component.properties.regexExpression", "Regex Expression is required and cannot be empty", SeverityLevelEnum.Error))

        # Validate that columnName exists in input schema
        if (hasattr(props, 'selectedColumnName') and props.selectedColumnName and
            hasattr(props, 'schema') and props.schema):
            if props.selectedColumnName not in props.schema:
                diagnostics.append(
                    Diagnostic("component.properties.selectedColumnName", f"Selected column '{props.selectedColumnName}' is not present in input schema.", SeverityLevelEnum.Error))

        return diagnostics

    def extract_capturing_groups(self, pattern):
        """Extract individual capturing group patterns from a regex string."""
        if not pattern:
            return []

        groups = []
        i = 0

        while i < len(pattern):
            if pattern[i] == '(' and (i == 0 or pattern[i-1] != '\\'):
                # Skip non-capturing groups (?:...) or other special groups (?=...), (?!...), etc.
                if i + 1 < len(pattern) and pattern[i+1] == '?':
                    # Find the end of this non-capturing group and skip it
                    paren_count = 1
                    j = i + 1
                    while j < len(pattern) and paren_count > 0:
                        if pattern[j] == '\\' and j + 1 < len(pattern):
                            j += 2
                            continue
                        elif pattern[j] == '(':
                            paren_count += 1
                        elif pattern[j] == ')':
                            paren_count -= 1
                        j += 1
                    i = j
                    continue

                # This is a capturing group - find its end
                start = i
                paren_count = 1
                j = i + 1

                while j < len(pattern) and paren_count > 0:
                    if pattern[j] == '\\' and j + 1 < len(pattern):
                        j += 2
                        continue
                    elif pattern[j] == '(':
                        paren_count += 1
                    elif pattern[j] == ')':
                        paren_count -= 1
                    j += 1

                if paren_count == 0:
                    group = pattern[start:j]
                    groups.append(group)
                i = j
            else:
                i += 1

        return groups

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        # Generate ColumnParse objects based on regex capturing groups
        parse_columns = []
        if hasattr(newState.properties, 'regexExpression') and newState.properties.regexExpression:
            try:
                # Validate regex first
                compiled_regex = re.compile(newState.properties.regexExpression)

                # Extract individual capturing group patterns
                group_patterns = self.extract_capturing_groups(newState.properties.regexExpression)

                # Get existing parseColumns if they exist
                existing_parse_columns = getattr(newState.properties, 'parseColumns', [])

                # Create ColumnParse objects for each capturing group
                for i, group_pattern in enumerate(group_patterns):
                    if i < len(existing_parse_columns):
                        # Preserve existing configuration, update regex expression
                        existing_col = existing_parse_columns[i]
                        parse_columns.append(ColumnParse(
                            columnName=existing_col.columnName,
                            dataType=existing_col.dataType,
                            rgxExpression=group_pattern
                        ))
                    else:
                        # Create new column with smart defaults
                        parse_columns.append(ColumnParse(
                            columnName=f"regex_col{i+1}",
                            dataType=self.infer_data_type_from_pattern(group_pattern),
                            rgxExpression=group_pattern
                        ))

            except re.error:
                # If regex is invalid, preserve existing parseColumns if any
                parse_columns = getattr(newState.properties, 'parseColumns', [])

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
            parseColumns=parse_columns
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: RegexProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)
        parseColumnsJson = json.dumps([{
                    "columnName": fld.columnName,
                    "dataType": fld.dataType,
                    "rgxExpression": fld.rgxExpression
                }
                for fld in props.parseColumns
            ]
        )

        parameter_list = [
            table_name,
            props.schema,
            props.selectedColumnName,
            props.regexExpression,
            props.outputMethod,
            props.caseInsensitive,
            props.allowBlankTokens,
            props.replacementText,
            props.copyUnmatchedText,
            props.tokenizeOutputMethod,
            props.noOfColumns,
            props.extraColumnsHandling,
            props.outputRootName,
            parseColumnsJson,
            props.matchColumnName,
            props.errorIfNotMatched,
        ]
        param_list_clean = []
        for p in parameter_list:
            if type(p) == str:
                param_list_clean.append("'" + p + "'")
            else:
                param_list_clean.append(str(p))
        non_empty_param = ",".join([param for param in param_list_clean if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        parseColumns = [
                ColumnParse(
                    columnName = fld.get("columnName"),
                    dataType = fld.get("dataType"),
                    rgxExpression = fld.get("rgxExpression")
                )
                for fld in json.loads(parametersMap.get('parseColumns', '[]'))
            ]
        return Regex.RegexProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            selectedColumnName=parametersMap.get('selectedColumnName'),
            regexExpression=parametersMap.get('regexExpression'),
            outputMethod=parametersMap.get('outputMethod'),
            caseInsensitive=bool(parametersMap.get('caseInsensitive')),
            allowBlankTokens=bool(parametersMap.get('allowBlankTokens')),
            replacementText=parametersMap.get('replacementText'),
            copyUnmatchedText=parametersMap.get('copyUnmatchedText'),
            tokenizeOutputMethod=parametersMap.get('tokenizeOutputMethod'),
            noOfColumns=int(parametersMap.get('noOfColumns')),
            extraColumnsHandling=parametersMap.get('extraColumnsHandling'),
            outputRootName=parametersMap.get('outputRootName'),
            parseColumns=parseColumns,
            matchColumnName=parametersMap.get('matchColumnName'),
            errorIfNotMatched=bool(parametersMap.get('errorIfNotMatched')),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        parseColumnsJson = json.dumps([{
                    "columnName": fld.columnName,
                    "dataType": fld.dataType,
                    "rgxExpression": fld.rgxExpression
                }
                for fld in properties.parseColumns
            ]
        )
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("selectedColumnName", str(properties.selectedColumnName)),
                MacroParameter("outputMethod", str(properties.outputMethod)),
                MacroParameter("regexExpression", str(properties.regexExpression)),
                MacroParameter("caseInsensitive", str(properties.caseInsensitive)),
                MacroParameter("replacementText", str(properties.replacementText)),
                MacroParameter("copyUnmatchedText", str(properties.copyUnmatchedText)),
                MacroParameter("tokenizeOutputMethod", str(properties.tokenizeOutputMethod)),
                MacroParameter("allowBlankTokens", str(properties.allowBlankTokens)),
                MacroParameter("noOfColumns", str(properties.noOfColumns)),
                MacroParameter("extraColumnsHandling", str(properties.extraColumnsHandling)),
                MacroParameter("outputRootName", str(properties.outputRootName)),
                MacroParameter("parseColumns", parseColumnsJson),
                MacroParameter("matchColumnName", str(properties.matchColumnName)),
                MacroParameter("errorIfNotMatched", str(properties.errorIfNotMatched)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)

