import dataclasses
import json
from dataclasses import dataclass

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


@dataclass(frozen=True)
class ColumnExpr:
    expression: str
    format: str


@dataclass(frozen=True)
class OrderByRule:
    expression: ColumnExpr
    sortType: str = "asc"


class RecordId(MacroSpec):
    name: str = "RecordId"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Prepare"
    minNumOfInputPorts: int = 1

    @dataclass(frozen=True)
    class RecordIdProperties(MacroProperties):
        # properties for the component with default values
        method: str = "incremental_id"
        incremental_id_column_name: str = "RecordID"
        incremental_id_starting_val: int = 1000
        incremental_id_type: str = "string"
        incremental_id_size: int = 6
        position: str = "first_column"
        groupByColumnNames: List[str] = field(default_factory=list)
        relation_name: List[str] = field(default_factory=list)
        orders: List[OrderByRule] = field(default_factory=list)

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
        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    TitleElement("Generate Record Identifier")
                )
                .addElement(
                    StepContainer()
                    .addElement(
                        Step(padding="2em")
                        .addElement(
                            RadioGroup(_title="  ", orientation="horizontal")
                            .addOption("First Column", "first_column",
                                       description="Add Record Id column as first column")
                            .addOption("Last Column", "last_column", description="Add Record Id column as last column")
                            .setOptionType("button")
                            .setVariant("medium")
                            .setButtonStyle("solid")
                            .bindProperty("position")
                        )
                        .addElement(
                            Condition()
                            .ifEqual(
                                PropExpr("component.properties.method"),
                                StringExpr("incremental_id"),
                            )
                            .then(
                                StepContainer()
                                .addElement(
                                    BasicTable(
                                        "OrderByTable",
                                        height="300px",
                                        columns=[
                                            Column(  # 1st column – expression editor
                                                "Order Columns",
                                                "expression.expression",
                                                ExpressionBox(ignoreTitle=True, language="sql")
                                                .bindPlaceholders()
                                                .withSchemaSuggestions()
                                                .bindLanguage("${record.expression.format}")
                                            ),
                                            Column(  # 2nd column – ASC / DESC etc.
                                                "Sort",
                                                "sortType",
                                                SelectBox("")
                                                .addOption("Ascending Nulls First", "asc")
                                                .addOption("Ascending Nulls Last", "asc_nulls_last")
                                                .addOption("Descending Nulls First", "desc_nulls_first")
                                                .addOption("Descending Nulls Last", "desc"),
                                                width="25%"
                                            ),
                                        ],
                                    )
                                    .bindProperty("orders")
                                )
                                .addElement(
                                    SchemaColumnsDropdown("Group By Column Names")
                                    .withMultipleSelection()
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("groupByColumnNames")
                                )
                                .addElement(
                                    ColumnsLayout(gap="1rem", height="100%")
                                    .addColumn(
                                        SelectBox("Type")
                                        .addOption("String", "string")
                                        .addOption("Integer", "integet")
                                        .bindProperty("incremental_id_type")
                                    )
                                    .addColumn(
                                        Condition()
                                        .ifEqual(
                                            PropExpr("component.properties.incremental_id_type"),
                                            StringExpr("string"),
                                        )
                                        .then(
                                            NumberBox("Size",
                                                      placeholder="6",
                                                      minValueVar=0,
                                                      maxValueVar=100,
                                                      )
                                            .bindProperty("incremental_id_size")
                                        )
                                    )
                                    .addColumn(
                                    )

                                )
                                .addElement(
                                    ColumnsLayout(gap="1rem", height="100%")
                                    .addColumn(
                                        TextBox("Column Name", placeholder="RecordID").bindProperty(
                                            "incremental_id_column_name")
                                    )
                                    .addColumn(
                                        NumberBox("Starting Value",
                                                  placeholder="1000",
                                                  minValueVar=1
                                                  )
                                        .bindProperty("incremental_id_starting_val")
                                    )
                                    .addColumn(
                                    )

                                )
                            )
                            .otherwise(
                                ColumnsLayout(gap="1rem", height="100%")
                                .addColumn(
                                    TextBox("Column Name", placeholder="RecordID").bindProperty(
                                        "incremental_id_column_name")
                                )
                                .addColumn(
                                )
                                .addColumn(
                                )
                            )
                        )
                        .addElement(
                            ColumnsLayout(gap="1rem", height="100%")
                            .addColumn(
                                SelectBox("Method")
                                .addOption("UUID", "uuid")
                                .addOption("Incremental Id", "incremental_id")
                                .bindProperty("method")
                            )
                        )

                    )
                )
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(RecordId, self).validate(context, component)
        props = component.properties

        # ── 1 · OrderBy grid -----------------------------------------------------
        for idx, rule in enumerate(props.orders):
            expr_text = (rule.expression.expression or "").strip()
            if rule.sortType and expr_text == "":
                diagnostics.append(
                    Diagnostic(
                        f"properties.orders[{idx}].expression.expression",
                        "Order column expression is required when a sort direction is selected.",
                        SeverityLevelEnum.Error,
                    )
                )

        # ── 2 · Record-ID column name ------------------------------------------
        if not (props.incremental_id_column_name or "").strip():
            diagnostics.append(
                Diagnostic(
                    "properties.incremental_id_column_name",
                    "Column name for the generated Record ID must not be empty.",
                    SeverityLevelEnum.Error,
                )
            )

        # ── 3 · Group-by columns must exist in the incoming schema (case-insensitive) ──
        # put everything in upper-case once so the test is case-agnostic
        field_names_upper = {f["name"].upper() for f in component.ports.inputs[0].schema["fields"]}

        if props.groupByColumnNames:
            # list-comprehension keeps original spelling but tests in UPPER
            missing = [c for c in props.groupByColumnNames if c.upper() not in field_names_upper]

            if missing:
                diagnostics.append(
                    Diagnostic(
                        "properties.groupByColumnNames",
                        f"Group-by column(s) not found in input schema: {', '.join(missing)}",
                        SeverityLevelEnum.Error,
                    )
                )

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: RecordIdProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # keep only rows where the user typed something

        order_rules: List[dict] = [
            {"expr": expr, "sort": r.sortType}
            for r in props.orders
            for expr in [(r.expression.expression or "").strip()]  # temp var
            if expr  # keep non-empty
        ]

        arguments = [
            "'" + table_name + "'",
            "'" + str(props.method) + "'",
            "'" + str(props.incremental_id_column_name) + "'",
            "'" + str(props.incremental_id_type) + "'",
            str(props.incremental_id_size),
            str(props.incremental_id_starting_val),
            "'" + str(props.position) + "'",
            str(props.groupByColumnNames),
            str(order_rules)
        ]

        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return RecordId.RecordIdProperties(
            relation_name=parametersMap.get('relation_name'),
            method=parametersMap.get('method'),
            incremental_id_column_name=parametersMap.get('incremental_id_column_name'),
            incremental_id_type=parametersMap.get('incremental_id_type'),
            incremental_id_size=float(parametersMap.get('incremental_id_size')),
            incremental_id_starting_val=float(parametersMap.get('incremental_id_starting_val')),
            position=parametersMap.get('position'),
            groupByColumnNames=json.loads(parametersMap.get('groupByColumnNames').replace("'", '"')),
            orders=parametersMap.get('orders')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("method", properties.method),
                MacroParameter("incremental_id_column_name", properties.incremental_id_column_name),
                MacroParameter("incremental_id_type", properties.incremental_id_type),
                MacroParameter("incremental_id_size", str(properties.incremental_id_size)),
                MacroParameter("incremental_id_starting_val", str(properties.incremental_id_starting_val)),
                MacroParameter("position", properties.position),
                MacroParameter("groupByColumnNames", json.dumps(properties.groupByColumnNames)),
                MacroParameter("orders", str(properties.orders))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)