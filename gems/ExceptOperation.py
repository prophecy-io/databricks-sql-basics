
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class ExceptOperation(MacroSpec):
    name: str = "ExceptOperation"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Join/Split"


    @dataclass(frozen=True)
    class ExceptOperationProperties(MacroProperties):
        # properties for the component with default values
        operationType: str = ""
        relation: str = ""

    def dialog(self) -> Dialog:
        return Dialog("ExceptOperations").addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
            .addColumn(
                Ports(minInputPorts = 2).editableInput(True), "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    TextBox("Dataframe names seperated with Comma").bindPlaceholder("in0,in1").bindProperty("relation")
                )
                .addElement(
                    RadioGroup("Operation Type")
                    .addOption(
                        "Except",
                        "except",
                        ("Except"),
                        ("Returns the rows in subquery1 which are not in subquery2 while removing duplicates.")
                    )
                    .addOption(
                        "Except All",
                        "exceptAll",
                        ("ExceptAll"),
                        ("If Except All is specified, each row in subquery2 will remove exactly one of possibly multiple matches from subquery1.")
                    )
                    .setOptionType("button")
                    .setVariant("large")
                    .setButtonStyle("solid")
                    .bindProperty("operationType")),
                "2fr"
            )
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(ExceptOperation, self).validate(context, component)
        props = component.properties

        if len(component.properties.relation) == 0:
            diagnostics.append(
                Diagnostic("component.properties.relation", "Please enter the relation names", SeverityLevelEnum.Error))

        if len(component.properties.operationType) == 0:
            diagnostics.append(
                Diagnostic("component.properties.operationType", "Please select the operation type", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        return newState

    def apply(self, props: ExceptOperationProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            "'" + props.operationType + "'"
        ]


        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return ExceptOperation.ExceptOperationProperties(
            relation=parametersMap.get('relation'),
            operationType=parametersMap.get('operationType')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("operationType", properties.operationType)
            ],
        )