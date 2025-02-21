
from dataclasses import dataclass


from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class XMLParse(MacroSpec):
    name: str = "XMLParse"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class XMLParseProperties(MacroProperties):
        # properties for the component with default values
        parameter1: str = "'default_value_of_parameter1'"

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
                    TextBox("Table Name")
                    .bindPlaceholder("Configure table name")
                    .bindProperty("parameter1")
                )
           )
       )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        return newState

    def apply(self, props: XMLParseProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        non_empty_param = ",".join([param for param in [props.parameter1] if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return XMLParse.XMLParseProperties(
            parameter1=parametersMap.get('parameter1')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("parameter1", properties.parameter1)
            ],
        )


