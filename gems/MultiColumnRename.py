from dataclasses import dataclass
import dataclasses

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.ComponentBuilderBase import Diagnostic, SeverityLevelEnum
from prophecy.cb.server.base.ComponentBuilderBase import *

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField

import json

class MultiColumnRename(MacroSpec):
    name: str = "MultiColumnRename"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Transform"


    @dataclass(frozen=True)
    class MultiColumnRenameProperties(MacroProperties):
        # properties for the component with default values
        schema: Optional[StructType] = StructType([])
        columnNames: List[str] = field(default_factory=list)
        renameMethod: str = ""
        editOperation: str = ""
        editType: str = ""
        editWith: str = ""
        suffix: str = ""
        customExpression: str = ""
        relation: str = ""

    def dialog(self) -> Dialog:
        horizontalDivider = HorizontalDivider()
        relationTextBox = TextBox("Table name").bindPlaceholder("in0").bindProperty("relation")
        renameMethod = SelectBox("")\
                        .addOption("Edit prefix/suffix", "editPrefixSuffix")\
                        .addOption("Advanced rename", "advancedRename")\
                        .bindProperty("renameMethod")
        
        dialog = Dialog("MultiColumnRename")\
            .addElement(
                ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(allowInputAddOrDelete=True),"content")
                .addColumn(
                    StackLayout(height="100%")
                    .addElement(horizontalDivider)
                    .addElement(relationTextBox)
                    .addElement(horizontalDivider)
                    .addElement(TitleElement("Select columns to rename"))
                    .addElement(
                        SchemaColumnsDropdown("")
                        .withMultipleSelection()
                        .bindSchema("schema")
                        .bindProperty("columnNames")
                    )
                    .addElement(horizontalDivider)
                    .addElement(TitleElement("Rename method"))
                    .addElement(renameMethod)
                    .addElement(
                        Condition()
                        .ifEqual(
                            PropExpr("component.properties.renameMethod"), 
                            StringExpr("editPrefixSuffix")
                        )
                        .then(
                            ColumnsLayout(gap="1rem", height="100%")
                            .addColumn(
                                ColumnsLayout(gap="1rem", height="100%")
                                .addElement(
                                    SelectBox("").addOption("Add", "Add").bindProperty("editOperation")
                                )
                                .addElement(
                                    SelectBox("").addOption("Prefix", "Prefix").addOption("Suffix", "Suffix").bindProperty("editType")
                                )
                                .addElement(
                                    TextBox("").bindPlaceholder("NEW_").bindProperty("editWith")
                                )                                                                
                            )
                        )
                    )
                    .addElement(
                        Condition()
                        .ifEqual(
                            PropExpr("component.properties.renameMethod"), 
                            StringExpr("advancedRename")
                        )
                        .then(
                            ColumnsLayout(gap=("1rem"), height=("100%"))
                            .addColumn(
                                StackLayout(height="100%")
                                .addElement(
                                    ExpressionBox("Output Expression")
                                    .bindPlaceholder("Write spark sql expression considering `column_name` as column name string literal. Example:\n For column name: upper(column_name)")
                                    .withSchemaSuggestions()
                                    .withCopilotEnabledExpression()
                                    .bindProperty("customExpression")
                                    )
                                )
                            )
                    )
                )
            )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(MultiColumnRename, self).validate(context,component)
        props = component.properties

        if len(component.properties.columnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.columnNames", "Select Column to Rename", SeverityLevelEnum.Error))            

        if len(component.properties.renameMethod) == 0:
            diagnostics.append(
                Diagnostic("component.properties.renameMethod", "Select Rename Method", SeverityLevelEnum.Error))  

        if (
                component.properties.renameMethod == "editPrefixSuffix" and 
                (
                    len(component.properties.editOperation) == 0 or 
                    len(component.properties.editType) == 0 or  
                    len(component.properties.editWith) == 0
                )
            ):
            diagnostics.append(
                Diagnostic("component.properties.renameMethod", "Missing Properties for Edit prefix/suffix Operation", SeverityLevelEnum.Error))

        if (component.properties.renameMethod == "advancedRename" and len(component.properties.customExpression) == 0):
            diagnostics.append(
                Diagnostic("component.properties.advancedRename", "Missing Properties for Advanced rename", SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        portSchema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in portSchema["fields"]]
        struct_fields = [StructField(field["name"], StructType(), True) for field in fields_array]

        newProperties = dataclasses.replace(
            newState.properties, 
            schema = StructType(struct_fields)
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: MultiColumnRenameProperties) -> str:
        # Get existing column names
        column_names = [field.name for field in props.schema.fields]

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            "'" + props.relation + "'",
            str(column_names),
            str(props.columnNames),
            "'" + str(props.renameMethod) + "'",
            "'" + str(props.editOperation) + "'",
            "'" + str(props.editType) + "'",
            "'" + str(props.editWith) + "'",
            '"' + str(props.customExpression) + '"'
        ]
        non_empty_param = ",".join([param for param in arguments if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:

        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return MultiColumnRename.MultiColumnRenameProperties(
            relation=parametersMap.get('relation'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            renameMethod=parametersMap.get('renameMethod'),
            editOperation=parametersMap.get('editOperation'),
            editType=parametersMap.get('editType'),
            editWith=parametersMap.get('editWith'),
            customExpression=parametersMap.get('customExpression')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", properties.relation),
                MacroParameter("columnNames", json.dumps(properties.columnNames)),
                MacroParameter("renameMethod", properties.renameMethod),
                MacroParameter("editOperation", properties.editOperation),
                MacroParameter("editType", properties.editType),
                MacroParameter("editWith", properties.editWith),
                MacroParameter("customExpression", properties.customExpression)
            ],
        )