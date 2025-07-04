
from dataclasses import dataclass

import dataclasses
import json
from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
import copy

class DataEncoder(MacroSpec):
    name: str = "DataEncoder"
    projectName: str = "DatabricksSqlBasics"
    category: str = "Custom"
    minNumOfInputPorts: int = 1


    @dataclass(frozen=True)
    class DataEncoderProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ''
        columnNames: List[str] = field(default_factory=list)
        enc_dec_method: str = ""
        enc_dec_charSet: str = ""
        aes_enc_dec_key: str = ""
        aes_enc_dec_mode: str = ""
        aes_encrypt_aad: str = ""
        aes_encrypt_iv: str = ""

    def dialog(self) -> Dialog:
        aes_encrypt_condition = Condition().ifEqual(
            PropExpr("component.properties.enc_dec_method"), StringExpr("aes_encrypt")
        )

        aes_decrypt_condition = Condition().ifEqual(
            PropExpr("component.properties.enc_dec_method"), StringExpr("aes_decrypt")
        )

        try_aes_decrypt_condition = Condition().ifEqual(
            PropExpr("component.properties.enc_dec_method"), StringExpr("try_aes_decrypt")
        )

        encode_condition = Condition().ifEqual(
            PropExpr("component.properties.enc_dec_method"), StringExpr("encode")
        )

        decode_condition = Condition().ifEqual(
            PropExpr("component.properties.enc_dec_method"), StringExpr("decode")
        )

        encode_decode_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(SelectBox("charSet").bindProperty("enc_dec_charSet").withDefault("UTF-8")
                        .addOption("'US-ASCII': Seven-bit ASCII, ISO646-US", "US-ASCII")
                        .addOption("'ISO-8859-1': ISO Latin Alphabet No. 1, ISO-LATIN-1", "ISO-8859-1")
                        .addOption("'UTF-8': Eight-bit UCS Transformation Format", "UTF-8")
                        .addOption("'UTF-16BE': Sixteen-bit UCS Transformation Format, big-endian byte order", "UTF-16BE")
                        .addOption("'UTF-16LE': Sixteen-bit UCS Transformation Format, little-endian byte order", "UTF-16LE")
                        .addOption("'UTF-16': Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark", "UTF-16")
                        )
        )

        try_aes_decrypt_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("key").bindProperty("aes_enc_dec_key").bindPlaceholder(""))
            .addElement(SelectBox("mode").bindProperty("aes_enc_dec_mode").withDefault("GCM")
                        .addOption("Galois/Counter Mode (GCM)", "GCM")
                        .addOption("Electronic CodeBook (ECB)", "ECB")
                        )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.aes_enc_dec_mode"), StringExpr("GCM")).then(
                    TextBox("authenticated additional data(aad)").bindProperty("aes_encrypt_aad").bindPlaceholder("")
                )
            )
        )

        aes_decrypt_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("key").bindProperty("aes_enc_dec_key").bindPlaceholder(""))
            .addElement(SelectBox("mode").bindProperty("aes_enc_dec_mode").withDefault("GCM")
                        .addOption("Galois/Counter Mode (GCM)", "GCM")
                        .addOption("Cipher-Block Chaining (CBC)", "CBC")
                        .addOption("Electronic CodeBook (ECB)", "ECB")
                        )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.aes_enc_dec_mode"), StringExpr("GCM")).then(
                    TextBox("authenticated additional data(aad)").bindProperty("aes_encrypt_aad").bindPlaceholder("")
                )
            )
        )


        aes_encrypt_params_ui = (
            StackLayout(gap="1rem", height="100%",direction="vertical", width="100%")
            .addElement(TextBox("key").bindProperty("aes_enc_dec_key").bindPlaceholder(""))
            .addElement(SelectBox("mode").bindProperty("aes_enc_dec_mode").withDefault("GCM")
                        .addOption("Galois/Counter Mode (GCM)", "GCM")
                        .addOption("Cipher-Block Chaining (CBC)", "CBC")
                        .addOption("Electronic CodeBook (ECB)", "ECB")
                        )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.aes_enc_dec_mode"), StringExpr("GCM")).then(
                    TextBox("authenticated additional data(aad)").bindProperty("aes_encrypt_aad").bindPlaceholder("")
                )
            )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.aes_enc_dec_mode"), StringExpr("GCM")).then(
                    TextBox("initialization vector(iv)").bindProperty("aes_encrypt_iv").bindPlaceholder("")
                )
            )
            .addElement(
                Condition().ifEqual(PropExpr("component.properties.aes_enc_dec_mode"), StringExpr("CBC")).then(
                    TextBox("initialization vector(iv)").bindProperty("aes_encrypt_iv").bindPlaceholder("")
                )
            )
        )

        dialog = Dialog("encoder_decoder").addElement(
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
                            .addElement(TitleElement("Select columns to encode"))
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
                    SelectBox("Choose your encoding/decoding method")
                    .bindProperty("enc_dec_method")
                    .withStyle({"width": "100%"})
                    .withDefault("")
                    .addOption("aes_encrypt", "aes_encrypt")
                    .addOption("aes_decrypt", "aes_decrypt")
                    .addOption("try_aes_decrypt", "try_aes_decrypt")
                    .addOption("base64", "base64")
                    .addOption("unbase64", "unbase64")
                    .addOption("hex", "hex")
                    .addOption("unhex", "unhex")
                    .addOption("encode", "encode")
                    .addOption("decode", "decode")
                )
                .addElement(
                    aes_encrypt_condition.then(
                        aes_encrypt_params_ui
                    )
                )
                .addElement(
                    aes_decrypt_condition.then(
                        aes_decrypt_params_ui
                    )
                )
                .addElement(
                    try_aes_decrypt_condition.then(
                        try_aes_decrypt_params_ui
                    )
                )
                .addElement(
                    encode_condition.then(
                        encode_decode_params_ui
                    )
                )
                .addElement(
                    decode_condition.then(
                        encode_decode_params_ui
                    )
                )

            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(DataEncoder, self).validate(context, component)
        enc_dec_method = component.properties.enc_dec_method
        if len(component.properties.columnNames) > 0 :
            missingKeyColumns = [col for col in component.properties.columnNames if
                                 col not in component.properties.schema]
            if missingKeyColumns:
                diagnostics.append(
                    Diagnostic("component.properties.columnNames", f"Selected columns {missingKeyColumns} are not present in input schema.", SeverityLevelEnum.Error)
                )
        if enc_dec_method == "aes_encrypt":
            key = component.properties.aes_enc_dec_key
            if len(key.encode('utf-8')) not in (16, 24, 32):
                diagnostics.append(
                    Diagnostic("component.properties.aes_enc_dec_key", f"length on key should be one of 16, 24, 32 bytes length. Right now length is {len(key.encode('utf-8'))}", SeverityLevelEnum.Error)
                )
            mode = component.properties.aes_enc_dec_mode
            if mode in ('GCM', 'CBC', 'ECB'):
                iv = component.properties.aes_encrypt_iv
                if mode == 'GCM' and len(iv.encode('utf-8')) != 12:
                    diagnostics.append(
                        Diagnostic("component.properties.aes_encrypt_iv", f"length on initialization vector,iv should be of 12 bytes for GCM mode. Right now length is {len(iv.encode('utf-8'))}", SeverityLevelEnum.Error)
                    )
                elif mode == 'CBC' and len(iv.encode('utf-8')) != 16:
                    diagnostics.append(
                        Diagnostic("component.properties.aes_encrypt_iv", f"length on initialization vector,iv should be of 16 bytes for CBC mode. Right now length is {len(iv.encode('utf-8'))}", SeverityLevelEnum.Error)
                    )
                elif mode == 'ECB' and len(iv.encode('utf-8')) > 0:
                    diagnostics.append(
                        Diagnostic("component.properties.aes_encrypt_iv", f"length on initialization vector,iv should be of 0 bytes for ECB mode. Right now length is {len(iv.encode('utf-8'))} AND IV = {iv}", SeverityLevelEnum.Error)
                    )

        elif enc_dec_method in ("aes_decrypt", "try_aes_decrypt"):
            key = component.properties.aes_enc_dec_key
            if len(key.encode('utf-8')) not in (16, 24, 32):
                diagnostics.append(
                    Diagnostic("component.properties.aes_enc_dec_key", f"length on key should be one of 16, 24, 32 bytes length. Right now length is {len(key.encode('utf-8'))}", SeverityLevelEnum.Error)
                )
        return diagnostics


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

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        old_enc_method, old_enc_mode = oldState.properties.enc_dec_method, oldState.properties.aes_enc_dec_mode
        new_enc_method, new_enc_mode = newState.properties.enc_dec_method, newState.properties.aes_enc_dec_mode

        if old_enc_method != new_enc_method or old_enc_mode != new_enc_mode:
            aes_encrypt_iv, aes_encrypt_aad = "", ""
        else:
            aes_encrypt_iv, aes_encrypt_aad = newState.properties.aes_encrypt_iv, newState.properties.aes_encrypt_aad

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name,
            aes_encrypt_aad=aes_encrypt_aad,
            aes_encrypt_iv=aes_encrypt_iv
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: DataEncoderProperties) -> str:
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
            "'" + table_name + "'",
            safe_str(props.columnNames),
            safe_str(props.enc_dec_method),
            safe_str(props.enc_dec_charSet),
            safe_str(props.aes_enc_dec_key),
            safe_str(props.aes_enc_dec_mode),
            safe_str(props.aes_encrypt_aad),
            safe_str(props.aes_encrypt_iv)
        ]

        params = ",".join(arguments)
        return f"{{{{ {resolved_macro_name}({params}) }}}}"

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return DataEncoder.DataEncoderProperties(
            relation_name=parametersMap.get('relation_name'),
            schema=parametersMap.get('schema'),
            columnNames=json.loads(parametersMap.get('columnNames').replace("'", '"')),
            enc_dec_method=parametersMap.get('enc_dec_method'),
            enc_dec_charSet=parametersMap.get('enc_dec_charSet'),
            aes_enc_dec_key=parametersMap.get('aes_enc_dec_key'),
            aes_enc_dec_mode=parametersMap.get('aes_enc_dec_mode'),
            aes_encrypt_aad=parametersMap.get('aes_encrypt_aad'),
            aes_encrypt_iv=parametersMap.get('aes_encrypt_iv')
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
                MacroParameter("enc_dec_method", str(properties.enc_dec_method)),
                MacroParameter("enc_dec_charSet", str(properties.enc_dec_charSet)),
                MacroParameter("aes_enc_dec_key", str(properties.aes_enc_dec_key)),
                MacroParameter("aes_enc_dec_mode", str(properties.aes_enc_dec_mode)),
                MacroParameter("aes_encrypt_aad", str(properties.aes_encrypt_aad)),
                MacroParameter("aes_encrypt_iv", str(properties.aes_encrypt_iv))
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


