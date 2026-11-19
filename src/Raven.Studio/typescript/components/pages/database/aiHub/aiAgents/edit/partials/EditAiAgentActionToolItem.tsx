import Code from "components/common/Code";
import { FormInput, FormGroup, FormLabel } from "components/common/Form";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EditAiAgentActionToolItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

export default function EditAiAgentActionToolItem({ index, remove, save, edit }: EditAiAgentActionToolItemProps) {
    const t = useStudioTranslation("editAiAgent");
    const { control, setValue, trigger } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const handleSave = async () => {
        const isValid = await trigger([`actions.${index}`]);
        if (!isValid) {
            return;
        }
        save();
    };

    const actionItem = formValues.actions[index];

    if (!actionItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{actionItem.name}</h4>
                    <small className="tool-description">{actionItem.description}</small>
                </div>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="chevron-down" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">{t("actionTool.configure")}</h4>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={handleSave}>
                        <Icon icon="chevron-up" margin="m-0" />
                    </Button>
                </div>
            </div>
            <hr className="mt-2 mb-3" />
            <FormGroup>
                <FormLabel>{t("tools.toolNameLabel")}</FormLabel>
                <FormInput
                    type="text"
                    control={control}
                    name={`actions.${index}.name`}
                    placeholder={t("actionTool.namePlaceholder")}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>{t("shared.description")}</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`actions.${index}.description`}
                    placeholder={t("actionTool.descriptionPlaceholder")}
                    rows={4}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName={`actions.${index}.parametersSampleObject`}
                sampleObjectLabel={t("tools.sampleParametersLabel")}
                sampleObject={actionItem.parametersSampleObject}
                sampleObjectSyntaxHelp={<ActionFieldSampleObjectSyntaxHelp />}
                sampleObjectTooltip={<ActionFieldSampleObjectTooltip />}
                sampleObjectPlaceholder={t("actionTool.sampleObjectPlaceholder")}
                jsonSchemaName={`actions.${index}.parametersSchema`}
                jsonSchemaLabel={t("tools.parametersSchemaLabel")}
                jsonSchema={actionItem.parametersSchema}
                jsonSchemaSyntaxHelp={<ActionFieldJsonSchemaSyntaxHelp />}
                schemaType="ToolParameters"
                helpActionTooltipTitle={t("shared.syntaxExample")}
                canRegenerateSchemaName={`actions.${index}.canRegenerateSchema`}
            />
        </div>
    );
}

function ActionFieldSampleObjectTooltip() {
    return (
        <StudioTrans
            ns="editAiAgent"
            i18nKey="actionTool.sampleObjectInfo"
            components={{ br: <br />, code: <code /> }}
        />
    );
}

function ActionFieldSampleObjectSyntaxHelp() {
    const t = useStudioTranslation("editAiAgent");

    const exampleCode = `{
    "customerId": "Fill in the ID of the customer who reported the problem.",
    "orderId": "Fill in the ID of the order the customer is complaining about.",
    "problemDescription": "Provide a description of the issue."
}`;

    const exampleCodeNoParameters = `{}`;

    return (
        <div>
            <div>
                <StudioTrans ns="editAiAgent" i18nKey="actionTool.sampleObjectExample" components={{ br: <br /> }} />
            </div>
            <Code code={exampleCode} language="json" />
            <div className="mt-2">{t("shared.exampleNoParameters", { number: 2 })}</div>
            <Code code={exampleCodeNoParameters} language="json" />
        </div>
    );
}

function ActionFieldJsonSchemaSyntaxHelp() {
    const exampleCode = `{
    "type": "object",
    "properties": {
        "customerId": {
            "type": "string",
            "description": "Fill in the ID of the customer who reported the problem."
        },
        "orderId": {
            "type": "string",
            "description": "Fill in the ID of the order the customer is complaining about."
        },
        "problemDescription": {
            "type": "string",
            "description": "Provide a description of the issue."
        }
    },
    "required": [
        "customerId",
        "orderId",
        "problemDescription"
    ],
    "additionalProperties": false
}`;

    return (
        <div>
            <Code code={exampleCode} language="json" />
        </div>
    );
}
