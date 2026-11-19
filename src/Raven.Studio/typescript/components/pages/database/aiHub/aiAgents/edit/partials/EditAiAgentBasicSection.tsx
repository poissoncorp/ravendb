import { FormGroup, FormInput, FormLabel, FormSelect, FormErrorIcon } from "components/common/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { SelectOption } from "components/common/select/Select";
import useBoolean from "components/hooks/useBoolean";
import TaskUtils from "components/utils/TaskUtils";
import Button from "react-bootstrap/Button";
import Code from "components/common/Code";
import Collapse from "react-bootstrap/Collapse";
import AiAssistantWindow from "components/common/aiAssistant/AiAssistantWindow";
import AiAssistantButton from "components/common/aiAssistant/AiAssistantButton";
import CollapseButton from "components/common/CollapseButton";
import { FormTaskConnectionString } from "components/common/formFields/FormTaskConnectionString";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EditAiAgentBasicSectionProps {
    isEditAiAgent: boolean;
}

export default function EditAiAgentBasicSection({ isEditAiAgent }: EditAiAgentBasicSectionProps) {
    const t = useStudioTranslation("editAiAgent");
    const { control, setValue } = useFormContext<EditAiAgentFormData>();
    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const { value: isAiAssistOpen, toggle: toggleIsAiAssistOpen } = useBoolean(false);

    const formValues = useWatch({
        control,
    });

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

    const stateOptions: SelectOption[] = [
        { label: t("basic.stateEnabled"), value: "Enabled" },
        { label: t("basic.stateDisabled"), value: "Disabled" },
    ];

    return (
        <>
            <div className="hstack align-items-center">
                <h3 className="m-0">{t("basic.heading")}</h3>
                <FormErrorIcon
                    control={control}
                    paths={[
                        "name",
                        "identifier",
                        "connectionStringName",
                        "systemPrompt",
                        "sampleObject",
                        "outputSchema",
                    ]}
                    onError={() => setIsPanelOpen(true)}
                />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("basic.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <FormGroup>
                            <FormLabel>{t("basic.nameLabel")}</FormLabel>
                            <FormInput
                                type="text"
                                control={control}
                                name="name"
                                placeholder={t("basic.namePlaceholder")}
                                onBlur={() => {
                                    if (!formValues.identifier) {
                                        handleGenerateIdentifier();
                                    }
                                }}
                                disabled={isEditAiAgent}
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>
                                {t("basic.identifierLabel")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="editAiAgent"
                                            i18nKey="basic.identifierInfo"
                                            components={{ br: <br /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <FormInput
                                control={control}
                                name="identifier"
                                type="text"
                                placeholder={t("basic.identifierPlaceholder")}
                                disabled={isEditAiAgent}
                                addon={
                                    <Button
                                        variant="link"
                                        className="text-reset px-0"
                                        onClick={handleGenerateIdentifier}
                                        title={t("basic.regenerateTitle")}
                                        disabled={isEditAiAgent}
                                    >
                                        <Icon icon="refresh" />
                                        {t("basic.regenerate")}
                                    </Button>
                                }
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>{t("basic.stateLabel")}</FormLabel>
                            <FormSelect control={control} name="state" options={stateOptions} />
                        </FormGroup>
                        <FormTaskConnectionString
                            control={control}
                            name="connectionStringName"
                            type="Ai"
                            modelType="Chat"
                        />
                        <FormGroup>
                            <FormLabel>
                                {t("basic.systemPromptLabel")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="editAiAgent"
                                            i18nKey="basic.systemPromptInfo"
                                            components={{ br: <br /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info-new" />
                                </PopoverWithHoverWrapper>
                            </FormLabel>
                            <div className="position-relative">
                                <FormInput
                                    type="textarea"
                                    as="textarea"
                                    control={control}
                                    name="systemPrompt"
                                    placeholder={t("basic.systemPromptPlaceholder")}
                                    rows={7}
                                />
                                {formValues.systemPrompt?.length > 0 && (
                                    <AiAssistantButton handleClick={toggleIsAiAssistOpen} />
                                )}
                                {isAiAssistOpen && (
                                    <AiAssistantWindow
                                        data={{
                                            View: "AI Agents",
                                            Message: getRefinePromptMessage(formValues),
                                        }}
                                        acceptResult={(text) => setValue("systemPrompt", text)}
                                        successMessage={t("basic.aiAssistantSuccess")}
                                        closeWindow={toggleIsAiAssistOpen}
                                    />
                                )}
                            </div>
                        </FormGroup>
                        <SampleObjectAndSchemaFields
                            control={control}
                            setValue={setValue}
                            sampleObjectName="sampleObject"
                            sampleObjectLabel={t("basic.sampleObjectLabel")}
                            sampleObject={formValues.sampleObject}
                            sampleObjectSyntaxHelp={<SampleObjectSyntaxHelp />}
                            sampleObjectTooltip={
                                <StudioTrans
                                    ns="editAiAgent"
                                    i18nKey="basic.sampleObjectInfo"
                                    components={{ br: <br /> }}
                                />
                            }
                            sampleObjectPlaceholder={t("basic.sampleObjectPlaceholder")}
                            jsonSchemaName="outputSchema"
                            jsonSchemaLabel={t("basic.jsonSchemaLabel")}
                            jsonSchema={formValues.outputSchema}
                            jsonSchemaSyntaxHelp={<JsonSchemaSyntaxHelp />}
                            jsonSchemaTooltip={
                                <StudioTrans
                                    ns="editAiAgent"
                                    i18nKey="basic.jsonSchemaInfo"
                                    components={{ br: <br /> }}
                                />
                            }
                            helpActionTooltipTitle={t("shared.syntaxExample")}
                            canRegenerateSchemaName="canRegenerateSchema"
                        />
                    </div>
                </div>
            </Collapse>
        </>
    );
}

const SampleObjectSyntaxHelp = () => {
    const t = useStudioTranslation("editAiAgent");

    const code = `{
    "Request": "Summarize the customer's request.",
    "Response": "Provide your response.",
    "CustomerId": "Provide the customer ID.",
    "RelatedProducts": ["list of the related products"]
}`;

    return (
        <div>
            <div>{t("basic.sampleObjectExample")}</div>
            <Code code={code} language="json" />
        </div>
    );
};

const JsonSchemaSyntaxHelp = () => {
    const t = useStudioTranslation("editAiAgent");

    const code = `{
    "name": "MEJIa2ZlclQvQ3VZYlIvTENUQmVHa3JGUXlONzcxZ2dBK2pwYnUybDh0OD0",
    "strict": true,
    "schema": {
        "type": "object",
        "properties": {
            "Request": {
                "type": "string",
                "description": "Summarize the customer's request."
            },
            "Response": {
                "type": "string",
                "description": "Provide your response."
            },
            "CustomerId": {
                "type": "string",
                "description": "Provide the customer ID."
            }
        },
        "required": [
            "Request",
            "Response",
            "CustomerId"
        ],
        "additionalProperties": false
    }
}`;

    return (
        <div>
            <div>{t("basic.jsonSchemaExample")}</div>
            <Code code={code} language="json" />
        </div>
    );
};

function getRefinePromptMessage(formValues: EditAiAgentFormData) {
    return `## Original prompt
${formValues.systemPrompt}
`;
}
