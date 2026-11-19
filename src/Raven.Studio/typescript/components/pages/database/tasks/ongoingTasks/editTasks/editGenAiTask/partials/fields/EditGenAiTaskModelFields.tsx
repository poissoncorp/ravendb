import {
    FormAceEditor,
    FormLabel,
    FormGroup,
    FormErrorIcon,
    FormSwitch,
    FormDurationPicker,
} from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { useRef } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import AiAssistantWindow from "components/common/aiAssistant/AiAssistantWindow";
import AiAssistantButton from "components/common/aiAssistant/AiAssistantButton";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import useEditGenAiTaskToolsSection from "../../hooks/useEditGenAiTaskToolsSection";
import useBoolean from "components/hooks/useBoolean";
import EditGenAiTaskQueryToolItem from "./EditGenAiTaskQueryToolItem";
import { jsonSchemaSamplesTabs, promptSamplesTabs, sampleObjectSamplesTabs } from "../../editGenAiTaskSamplesData";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditGenAiTaskModelFields() {
    const t = useStudioTranslation("editGenAiTask");
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();
    const toolsEditor = useEditGenAiTaskToolsSection();
    const { value: isToolsPanelOpen, setValue: setIsToolsPanelOpen, toggle: toggleIsToolsPanelOpen } = useBoolean(true);

    const formValues = useWatch({ control });

    const promptRef = useRef<ReactAce>(null);

    const { value: isAiAssistOpen, toggle: toggleIsAiAssistOpen } = useBoolean(false);

    return (
        <>
            <FormGroup>
                <FormLabel>
                    {t("model.prompt")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans ns="editGenAiTask" i18nKey="model.promptInfo" components={{ br: <br /> }} />
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    aceRef={promptRef}
                    control={control}
                    name="prompt"
                    mode="text"
                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                    samplesPanel={{ tabs: promptSamplesTabs }}
                    overlaySlot={
                        <>
                            {formValues.prompt?.length > 0 && (
                                <AiAssistantButton handleClick={toggleIsAiAssistOpen} right="48px" />
                            )}
                            {isAiAssistOpen && (
                                <AiAssistantWindow
                                    data={{
                                        View: "GenAI",
                                        Message: getRefinePromptMessage(formValues),
                                    }}
                                    acceptResult={(text) => setValue("prompt", text)}
                                    successMessage={t("model.aiAssistSuccess")}
                                    closeWindow={toggleIsAiAssistOpen}
                                    right="48px"
                                />
                            )}
                        </>
                    }
                    wrapEnabled
                    setOptions={{
                        indentedSoftWrap: false,
                    }}
                    isFullScreenLabelHidden
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName="sampleObject"
                sampleObject={formValues.sampleObject}
                sampleObjectSamplesPanel={{ tabs: sampleObjectSamplesTabs }}
                jsonSchemaName="jsonSchema"
                jsonSchema={formValues.jsonSchema}
                jsonSchemaSamplesPanel={{ tabs: jsonSchemaSamplesTabs }}
                canRegenerateSchemaName="canRegenerateSchema"
            />
            <div className="hstack mt-3 mb-1">
                <h3 className="mb-0">
                    {t("model.queryToolsTitle")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="model.queryToolsInfo"
                                components={{ br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["queries"]} onError={() => setIsToolsPanelOpen(true)} />
                <CollapseButton isExpanded={isToolsPanelOpen} toggle={toggleIsToolsPanelOpen} />
            </div>
            <Collapse in={isToolsPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary mb-2">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="query" color="primary" margin="m-0" />
                                </div>
                                <div>{t("model.queryTools")}</div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddQuery}>
                                <Icon icon="plus" />
                                {t("model.addQueryTool")}
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.queriesFieldArray.fields.map((field, index) => (
                                <EditGenAiTaskQueryToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveQuery(index)}
                                    save={() => toolsEditor.handleSaveQuery(index)}
                                    edit={() => toolsEditor.handleEditQuery(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
            <TracingFields />
        </>
    );
}

function TracingFields() {
    const t = useStudioTranslation("editGenAiTask");
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <FormGroup marginClass="mb-0 mt-2">
                <FormSwitch control={control} name="isEnableTracing">
                    {t("model.enableTracing")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="model.enableTracingInfo"
                                components={{ code: <code />, br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            {formValues.isEnableTracing && (
                <FormGroup marginClass="mt-1">
                    <FormSwitch control={control} name="isSetTracingExpiration">
                        {t("model.setTracingExpiration")}
                        <PopoverWithHoverWrapper message={t("model.setTracingExpirationInfo")}>
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                    {formValues.isSetTracingExpiration && (
                        <FormDurationPicker control={control} name="tracingExpirationInSeconds" showDays isFlexGrow />
                    )}
                </FormGroup>
            )}
        </>
    );
}

function getRefinePromptMessage(formValues: EditGenAiTaskFormData) {
    return `## Original prompt
${formValues.prompt}

## Source collection name
${formValues.collectionName}

## Context generation script
${formValues.script}

## Output structure
${formValues.jsonSchema || formValues.sampleObject || ""}`;
}
