import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { AboutViewHeading } from "components/common/AboutView";
import useBoolean from "components/hooks/useBoolean";
import AceEditor from "components/common/ace/AceEditor";
import { AceEditorMode } from "components/models/aceEditor";
import Collapse from "react-bootstrap/Collapse";
import { ThemeColor } from "components/models/common";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";
import { FormSwitch, FormGroup } from "components/common/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";
import moment from "moment";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditGenAiTaskStepSummary() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();

    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);

    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const taskStateLabels: Partial<Record<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState, string>> = {
        Enabled: t("basic.stateEnabled"),
        Disabled: t("basic.stateDisabled"),
    };

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title={t("summary.title")} marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                <StudioTrans ns="editGenAiTask" i18nKey="summary.intro" components={{ br: <br /> }} />
            </p>
            <div className="hstack justify-content-between">
                <div>{t("summary.basicSettings")}</div>
                <Button variant="link" onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))} size="sm">
                    <Icon icon="edit" /> {t("summary.edit")}
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>{t("summary.taskName")}</div>
                    <div>{formValues.name}</div>
                </div>
                <div className="hstack justify-content-between">
                    <div>{t("summary.taskState")}</div>
                    <div className={getBooleanColorClass(formValues.state === "Enabled")}>
                        {taskStateLabels[formValues.state] ?? formValues.state}
                    </div>
                </div>
                {formValues.responsibleNode && (
                    <div className="hstack justify-content-between">
                        <div>{t("summary.responsibleNode")}</div>
                        <div>
                            <Icon icon="node" color="node" />
                            {formValues.responsibleNode}
                        </div>
                    </div>
                )}
                <div className="hstack justify-content-between">
                    <div>{t("summary.connectionString")}</div>
                    <div>{formValues.connectionStringName}</div>
                </div>
                <div className="hstack justify-content-between">
                    <div>{t("summary.maxConcurrency")}</div>
                    <div>{formValues.maxConcurrency || editGenAiTaskUtils.defaultMaxConcurrency}</div>
                </div>
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>{t("summary.contextInput")}</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                    size="sm"
                >
                    <Icon icon="edit" /> {t("summary.edit")}
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>{t("summary.sourceCollection")}</div>
                    <div>{formValues.collectionName}</div>
                </div>
                <RowWithPreview
                    label={t("summary.contextGenerationScript")}
                    value={formValues.script}
                    mode="javascript"
                />
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>{t("summary.promptAndSchema")}</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                    size="sm"
                >
                    <Icon icon="edit" /> {t("summary.edit")}
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>{t("summary.prompt")}</div>
                    <div style={{ maxWidth: 200 }} className="text-truncate" title={formValues.prompt}>
                        {formValues.prompt}
                    </div>
                </div>
                {formValues.sampleObject && !formValues.jsonSchema && (
                    <RowWithPreview label={t("summary.sampleObject")} value={formValues.sampleObject} mode="json" />
                )}
                {formValues.jsonSchema && (
                    <RowWithPreview label={t("summary.jsonSchema")} value={formValues.jsonSchema} mode="json" />
                )}
                {formValues.queries?.length > 0 && (
                    <div className="hstack justify-content-between">
                        <div>{t("summary.queryTools")}</div>
                        <div>{formValues.queries.length}</div>
                    </div>
                )}
                <div className="hstack justify-content-between">
                    <div>{t("summary.conversationDocuments")}</div>
                    <div className={getBooleanColorClass(formValues.isEnableTracing)}>
                        {formValues.isEnableTracing ? t("summary.enabled") : t("summary.disabled")}
                    </div>
                </div>
                {formValues.isSetTracingExpiration && (
                    <div className="hstack justify-content-between">
                        <div>{t("summary.conversationDocumentsExpiration")}</div>
                        <div>
                            {genUtils.formatDuration(
                                moment.duration(formValues.tracingExpirationInSeconds, "seconds"),
                                true
                            )}
                        </div>
                    </div>
                )}
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>{t("summary.documentUpdate")}</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                    size="sm"
                >
                    <Icon icon="edit" /> {t("summary.edit")}
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <RowWithPreview label={t("summary.updateScript")} value={formValues.updateScript} mode="javascript" />
            </div>
            {isEditTask && (
                <div className="hstack justify-content-end mt-4">
                    <FormGroup>
                        <FormSwitch control={control} name="isResetScript">
                            {t("summary.reprocessAll")}
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="editGenAiTask"
                                        i18nKey="summary.reprocessAllInfo"
                                        components={{ br: <br /> }}
                                    />
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </FormSwitch>
                    </FormGroup>
                </div>
            )}
        </>
    );
}

export function EditGenAiTaskStepSummaryFooter() {
    const t = useStudioTranslation("editGenAiTask");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                >
                    <Icon icon="arrow-left" /> {t("summary.back")}
                </Button>
            </div>

            <Button type="submit" variant="primary" className="rounded-pill">
                {commonT("save")} <Icon icon="save" margin="ms-1" />
            </Button>
        </div>
    );
}

function RowWithPreview(props: { label: string; value: string; mode: AceEditorMode }) {
    const t = useStudioTranslation("editGenAiTask");
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <div>
            <div className="hstack justify-content-between">
                <div>{props.label}</div>
                <Button variant="link" size="xs" onClick={toggleIsOpen} className="pe-0">
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} />
                    {isOpen ? t("summary.collapse") : t("summary.show")}
                </Button>
            </div>
            <Collapse in={isOpen} mountOnEnter unmountOnExit>
                <div className="mt-2">
                    <AceEditor mode={props.mode} value={props.value || ""} readOnly />
                </div>
            </Collapse>
        </div>
    );
}

function getBooleanColorClass(value: boolean): `text-${ThemeColor}` {
    return value ? "text-success" : "text-danger";
}
