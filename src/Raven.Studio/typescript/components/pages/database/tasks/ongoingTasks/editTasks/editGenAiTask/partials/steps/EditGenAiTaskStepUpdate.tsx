import { FormGroup, FormLabel } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import { useRef } from "react";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";
import { updateScriptSamplesTabs } from "../../editGenAiTaskSamplesData";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditGenAiTaskStepUpdate() {
    const t = useStudioTranslation("editGenAiTask");
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const scriptRef = useRef<ReactAce>(null);

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title={t("update.title")} marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                <StudioTrans ns="editGenAiTask" i18nKey="update.intro" components={{ br: <br /> }} />
            </p>
            <FormGroup>
                <FormLabel>
                    {t("update.updateScript")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="update.updateScriptInfo"
                                components={{ br: <br />, code: <code /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    aceRef={scriptRef}
                    control={control}
                    name="updateScript"
                    mode="javascript"
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: (
                                <AceEditor.LoadFileAction
                                    onLoad={(value) => setValue("updateScript", value, { shouldValidate: true })}
                                />
                            ),
                        },
                    ]}
                    samplesPanel={{ tabs: updateScriptSamplesTabs }}
                />
            </FormGroup>
        </>
    );
}

export function EditGenAiTaskStepUpdateFooter() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    const { handleUpdateScriptTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["updateScript"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }
    };

    const isTestButtonDisabled =
        !formValues.playgroundDocument ||
        formValues.playgroundContexts.length === 0 ||
        formValues.playgroundModelOutputs.length === 0 ||
        formValues.playgroundContexts.length !== formValues.playgroundModelOutputs.length;

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                >
                    <Icon icon="arrow-left" /> {t("footer.back")}
                </Button>
            </div>
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !formValues.playgroundDocument,
                            message: t("update.testDisabledNoDocument"),
                        },
                        {
                            isActive: formValues.playgroundContexts.length === 0,
                            message: t("update.testDisabledNoContexts"),
                        },
                        {
                            isActive: formValues.playgroundModelOutputs.length === 0,
                            message: t("update.testDisabledNoModelOutputs"),
                        },
                        {
                            isActive: formValues.playgroundContexts.length !== formValues.playgroundModelOutputs.length,
                            message: t("update.testDisabledCountMismatch"),
                        },
                        {
                            isActive: true,
                            message: (
                                <StudioTrans ns="editGenAiTask" i18nKey="update.testInfo" components={{ br: <br /> }} />
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleUpdateScriptTest}
                        isSpinning={updateScriptTest.status === "loading"}
                        icon="test"
                        disabled={isTestButtonDisabled}
                    >
                        {t("update.test")}
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    {t("footer.next")} <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
