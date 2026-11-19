import EditGenAiTaskContextFields from "../fields/EditGenAiTaskContextFields";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function EditGenAiTaskStepContext() {
    const t = useStudioTranslation("editGenAiTask");

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title={t("context.title")} marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                <StudioTrans ns="editGenAiTask" i18nKey="context.intro" components={{ br: <br /> }} />
            </p>
            <EditGenAiTaskContextFields />
        </>
    );
}

export function EditGenAiTaskStepContextFooter() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);

    const { handleContextTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["collectionName", "script"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("modelInput"));
        }
    };

    const isTestButtonDisabled = !formValues.playgroundDocument;

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))}
                >
                    <Icon icon="arrow-left" /> {t("footer.back")}
                </Button>
            </div>
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !formValues.playgroundDocument,
                            message: t("context.testDisabledNoDocument"),
                        },
                        {
                            isActive: true,
                            message: (
                                <StudioTrans
                                    ns="editGenAiTask"
                                    i18nKey="context.testInfo"
                                    components={{ br: <br /> }}
                                />
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleContextTest}
                        isSpinning={contextTest.status === "loading"}
                        disabled={isTestButtonDisabled}
                        icon="test"
                    >
                        {t("context.test")}
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    {t("footer.next")} <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
