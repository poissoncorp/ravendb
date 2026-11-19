import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskModelFields from "../fields/EditGenAiTaskModelFields";
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

export function EditGenAiTaskStepModel() {
    const t = useStudioTranslation("editGenAiTask");

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title={t("model.title")} marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                <StudioTrans ns="editGenAiTask" i18nKey="model.intro" components={{ br: <br /> }} />
            </p>
            <EditGenAiTaskModelFields />
        </>
    );
}

export function EditGenAiTaskStepModelFooter() {
    const t = useStudioTranslation("editGenAiTask");
    const dispatch = useAppDispatch();

    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const { handleModelInputTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger([
            "prompt",
            "sampleObject",
            "jsonSchema",
            "queries",
            "isEnableTracing",
            "isSetTracingExpiration",
            "tracingExpirationInSeconds",
        ]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("updateScript"));
        }
    };

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                >
                    <Icon icon="arrow-left" /> {t("footer.back")}
                </Button>
            </div>
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: formValues.playgroundContexts.length === 0,
                            message: t("model.testDisabledNoContexts"),
                        },
                        {
                            isActive: true,
                            message: (
                                <StudioTrans ns="editGenAiTask" i18nKey="model.testInfo" components={{ br: <br /> }} />
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleModelInputTest}
                        isSpinning={modelInputTest.status === "loading"}
                        icon="test"
                        disabled={formValues.playgroundContexts.length === 0}
                    >
                        {t("model.test")}
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    {t("footer.next")} <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
