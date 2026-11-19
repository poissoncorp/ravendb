import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../store/editGenAiTaskSlice";
import { ReactNode } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { EditGenAiTaskStepBasic, EditGenAiTaskStepBasicFooter } from "../partials/steps/EditGenAiTaskStepBasic";
import { EditGenAiTaskStepContext, EditGenAiTaskStepContextFooter } from "../partials/steps/EditGenAiTaskStepContext";
import { EditGenAiTaskStepModel, EditGenAiTaskStepModelFooter } from "../partials/steps/EditGenAiTaskStepModel";
import { EditGenAiTaskStepUpdate, EditGenAiTaskStepUpdateFooter } from "../partials/steps/EditGenAiTaskStepUpdate";
import { EditGenAiTaskStepSummary, EditGenAiTaskStepSummaryFooter } from "../partials/steps/EditGenAiTaskStepSummary";

export type EditGenAiTaskStepId = "basic" | "context" | "modelInput" | "updateScript" | "summary";

export interface EditGenAiTaskStep {
    id: EditGenAiTaskStepId;
    title: string;
    component: ReactNode;
    footer?: ReactNode;
    isCurrent: boolean;
}

export function useEditGenAiTaskSteps(): EditGenAiTaskStep[] {
    const t = useStudioTranslation("editGenAiTask");
    const currentStep = useAppSelector(editGenAiTaskSelectors.currentStep);

    return [
        {
            id: "basic",
            title: t("steps.basic"),
            component: <EditGenAiTaskStepBasic />,
            footer: <EditGenAiTaskStepBasicFooter />,
            isCurrent: currentStep === "basic",
        },
        {
            id: "context",
            title: t("steps.context"),
            component: <EditGenAiTaskStepContext />,
            footer: <EditGenAiTaskStepContextFooter />,
            isCurrent: currentStep === "context",
        },
        {
            id: "modelInput",
            title: t("steps.modelInput"),
            component: <EditGenAiTaskStepModel />,
            footer: <EditGenAiTaskStepModelFooter />,
            isCurrent: currentStep === "modelInput",
        },
        {
            id: "updateScript",
            title: t("steps.updateScript"),
            component: <EditGenAiTaskStepUpdate />,
            footer: <EditGenAiTaskStepUpdateFooter />,
            isCurrent: currentStep === "updateScript",
        },
        {
            id: "summary",
            title: t("steps.summary"),
            component: <EditGenAiTaskStepSummary />,
            footer: <EditGenAiTaskStepSummaryFooter />,
            isCurrent: currentStep === "summary",
        },
    ];
}
