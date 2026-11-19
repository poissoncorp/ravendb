import messagePublisher from "common/messagePublisher";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { editCdcSinkTaskUtils } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskUtils";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch, useAppSelector } from "components/store";
import { UseFormReturn } from "react-hook-form";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export function useEditCdcSinkTaskRawViewSync(editForm: UseFormReturn<EditCdcSinkTaskFormData>) {
    const t = useStudioTranslation("editCdcSinkTask");
    const dispatch = useAppDispatch();
    const isRawView = useAppSelector(editCdcSinkTaskSelectors.isRawView);
    const rawViewContent = useAppSelector(editCdcSinkTaskSelectors.rawViewContent);

    const applyRawViewContent = (): boolean => {
        if (!isRawView) {
            return true;
        }

        try {
            const formData = editCdcSinkTaskUtils.mapConfigFromDto(JSON.parse(rawViewContent));
            editForm.reset(formData, { keepDefaultValues: true });
            return true;
        } catch (error) {
            messagePublisher.reportError(t("rawView.rawConfigConversionError"), error);
            return false;
        }
    };

    const revealValidationErrors = () => {
        if (!isRawView) {
            return;
        }

        dispatch(editCdcSinkTaskActions.rawViewClosed());
        messagePublisher.reportWarning(t("rawView.validationErrorsWarning"));
    };

    return { applyRawViewContent, revealValidationErrors };
}
