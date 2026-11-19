import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useAppDispatch } from "components/store";
import { editAiAgentActions } from "../store/editAiAgentSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { UseFormReturn } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface EditAiAgentFooterProps {
    editForm: UseFormReturn<EditAiAgentFormData>;
    generateTestParameters: () => void;
}

export default function EditAiAgentFooter({ editForm, generateTestParameters }: EditAiAgentFooterProps) {
    const t = useStudioTranslation("editAiAgent");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();

    const { forCurrentDatabase } = useAppUrls();

    const handleOpenTest = async () => {
        const isValid = await editForm.trigger();
        if (!isValid) {
            return;
        }

        generateTestParameters();
        dispatch(editAiAgentActions.isTestOpenSet(true));
    };

    return (
        <div className="hstack justify-content-between">
            <a
                href={forCurrentDatabase.aiAgents()}
                className="btn btn-outline-secondary rounded-pill"
                title={t("footer.cancelTitle")}
            >
                {commonT("cancel")}
            </a>
            <div className="hstack gap-2">
                <PopoverWithHoverWrapper message={t("footer.testInfo")}>
                    <Button variant="info" className="rounded-pill" onClick={handleOpenTest}>
                        <Icon icon="test" />
                        {t("footer.test")}
                    </Button>
                </PopoverWithHoverWrapper>

                <ButtonWithSpinner
                    type="submit"
                    variant="primary"
                    className="rounded-pill"
                    icon="save"
                    isSpinning={editForm.formState.isSubmitting}
                >
                    {commonT("save")}
                </ButtonWithSpinner>
            </div>
        </div>
    );
}
