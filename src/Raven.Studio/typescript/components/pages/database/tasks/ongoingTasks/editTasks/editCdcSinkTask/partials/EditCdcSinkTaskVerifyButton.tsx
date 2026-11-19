import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { RavenButtonVariants } from "react-bootstrap/Button";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { EditCdcSinkTaskWatchedVerification } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskVerification";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

interface EditCdcSinkTaskVerifyButtonProps {
    verification: EditCdcSinkTaskWatchedVerification;
    onVerify: () => void;
    isDisabled: boolean;
}

export default function EditCdcSinkTaskVerifyButton({
    verification,
    onVerify,
    isDisabled,
}: EditCdcSinkTaskVerifyButtonProps) {
    const t = useStudioTranslation("editCdcSinkTask");
    const buttonData = getStatusButtonData(verification, t);

    return (
        <PopoverWithHoverWrapper message={t("verification.buttonInfo")}>
            <ButtonWithSpinner
                type="button"
                variant={buttonData.variant}
                className="rounded-pill"
                onClick={onVerify}
                isSpinning={verification.loading}
                disabled={isDisabled}
                icon={buttonData.icon}
            >
                {buttonData.label}
            </ButtonWithSpinner>
        </PopoverWithHoverWrapper>
    );
}

function getStatusButtonData(
    { loading, error, result }: EditCdcSinkTaskWatchedVerification,
    t: StudioTranslate<"editCdcSinkTask">
): {
    label: string;
    icon: IconName;
    variant: RavenButtonVariants;
} {
    if (loading) {
        return { label: t("verification.verifying"), icon: "shield", variant: "outline-info" };
    }
    if (error || (result && !result.Success)) {
        return { label: t("verification.failed"), icon: "danger", variant: "outline-danger" };
    }
    if (result?.Warnings.length) {
        return { label: t("verification.verifiedWithWarnings"), icon: "warning", variant: "outline-warning" };
    }
    if (result) {
        return { label: t("verification.verified"), icon: "check", variant: "outline-success" };
    }
    return { label: t("verification.verify"), icon: "shield", variant: "outline-info" };
}
