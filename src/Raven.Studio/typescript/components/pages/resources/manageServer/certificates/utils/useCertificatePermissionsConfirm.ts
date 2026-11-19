import useConfirm from "components/common/ConfirmDialog";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function useCertificatePermissionsConfirm() {
    const t = useStudioTranslation("certificates");
    const confirm = useConfirm();

    return async (formData: CertificatesGenerateFormData) => {
        if (formData.securityClearance === "ValidUser" && formData.databasePermissions.length === 0) {
            return await confirm({
                title: t("permissionsConfirm.title"),
                message: t("permissionsConfirm.message"),
                confirmText: t("permissionsConfirm.confirm"),
            });
        }

        return true;
    };
}
