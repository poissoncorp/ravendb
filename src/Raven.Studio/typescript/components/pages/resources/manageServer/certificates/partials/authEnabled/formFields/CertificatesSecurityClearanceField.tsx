import { FormSelect, FormLabel } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useAppSelector } from "components/store";
import useUniqueId from "components/hooks/useUniqueId";
import { useFormContext } from "react-hook-form";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function CertificatesSecurityClearanceField() {
    const t = useStudioTranslation("certificates");
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const selectId = useUniqueId("security-clearance-select");

    const { control } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData | CertificatesEditFormData
    >();

    return (
        <div className="mb-3">
            <FormLabel htmlFor={selectId}>{t("clearanceField.label")}</FormLabel>
            <FormSelect
                inputId={selectId}
                control={control}
                name="securityClearance"
                options={
                    [
                        {
                            value: "ClusterAdmin",
                            label: t("clearance.clusterAdmin"),
                            isDisabled: !isClusterAdminOrClusterNode,
                        },
                        { value: "Operator", label: t("clearance.operator") },
                        { value: "ValidUser", label: t("clearance.user") },
                    ] satisfies SelectOption[]
                }
            />
        </div>
    );
}
