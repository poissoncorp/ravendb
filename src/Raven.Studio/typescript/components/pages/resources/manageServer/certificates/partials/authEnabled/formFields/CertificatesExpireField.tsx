import { FormInput, FormSelect, FormGroup, FormLabel } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useFormContext } from "react-hook-form";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

export default function CertificatesExpireField() {
    const t = useStudioTranslation("certificates");
    const { control } = useFormContext<CertificatesGenerateFormData | CertificatesCloneFormData>();

    return (
        <FormGroup>
            <FormLabel>{t("expire.label")}</FormLabel>
            <FormInput
                type="number"
                control={control}
                name="expireIn"
                placeholder={t("expire.placeholder")}
                addon={
                    <FormSelect
                        control={control}
                        name="expireTimeUnits"
                        options={
                            [
                                { value: "days", label: t("expire.days") },
                                { value: "months", label: t("expire.months") },
                            ] satisfies SelectOption<ExpireTimeUnit>[]
                        }
                    />
                }
            />
        </FormGroup>
    );
}
