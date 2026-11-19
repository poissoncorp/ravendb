import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useFormContext } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormInput, FormLabel } from "components/common/Form";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import OptionalLabel from "components/common/OptionalLabel";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type FormData = ConnectionFormData<AiConnection>;

export default function EmbeddingsMaxConcurrentBatches({
    baseName,
}: {
    baseName: Exclude<FormData["connectorType"], "anthropicSettings">;
}) {
    const t = useStudioTranslation("connectionStrings");
    const { control } = useFormContext<FormData>();

    return (
        <div className="mb-2">
            <FormLabel>
                {t("ai.embeddingsMaxConcurrentBatchesLabel")} <OptionalLabel />
                <PopoverWithHoverWrapper message={t("ai.embeddingsMaxConcurrentBatchesInfo")}>
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormInput control={control} name={`${baseName}.embeddingsMaxConcurrentBatches`} type="number" />
        </div>
    );
}
