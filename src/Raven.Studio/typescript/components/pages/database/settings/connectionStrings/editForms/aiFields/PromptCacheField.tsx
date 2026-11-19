import { FormLabel, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { ReactNode } from "react";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<AiConnection>;

interface PromptCacheFieldProps {
    baseName: Extract<
        FormData["connectorType"],
        "azureOpenAiSettings" | "googleSettings" | "openAiSettings" | "anthropicSettings"
    >;
    serverDefaultValue: boolean;
    label?: string;
    description?: ReactNode;
}

export default function PromptCacheField({ baseName, serverDefaultValue, label, description }: PromptCacheFieldProps) {
    const t = useStudioTranslation("connectionStrings");
    const { control } = useFormContext<FormData>();

    const modelType = useWatch({ control, name: "modelType" });

    if (modelType !== "Chat") {
        return null;
    }

    const fieldName = `${baseName}.enablePromptCache` satisfies FieldPath<FormData>;

    return (
        <div className="mb-2">
            <FormLabel>
                {label ?? t("ai.promptCacheKey.label")}
                <PopoverWithHoverWrapper
                    message={
                        description ?? (
                            <>
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="ai.promptCacheKey.info"
                                    components={{ code: <code /> }}
                                />
                                <br />
                                <br />
                                {t("ai.promptCacheKey.reuse")}
                                <br />
                                <br />
                                <ul className="mb-0">
                                    <li>
                                        <StudioTrans
                                            ns="connectionStrings"
                                            i18nKey="ai.promptCacheKey.defaultItem"
                                            options={{ context: serverDefaultValue ? "sent" : "notSent" }}
                                            components={{ strong: <strong />, code: <code />, br: <br /> }}
                                        />
                                    </li>
                                    <li className="mt-1">
                                        <StudioTrans
                                            ns="connectionStrings"
                                            i18nKey="ai.promptCacheKey.alwaysSend"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                    <li className="mt-1">
                                        <StudioTrans
                                            ns="connectionStrings"
                                            i18nKey="ai.promptCacheKey.neverSend"
                                            components={{ strong: <strong /> }}
                                        />
                                    </li>
                                </ul>
                            </>
                        )
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormSelect control={control} name={fieldName} options={getPromptCacheOptions(t)} />
        </div>
    );
}

function getPromptCacheOptions(t: StudioTranslate<"connectionStrings">): SelectOption<boolean | null>[] {
    return [
        { label: t("ai.promptCacheKey.optionDefault"), value: null },
        { label: t("ai.promptCacheKey.optionTrue"), value: true },
        { label: t("ai.promptCacheKey.optionFalse"), value: false },
    ];
}
