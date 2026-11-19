import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch } from "components/common/Form";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import { chatAiAgentSelectors } from "../store/chatAiAgentSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ChatAiAgentPersistenceSection() {
    const t = useStudioTranslation("aiAgentChat");
    const { control } = useFormContext<ChatAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const isDocumentExpirationEnabled = useAppSelector(chatAiAgentSelectors.isDocumentExpirationEnabled);

    return (
        <div>
            <h3>
                <Icon icon="press-releases" color="primary" /> {t("persistence.heading")}
                <PopoverWithHoverWrapper
                    message={
                        <StudioTrans
                            ns="aiAgentChat"
                            i18nKey="persistence.headingInfo"
                            components={{ code: <code />, br: <br /> }}
                        />
                    }
                >
                    <Icon icon="info-new" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                <FormGroup>
                    <FormLabel>
                        {t("persistence.idPrefixLabel")}
                        <PopoverWithHoverWrapper message={t("persistence.idPrefixInfo")}>
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput
                        control={control}
                        name="persistenceConversationIdPrefix"
                        type="text"
                        placeholder={t("persistence.idPrefixPlaceholder")}
                    />
                </FormGroup>
                {isDocumentExpirationEnabled.status === "success" && !isDocumentExpirationEnabled.data && (
                    <FormGroup marginClass="mb-1">
                        <FormSwitch control={control} name="isEnableDocumentExpiration">
                            {t("persistence.enableExpiration")}
                        </FormSwitch>
                    </FormGroup>
                )}
                {(formValues.isEnableDocumentExpiration || isDocumentExpirationEnabled.data) && (
                    <FormGroup marginClass="mb-0">
                        <FormSwitch name="isDocumentExpireInCustomizeEnabled" control={control}>
                            {t("persistence.setExpiration")}
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="aiAgentChat"
                                        i18nKey="persistence.setExpirationInfo"
                                        components={{ br: <br /> }}
                                    />
                                }
                            >
                                <Icon icon="info-new" />
                            </PopoverWithHoverWrapper>
                        </FormSwitch>
                        {formValues.isDocumentExpireInCustomizeEnabled && (
                            <FormDurationPicker
                                control={control}
                                name="persistenceExpiresInSeconds"
                                showDays
                                isFlexGrow
                            />
                        )}
                    </FormGroup>
                )}
            </div>
        </div>
    );
}
