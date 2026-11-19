import { FormDurationPicker, FormGroup, FormInput, FormLabel, FormSwitch, FormErrorIcon } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { AiAgentTrimmingMethod, EditAiAgentFormData } from "../utils/editAiAgentValidation";
import ClickableCard from "components/common/ClickableCard";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditAiAgentTrimmingSection() {
    const t = useStudioTranslation("editAiAgent");
    const { control, setValue } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    const handleSetTrimmingMethod = (method: AiAgentTrimmingMethod) => {
        if (formValues.trimming.method === method) {
            setValue("trimming.method", null);
        } else {
            setValue("trimming.method", method);
        }
    };

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    {t("trimming.heading")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="trimming.headingInfo"
                                components={{ br: <br />, code: <code /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["trimming"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("trimming.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <FormGroup>
                            <FormLabel className="hstack justify-content-between">
                                <div>{t("trimming.methodLabel")}</div>
                                {formValues.trimming.method != null && (
                                    <Button variant="link" size="sm" onClick={() => setValue("trimming.method", null)}>
                                        {t("trimming.clearSelection")}
                                    </Button>
                                )}
                            </FormLabel>
                            <div className="d-flex">
                                <ClickableCard
                                    icon="tasks-list"
                                    title={t("trimming.summarizeTitle")}
                                    description={t("trimming.summarizeDescription")}
                                    className="flex-grow-1"
                                    isSelected={formValues.trimming.method === "Tokens"}
                                    onClick={() => handleSetTrimmingMethod("Tokens")}
                                />
                                {/* maybe add it in RC2
                        <ClickableCard
                            icon="collapse-vertical"
                            title="Truncate chat"
                            description="Remove older messages when the number of chat messages exceeds the configured maximum."
                            className="w-50"
                            isSelected={formValues.trimming.method === "Truncate"}
                            onClick={() => handleSetTrimmingMethod("Truncate")}
                        /> */}
                            </div>
                        </FormGroup>
                        {formValues.trimming.method === "Tokens" && <TokensFields />}
                        {/* {formValues.trimming.method === "Truncate" && <TruncateFields />} */}
                    </div>
                </div>
            </Collapse>
        </>
    );
}

function TokensFields() {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();

    return (
        <>
            <FormGroup>
                <FormLabel>
                    {t("trimming.maxTokensBefore")}
                    <PopoverWithHoverWrapper message={t("trimming.maxTokensBeforeInfo")}>
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.maxTokensBeforeSummarization"
                    placeholder={t("trimming.defaultPlaceholder", { value: defaultMaxTokensBeforeSummarization })}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    {t("trimming.maxTokensAfter")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="trimming.maxTokensAfterInfo"
                                components={{ br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    type="number"
                    control={control}
                    name="trimming.maxTokensAfterSummarization"
                    placeholder={t("trimming.defaultPlaceholder", { value: defaultMaxTokensAfterSummarization })}
                />
            </FormGroup>
            <HistoryFields />
        </>
    );
}

// function TruncateFields() {
//     const { control } = useFormContext<EditAiAgentFormData>();

//     return (
//         <>
//             <FormGroup>
//                 <FormLabel>
//                     Messages length before truncate
//                     <PopoverWithHoverWrapper message="Truncation is triggered when this number of messages is exceeded.">
//                         <Icon icon="info-new" />
//                     </PopoverWithHoverWrapper>
//                 </FormLabel>
//                 <FormInput
//                     type="number"
//                     control={control}
//                     name="trimming.messagesLengthBeforeTruncate"
//                     placeholder={`Default (${defaultMessagesLengthBeforeTruncate})`}
//                 />
//             </FormGroup>
//             <FormGroup>
//                 <FormLabel>
//                     Messages length after truncate
//                     <PopoverWithHoverWrapper
//                         message={
//                             <>
//                                 The number of most recent messages to keep after truncation.
//                                 <br />
//                                 Older messages beyond this number will be discarded.
//                             </>
//                         }
//                     >
//                         <Icon icon="info-new" />
//                     </PopoverWithHoverWrapper>
//                 </FormLabel>
//                 <FormInput
//                     type="number"
//                     control={control}
//                     name="trimming.messagesLengthAfterTruncate"
//                     placeholder={`Default (${defaultMessagesLengthAfterTruncate})`}
//                 />
//             </FormGroup>
//             <HistoryFields />
//         </>
//     );
// }

function HistoryFields() {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <FormGroup marginClass="mb-0">
                <FormSwitch control={control} name="trimming.isEnableHistory">
                    {t("trimming.enableHistory")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="trimming.enableHistoryInfo"
                                components={{ code: <code /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            {formValues.trimming.isEnableHistory && (
                <FormGroup marginClass="mt-1">
                    <FormSwitch control={control} name="trimming.isSetHistoryExpiration">
                        {t("trimming.setHistoryExpiration")}
                        <PopoverWithHoverWrapper message={t("trimming.setHistoryExpirationInfo")}>
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                    {formValues.trimming.isSetHistoryExpiration && (
                        <FormDurationPicker
                            control={control}
                            name="trimming.historyExpirationInSeconds"
                            showDays
                            isFlexGrow
                        />
                    )}
                </FormGroup>
            )}
        </>
    );
}

// const defaultMessagesLengthBeforeTruncate = 500;
// const defaultMessagesLengthAfterTruncate = defaultMessagesLengthBeforeTruncate / 2;
const defaultMaxTokensBeforeSummarization = 32 * 1024;
const defaultMaxTokensAfterSummarization = 1024;
