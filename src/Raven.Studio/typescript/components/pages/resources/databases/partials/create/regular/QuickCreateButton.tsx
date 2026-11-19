import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { PropSummary, PropSummaryItem, PropSummaryName, PropSummaryValue } from "components/common/PropSummary";
import { CreateDatabaseRegularFormData } from "./createDatabaseRegularValidation";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ReactNode } from "react";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface QuickCreateButtonProps {
    formValues: CreateDatabaseRegularFormData;
    isSubmitting: boolean;
    disabledReason: ReactNode;
    handleQuickCreate: () => void;
}

export default function QuickCreateButton({
    formValues,
    isSubmitting,
    disabledReason,
    handleQuickCreate,
}: QuickCreateButtonProps) {
    const t = useStudioTranslation("databases");

    return (
        <PopoverWithHoverWrapper
            isInPopoverBody={!!disabledReason}
            message={disabledReason ?? <ConfigurationSummary formValues={formValues} />}
        >
            <ButtonWithSpinner
                type="button"
                onClick={handleQuickCreate}
                className="rounded-pill me-1"
                variant="secondary"
                icon="star"
                isSpinning={isSubmitting}
                disabled={!!disabledReason}
                title={t("createDatabase.quickCreate.title")}
            >
                {t("createDatabase.quickCreate.button")}
            </ButtonWithSpinner>
        </PopoverWithHoverWrapper>
    );
}

function ConfigurationSummary({ formValues }: { formValues: CreateDatabaseRegularFormData }) {
    const t = useStudioTranslation("databases");

    return (
        <PropSummary>
            <PropSummaryItem>
                <PropSummaryName>
                    <Icon icon="encryption" /> {t("createDatabase.quickCreate.encryption")}
                </PropSummaryName>
                {formValues.basicInfoStep.isEncrypted ? (
                    <PropSummaryValue color="success"> {t("createDatabase.quickCreate.on")}</PropSummaryValue>
                ) : (
                    <PropSummaryValue color="danger"> {t("createDatabase.quickCreate.off")}</PropSummaryValue>
                )}
            </PropSummaryItem>

            <PropSummaryItem>
                <PropSummaryName>
                    <Icon icon="replication" /> {t("createDatabase.quickCreate.replication")}
                </PropSummaryName>
                {formValues.replicationAndShardingStep.replicationFactor > 1 ? (
                    <PropSummaryValue color="success"> {t("createDatabase.quickCreate.on")}</PropSummaryValue>
                ) : (
                    <PropSummaryValue color="danger"> {t("createDatabase.quickCreate.off")}</PropSummaryValue>
                )}
            </PropSummaryItem>

            <PropSummaryItem>
                <PropSummaryName>
                    <Icon icon="sharding" /> {t("createDatabase.quickCreate.sharding")}
                </PropSummaryName>
                {formValues.replicationAndShardingStep.isSharded ? (
                    <PropSummaryValue color="success"> {t("createDatabase.quickCreate.on")}</PropSummaryValue>
                ) : (
                    <PropSummaryValue color="danger"> {t("createDatabase.quickCreate.off")}</PropSummaryValue>
                )}
            </PropSummaryItem>

            {formValues.replicationAndShardingStep.isManualReplication && (
                <PropSummaryItem>
                    <PropSummaryName>
                        <Icon icon="node" /> {t("createDatabase.quickCreate.manualNodeSelection")}
                    </PropSummaryName>
                    <PropSummaryValue color="success"> {t("createDatabase.quickCreate.on")}</PropSummaryValue>
                </PropSummaryItem>
            )}

            <PropSummaryItem>
                <PropSummaryName>
                    {formValues.dataDirectoryStep.isDefault ? (
                        <>
                            <Icon icon="path" />{" "}
                            <StudioTrans
                                ns="databases"
                                i18nKey="createDatabase.quickCreate.path"
                                options={{ context: "default" }}
                                components={{ strong: <strong /> }}
                            />
                        </>
                    ) : (
                        <>
                            <Icon icon="path" />{" "}
                            <StudioTrans
                                ns="databases"
                                i18nKey="createDatabase.quickCreate.path"
                                options={{ context: "custom" }}
                                components={{ strong: <strong className="text-success" /> }}
                            />
                        </>
                    )}
                </PropSummaryName>
            </PropSummaryItem>
        </PropSummary>
    );
}
