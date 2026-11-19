import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import SetupWizardClickableCard from "../partials/SetupWizardClickableCard";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import assertUnreachable from "components/utils/assertUnreachable";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { SetupWizardStepItem } from "components/setupWizard/partials/SetupWizardStepItem";
import { NumberedList } from "components/common/NumberedList";
import { useEffect } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function SetupWizardSetupMethodStep() {
    const t = useStudioTranslation("setupWizard");
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const {
        setupMethodStep: { method: selectedMethod },
    } = useWatch({ control });
    const { reportEvent } = useEventsCollector();

    useEffect(() => {
        setValue("setupMethodStep.method", selectedMethod ?? "newCluster");
    }, []);

    return (
        <div>
            <h2 className="mb-1">{t("setupMethodStep.title")}</h2>
            <p className="mb-4 text-muted">
                <StudioTrans ns="setupWizard" i18nKey="setupMethodStep.description" components={{ br: <br /> }} />
            </p>
            <div className="mt-4">
                <h5 className="mb-1">{t("setupMethodStep.newSetupHeading")}</h5>
                <SetupWizardClickableCard
                    icon="server"
                    title={t("setupMethodStep.newCluster.title")}
                    description={t("setupMethodStep.newCluster.description")}
                    isSelected={selectedMethod === "newCluster"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "newCluster");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "newCluster");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>{t("setupMethodStep.newCluster.firstDeployment")}</li>
                            <li>{t("setupMethodStep.newCluster.singleOrMultiNode")}</li>
                            <li>{t("setupMethodStep.newCluster.fromScratch")}</li>
                        </ul>
                    }
                />
                <SetupWizardClickableCard
                    className="mt-2"
                    icon="default"
                    title={t("setupMethodStep.createPackage.title")}
                    description={t("setupMethodStep.createPackage.description")}
                    isSelected={selectedMethod === "createPackage"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "createPackage");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "createPackage");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="setupMethodStep.createPackage.externalEnvironment"
                                    components={{ br: <br /> }}
                                />
                            </li>
                            <li>{t("setupMethodStep.createPackage.withoutLocalServer")}</li>
                            <li>{t("setupMethodStep.createPackage.remoteInstallations")}</li>
                        </ul>
                    }
                />
            </div>
            <div className="my-4">
                <h5 className="mb-1">{t("setupMethodStep.existingPackageHeading")}</h5>
                <SetupWizardClickableCard
                    icon="default"
                    addon="arrow-up"
                    title={t("setupMethodStep.usePackage.title")}
                    description={t("setupMethodStep.usePackage.description")}
                    isSelected={selectedMethod === "usePackage"}
                    onClick={() => {
                        setValue("setupMethodStep.method", "usePackage");
                        reportEvent(setupWizardGA4Prefixes.setupMethod, "select-method", "usePackage");
                    }}
                    popoverMessage={
                        <>
                            <ul className="ps-3">
                                <li>{t("setupMethodStep.usePackage.additionalNode")}</li>
                                <li>{t("setupMethodStep.usePackage.preconfiguredCluster")}</li>
                            </ul>
                            <p className="mb-0">
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="setupMethodStep.usePackage.note"
                                    components={{ br: <br />, strong: <strong /> }}
                                />
                            </p>
                        </>
                    }
                />
            </div>
        </div>
    );
}

export function SetupWizardSetupMethodStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { control, setValue } = useFormContext<SetupWizardFormData>();

    const {
        setupMethodStep: { method: selectedMethod },
    } = useWatch({ control });

    const { reportEvent } = useEventsCollector();

    const handleContinue = async () => {
        // Report continue with selected method
        if (selectedMethod) {
            reportEvent(setupWizardGA4Prefixes.setupMethod, "continue", selectedMethod);
        }
        switch (selectedMethod) {
            case "newCluster":
            case "createPackage":
                setValue("currentStep", "License key");
                break;
            case "usePackage":
                setValue("currentStep", "Use setup package");
                break;
            default:
                assertUnreachable(selectedMethod);
        }
    };

    return (
        <div className="d-flex justify-content-between">
            <PopoverWithHoverWrapper
                message={
                    <SetupWizardInfoPopover
                        description={
                            <NumberedList>
                                <SetupWizardStepItem stepIndicator={1}>
                                    <span>
                                        <StudioTrans
                                            ns="setupWizard"
                                            i18nKey="setupMethodStep.manualSetup.openSettings"
                                            components={{ em: <em /> }}
                                        />
                                    </span>
                                </SetupWizardStepItem>
                                <SetupWizardStepItem stepIndicator={2}>
                                    <span>
                                        <StudioTrans
                                            ns="setupWizard"
                                            i18nKey="setupMethodStep.manualSetup.changeSetupMode"
                                            components={{ code: <code /> }}
                                            options={{ example: '"Setup.Mode": "None"' }}
                                        />
                                    </span>
                                </SetupWizardStepItem>
                            </NumberedList>
                        }
                        ravenLinkHash="3VLEQL"
                    />
                }
            >
                <span className="md-label mb-0">
                    <Icon icon="info" />
                    {t("setupMethodStep.manualSetup.title")}
                </span>
            </PopoverWithHoverWrapper>
            <ConditionalPopover
                conditions={{
                    isActive: !selectedMethod,
                    message: t("setupMethodStep.completeStep"),
                }}
                popoverPlacement="top"
            >
                <Button variant="primary" className="rounded-pill" onClick={handleContinue} disabled={!selectedMethod}>
                    {t("actions.continue")} <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </ConditionalPopover>
        </div>
    );
}
