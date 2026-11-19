import { SetupWizardStepId, SetupWizardSetupMethod, SetupWizardSecurityOption } from "../setupWizardValidation";
import {
    SetupWizardAdditionalSettingsStep,
    SetupWizardAdditionalSettingsStepFooter,
} from "../steps/SetupWizardAdditionalSettingsStep";
import { SetupWizardDomainStep, SetupWizardDomainStepFooter } from "../steps/SetupWizardDomainStep";
import { SetupWizardEulaStep, SetupWizardEulaStepFooter } from "../steps/SetupWizardEulaStep";
import { SetupWizardFinishStep, SetupWizardFinishStepFooter } from "../steps/SetupWizardFinishStep";
import { SetupWizardLicenseKeyStep, SetupWizardLicenseKeyStepFooter } from "../steps/SetupWizardLicenseKeyStep";
import { SetupWizardNodeAddressStep, SetupWizardNodeAddressStepFooter } from "../steps/SetupWizardNodeAddressStep";
import { SetupWizardSecurityStep, SetupWizardSecurityStepFooter } from "../steps/SetupWizardSecurityStep";
import {
    SetupWizardSelfSignedCertificateStep,
    SetupWizardSelfSignedCertificateStepFooter,
} from "../steps/SetupWizardSelfSignedCertificateStep";
import { SetupWizardSetupMethodStep, SetupWizardSetupMethodStepFooter } from "../steps/SetupWizardSetupMethodStep";
import { SetupWizardSummaryStep, SetupWizardSummaryStepFooter } from "../steps/SetupWizardSummaryStep";
import { SetupWizardUsePackageStep, SetupWizardUsePackageStepFooter } from "../steps/SetupWizardUsePackageStep";
import { useAppSelector } from "components/store";
import { setupWizardSelectors } from "components/setupWizard/store/setupWizardSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface SetupWizardStep {
    title: SetupWizardStepId;
    label: string;
    description: string;
    component: React.ReactNode;
    footer: React.ReactNode;
    isCurrent?: boolean;
    isAvailable?: boolean;
    isVisible?: boolean;
}

export interface SetupWizardStepsProps {
    currentStep: SetupWizardStepId;
    setupMethod: SetupWizardSetupMethod;
    securityOption: SetupWizardSecurityOption;
}

export function useSetupWizardSteps({
    currentStep,
    setupMethod,
    securityOption,
}: SetupWizardStepsProps): SetupWizardStep[] {
    const t = useStudioTranslation("setupWizard");
    const finishStatus = useAppSelector(setupWizardSelectors.finishStepStatus);
    const getIsNotInStepIds = (stepIds: SetupWizardStepId[]) => !stepIds.some((x) => currentStep === x);

    const steps: SetupWizardStep[] = [
        {
            title: "Eula",
            label: t("steps.eula.title"),
            description: t("steps.eula.description"),
            component: <SetupWizardEulaStep />,
            footer: <SetupWizardEulaStepFooter />,
            isCurrent: currentStep === "Eula",
            isAvailable: true,
            isVisible: false,
        },
        {
            title: "Setup method",
            label: t("steps.setupMethod.title"),
            description: t("steps.setupMethod.description", { context: setupMethod }),
            component: <SetupWizardSetupMethodStep />,
            footer: <SetupWizardSetupMethodStepFooter />,
            isCurrent: currentStep === "Setup method",
            isAvailable: true,
            isVisible: getIsNotInStepIds(["Eula"]),
        },
        {
            title: "Use setup package",
            label: t("steps.usePackage.title"),
            description: t("steps.usePackage.description"),
            component: <SetupWizardUsePackageStep />,
            footer: <SetupWizardUsePackageStepFooter />,
            isCurrent: currentStep === "Use setup package",
            isAvailable: setupMethod === "usePackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method"]),
        },
        {
            title: "License key",
            label: t("steps.licenseKey.title"),
            description: t("steps.licenseKey.description"),
            component: <SetupWizardLicenseKeyStep />,
            footer: <SetupWizardLicenseKeyStepFooter />,
            isCurrent: currentStep === "License key",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Use setup package"]),
        },
        {
            title: "Security",
            label: t("steps.security.title"),
            description: t("steps.security.description", { context: securityOption }),
            component: <SetupWizardSecurityStep />,
            footer: <SetupWizardSecurityStepFooter />,
            isCurrent: currentStep === "Security",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Use setup package"]),
        },
        {
            title: "Self-signed certificate",
            label: t("steps.selfSignedCertificate.title"),
            description: t("steps.selfSignedCertificate.description"),
            component: <SetupWizardSelfSignedCertificateStep />,
            footer: <SetupWizardSelfSignedCertificateStepFooter />,
            isCurrent: currentStep === "Self-signed certificate",
            isAvailable:
                (setupMethod === "newCluster" || setupMethod === "createPackage") &&
                securityOption === "ownCertificate",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "Security"]),
        },
        {
            title: "Domain",
            label: t("steps.domain.title"),
            description: t("steps.domain.description"),
            component: <SetupWizardDomainStep />,
            footer: <SetupWizardDomainStepFooter />,
            isCurrent: currentStep === "Domain",
            isAvailable:
                (setupMethod === "newCluster" || setupMethod === "createPackage") && securityOption === "letsEncrypt",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Node addresses",
            label: t("steps.nodeAddresses.title"),
            description: t("steps.nodeAddresses.description"),
            component: <SetupWizardNodeAddressStep />,
            footer: <SetupWizardNodeAddressStepFooter />,
            isCurrent: currentStep === "Node addresses",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Additional settings",
            label: t("steps.additionalSettings.title"),
            description: t("steps.additionalSettings.description"),
            component: <SetupWizardAdditionalSettingsStep />,
            footer: <SetupWizardAdditionalSettingsStepFooter />,
            isCurrent: currentStep === "Additional settings",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
        {
            title: "Summary",
            label: t("steps.summary.title"),
            description: t("steps.summary.description"),
            component: <SetupWizardSummaryStep />,
            footer: <SetupWizardSummaryStepFooter />,
            isCurrent: currentStep === "Summary",
            isAvailable: setupMethod === "newCluster" || setupMethod === "createPackage",
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security", "Use setup package"]),
        },
        {
            title: "Finish",
            label: t("steps.finish.title"),
            description: t("steps.finish.description", {
                context: finishStatus === "Completed" ? "completed" : undefined,
            }),
            component: <SetupWizardFinishStep />,
            footer: <SetupWizardFinishStepFooter />,
            isCurrent: currentStep === "Finish",
            isAvailable: true,
            isVisible: getIsNotInStepIds(["Eula", "Setup method", "License key", "Security"]),
        },
    ];

    return steps.filter((x) => x.isAvailable);
}
