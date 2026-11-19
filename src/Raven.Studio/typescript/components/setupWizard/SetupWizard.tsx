import "./SetupWizard.scss";
import { Icon } from "components/common/Icon";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { SetupWizardFormData, getSetupWizardSchema, SetupWizardStepId } from "./setupWizardValidation";
import { yupResolver } from "@hookform/resolvers/yup";
import { NumberedList } from "components/common/NumberedList";
import classNames from "classnames";
import { SetupWizardStepItem } from "./partials/SetupWizardStepItem";
import { useSetupWizardSteps } from "./hooks/useSetupWizardSteps";
import useConfirm from "components/common/ConfirmDialog";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { useRavenLink } from "hooks/useRavenLink";
import { useAppSelector } from "components/store";
import { setupWizardSelectors } from "components/setupWizard/store/setupWizardSlice";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import { useStudioTranslation } from "hooks/useStudioTranslation";

const ravenLogo = require("Content/img/ravendb_logo.svg");
const ravenSidebarImg = require("Content/img/setupWizard/setup-wizard-sidebar-background.png");

export default function SetupWizard() {
    const t = useStudioTranslation("setupWizard");
    const docsLink = useRavenLink({ hash: "37GM2Z" });
    const form = useForm<SetupWizardFormData>({
        resolver: yupResolver(getSetupWizardSchema(t)),
        mode: "onChange",
        defaultValues: setupWizardFormDefaultValues,
    });

    const { handleSubmit, control, setValue, getValues } = form;

    const formValues = useWatch({ control });

    const steps = useSetupWizardSteps({
        currentStep: formValues.currentStep,
        setupMethod: formValues.setupMethodStep.method,
        securityOption: formValues.securityStep.securityOption,
    });

    const finishStatus = useAppSelector(setupWizardSelectors.finishStepStatus);

    const confirm = useConfirm();

    const { reportEvent } = useEventsCollector();

    const currentStepIdx = steps.findIndex((x) => x.isCurrent);

    const getStepLabel = (stepId: SetupWizardStepId) => steps.find((s) => s.title === stepId)?.label ?? stepId;

    const getStepKey = (stepTitle: SetupWizardStepId): keyof SetupWizardFormData => {
        const stepKeyMap: Partial<Record<SetupWizardStepId, keyof SetupWizardFormData>> = {
            "Setup method": "setupMethodStep",
            "Use setup package": "usePackageStep",
            "License key": "licenseKeyStep",
            Domain: "domainStep",
            Security: "securityStep",
            "Self-signed certificate": "selfSignedCertificateStep",
            "Node addresses": "nodeAddressStep",
            "Additional settings": "additionalSettingsStep",
        };

        return stepKeyMap[stepTitle];
    };

    const handleStepNavigation = async (stepTitle: SetupWizardStepId) => {
        if (finishStatus === "Completed") {
            return; // if finish status is completed, we don't need to navigate to any other step
        }

        const targetStepIdx = steps.findIndex((s) => s.title === stepTitle);

        if (targetStepIdx > currentStepIdx) {
            return;
        }

        if (targetStepIdx < currentStepIdx) {
            const currentValues = getValues();

            const stepsToClear = steps
                .filter((_, idx) => idx > targetStepIdx && idx <= currentStepIdx)
                .map((s) => s.title);

            if (stepsToClear.length > 0) {
                const dirtyStepsToClear = stepsToClear.filter((stepTitle) => {
                    const stepKey = getStepKey(stepTitle);
                    if (stepKey === "currentStep") {
                        return false;
                    }

                    const fieldState = form.getFieldState(stepKey);
                    return fieldState.isDirty;
                });

                if (dirtyStepsToClear.length > 0 && currentValues.finishStep.finishingStatus !== "Faulted") {
                    const isConfirmed = await confirm({
                        title: t("goBackConfirm.title", { step: getStepLabel(stepTitle) }),
                        message: (
                            <div>
                                <p>{t("goBackConfirm.clearsData")}</p>
                                <ul>
                                    {dirtyStepsToClear.map((step) => (
                                        <li key={step}>
                                            <strong>{getStepLabel(step)}</strong>
                                        </li>
                                    ))}
                                </ul>
                                <p>{t("goBackConfirm.question")}</p>
                            </div>
                        ),
                        actionColor: "warning",
                        icon: "warning",
                        confirmText: t("goBackConfirm.confirm"),
                    });

                    if (!isConfirmed) {
                        return;
                    }
                }
            }

            stepsToClear.forEach((stepTitle) => {
                const stepKey = getStepKey(stepTitle);
                if (stepKey !== "currentStep" && stepKey in setupWizardFormDefaultValues && stepKey in currentValues) {
                    setValue(stepKey, setupWizardFormDefaultValues[stepKey]);
                }
            });
        }

        if (targetStepIdx < currentStepIdx) {
            const fromTitle = steps[currentStepIdx]?.title;
            const toTitle = stepTitle;
            const label = fromTitle && toTitle ? `${fromTitle} -> ${toTitle}` : toTitle;
            reportEvent(setupWizardGA4Prefixes.navigation, "back", label);
        }

        setValue("currentStep", stepTitle);
    };

    return (
        <FormProvider {...form}>
            <form onSubmit={handleSubmit(console.log)} className="h-100">
                <div className="setup-wizard-container">
                    <div className="setup-wizard-main">
                        <div className="d-flex flex-column h-100 w-75">
                            <div className="logo-container position-sticky top-0 py-3 z-3">
                                <img src={ravenLogo} alt={t("logoAlt")} width="120" />
                            </div>
                            {steps[currentStepIdx].component}
                        </div>
                    </div>
                    <div className="setup-wizard-footer">
                        <div className="w-75">{steps[currentStepIdx].footer}</div>
                    </div>
                    <div className={classNames("setup-wizard-sidebar", { "p-0": formValues.currentStep === "Eula" })}>
                        {formValues.currentStep === "Eula" ? (
                            <img className="h-100 object-fit-cover" src={ravenSidebarImg} alt={t("steps.eula.title")} />
                        ) : (
                            <>
                                <div className="flex-grow">
                                    <NumberedList>
                                        {steps.map((step, idx) => (
                                            <SetupWizardStepItem
                                                key={step.title}
                                                isCurrent={step.isCurrent}
                                                isChecked={idx < currentStepIdx}
                                                isInactive={idx > currentStepIdx}
                                                className={classNames("cursor-pointer", {
                                                    "d-none": !step.isVisible,
                                                    "cursor-not-allowed":
                                                        idx > currentStepIdx || finishStatus === "Completed",
                                                })}
                                                onClick={() => handleStepNavigation(step.title)}
                                                title={
                                                    finishStatus === "Completed"
                                                        ? t("stepNavigation.completed")
                                                        : idx > currentStepIdx
                                                          ? t("stepNavigation.completePrevious")
                                                          : t("stepNavigation.goToStep")
                                                }
                                            >
                                                <div
                                                    className={classNames("vstack gap-1", {
                                                        "opacity-25": idx > currentStepIdx,
                                                    })}
                                                >
                                                    <h5 className="mb-0">{step.label}</h5>
                                                    <small className="text-muted">{step.description}</small>
                                                </div>
                                            </SetupWizardStepItem>
                                        ))}
                                    </NumberedList>
                                </div>

                                <div className="d-flex flex-column gap-3">
                                    <Icon icon="lifebuoy" className="fs-3" margin="m-0" />
                                    <div className="vstack gap-1">
                                        <h4 className="mb-0">{t("help.title")}</h4>
                                        <p className="text-muted mb-0" style={{ lineHeight: 1.3 }}>
                                            {t("help.description")}
                                        </p>
                                    </div>
                                    <a
                                        target="_blank"
                                        className="btn btn-outline-secondary w-fit-content"
                                        href={docsLink}
                                    >
                                        {t("help.seeDocumentation")} <Icon icon="newtab" margin="m-0" />
                                    </a>
                                </div>
                            </>
                        )}
                    </div>
                </div>
            </form>
        </FormProvider>
    );
}
