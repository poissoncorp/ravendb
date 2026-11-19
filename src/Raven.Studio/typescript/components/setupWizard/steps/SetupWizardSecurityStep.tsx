import { useFormContext, useWatch } from "react-hook-form";
import { LicenseTypeToGenerate, SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import SetupWizardClickableCard from "../partials/SetupWizardClickableCard";
import Button from "react-bootstrap/Button";
import { FormCheckbox } from "components/common/Form";
import assertUnreachable from "components/utils/assertUnreachable";
import Badge from "react-bootstrap/Badge";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import Spinner from "react-bootstrap/Spinner";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEffect } from "react";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function SetupWizardSecurityStep() {
    const t = useStudioTranslation("setupWizard");
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const {
        securityStep: { securityOption },
        licenseKeyStep: { key, licenseInfo },
    } = useWatch({ control });

    const isLetsEncryptDisabled = !key;
    const isSecureRecommended = !!key && licenseInfo?.licenseStatus?.Type !== "Developer";

    const handleGoToGenerateLicense = (licenseType: LicenseTypeToGenerate) => {
        reportEvent(setupWizardGA4Prefixes.securityStep, "go-to-generate-license", licenseType);
        setValue("licenseKeyStep.licenseTypeToGenerate", licenseType);
        setValue("securityStep", setupWizardFormDefaultValues["securityStep"]);
        setValue("currentStep", "License key");
    };

    useEffect(() => {
        if (isSecureRecommended) {
            setValue("securityStep.securityOption", "letsEncrypt");
        }
    }, []);

    return (
        <div>
            <h2 className="mb-1">{t("securityStep.title")}</h2>
            <p className="mb-4 text-muted">{t("securityStep.description")}</p>
            <div className="mt-4">
                <h5 className="mb-1 d-flex align-items-center">
                    <Icon icon="lock" color="success" />
                    {t("securityStep.secureHeading")}
                    {isSecureRecommended && (
                        <PopoverWithHoverWrapper
                            message={
                                <div>
                                    <StudioTrans
                                        ns="setupWizard"
                                        i18nKey="securityStep.recommendedInfo"
                                        components={{ b: <b /> }}
                                    />
                                </div>
                            }
                        >
                            <Badge pill bg="faded-success" className="ms-1">
                                {t("securityStep.recommended")}
                            </Badge>
                        </PopoverWithHoverWrapper>
                    )}
                </h5>
                <ConditionalPopover
                    className="w-100"
                    conditions={{
                        isActive: isLetsEncryptDisabled,
                        message: (
                            <div>
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="securityStep.letsEncryptUnavailable"
                                    components={{
                                        b: <b />,
                                        community: (
                                            <Button
                                                variant="link"
                                                className="text-info p-0 text-decoration-underline"
                                                onClick={() => handleGoToGenerateLicense("community")}
                                            />
                                        ),
                                        developer: (
                                            <Button
                                                variant="link"
                                                className="text-developer p-0 text-decoration-underline"
                                                onClick={() => handleGoToGenerateLicense("developer")}
                                            />
                                        ),
                                    }}
                                />
                            </div>
                        ),
                    }}
                >
                    <SetupWizardClickableCard
                        className="w-100"
                        icon="lets-encrypt"
                        title={t("securityStep.letsEncrypt.title")}
                        description={t("securityStep.letsEncrypt.description")}
                        isSelected={securityOption === "letsEncrypt"}
                        onClick={() => {
                            setValue("securityStep.securityOption", "letsEncrypt", {
                                shouldDirty: true,
                            });
                            reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "letsEncrypt");
                        }}
                        isDisabled={isLetsEncryptDisabled}
                        popoverMessage={
                            <ul className="mb-0 ps-3">
                                <li>{t("securityStep.letsEncrypt.defaultChoice")}</li>
                                <li className="mt-1">{t("securityStep.letsEncrypt.autoRenew")}</li>
                                <li className="mt-1">{t("securityStep.letsEncrypt.standardDeployments")}</li>
                            </ul>
                        }
                    />
                </ConditionalPopover>
                <SetupWizardClickableCard
                    className="mt-2 w-100"
                    icon="certificate"
                    title={t("securityStep.ownCertificate.title")}
                    description={t("securityStep.ownCertificate.description")}
                    isSelected={securityOption === "ownCertificate"}
                    onClick={() => {
                        setValue("securityStep.securityOption", "ownCertificate", {
                            shouldDirty: true,
                        });
                        reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "ownCertificate");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>{t("securityStep.ownCertificate.customCertificate")}</li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="securityStep.ownCertificate.productionEnvironments"
                                    components={{ b: <b /> }}
                                />
                            </li>
                        </ul>
                    }
                />
            </div>
            <div className="my-4">
                <h5 className="mb-1">
                    <Icon icon="lock" color="warning" />
                    {t("securityStep.unsecureHeading")}
                </h5>
                <SetupWizardClickableCard
                    icon="empty-set"
                    title={t("securityStep.noCertificate.title")}
                    description={t("securityStep.noCertificate.description")}
                    isSelected={securityOption === "none"}
                    onClick={() => {
                        setValue("securityStep.securityOption", "none", {
                            shouldDirty: true,
                        });
                        reportEvent(setupWizardGA4Prefixes.securityStep, "select-option", "none");
                    }}
                    popoverMessage={
                        <ul className="mb-0 ps-3">
                            <li>
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="securityStep.noCertificate.trustedEnvironments"
                                    components={{ b: <b /> }}
                                />
                            </li>
                            <li className="mt-1">
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="securityStep.noCertificate.notForProduction"
                                    components={{ b: <b /> }}
                                />
                            </li>
                        </ul>
                    }
                />
            </div>
        </div>
    );
}

export function SetupWizardSecurityStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { control, setValue } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();
    const { reportEvent } = useEventsCollector();

    const {
        securityStep: { securityOption, isLetsEncryptAgreementAccepted },
        licenseKeyStep: { key, licenseInfo },
    } = useWatch({ control });

    const asyncGetLetsEncryptAgreement = useAsync(async () => {
        if (!key) {
            return null;
        }
        return setupWizardService.getLetsEncryptAgreement(licenseInfo?.userDomainsWithIps?.email?.[0] ?? "");
    }, []);

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.securityStep, "back");
        setValue("currentStep", "License key");
        setValue("securityStep", setupWizardFormDefaultValues["securityStep"]);
    };

    const handleContinue = () => {
        reportEvent(setupWizardGA4Prefixes.securityStep, "continue", securityOption);
        switch (securityOption) {
            case "letsEncrypt":
                setValue("currentStep", "Domain");
                break;
            case "ownCertificate":
                setValue("currentStep", "Self-signed certificate");
                break;
            case "none":
                setValue("currentStep", "Node addresses");
                break;
            default:
                assertUnreachable(securityOption);
        }
    };

    const isContinueDisabled =
        (!isLetsEncryptAgreementAccepted && securityOption === "letsEncrypt") || securityOption === null;

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> {t("actions.back")}
            </Button>
            <div className="hstack gap-2 align-items-center">
                {securityOption === "letsEncrypt" && (
                    <FormCheckbox
                        disabled={asyncGetLetsEncryptAgreement.loading}
                        className="mb-0"
                        control={control}
                        name="securityStep.isLetsEncryptAgreementAccepted"
                    >
                        <StudioTrans
                            ns="setupWizard"
                            i18nKey="securityStep.acceptAgreement"
                            components={{
                                spinner: asyncGetLetsEncryptAgreement.loading ? <Spinner /> : <></>,
                                a: <a target="_blank" href={asyncGetLetsEncryptAgreement.result as string} />,
                            }}
                        />
                    </FormCheckbox>
                )}
                <Button
                    disabled={isContinueDisabled}
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleContinue}
                >
                    {t("actions.continue")} <Icon icon="arrow-right" margin="m-0" />
                </Button>
            </div>
        </div>
    );
}
