import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import { FormGroup, FormLabel, FormSelect, FormSelectAutocomplete } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import React, { useEffect, useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "hooks/useServices";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import InputGroupText from "react-bootstrap/InputGroupText";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Button from "react-bootstrap/Button";
import messagePublisher from "common/messagePublisher";
import { has } from "lodash";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export function SetupWizardDomainStep() {
    const t = useStudioTranslation("setupWizard");
    const { control, setValue, setError, clearErrors } = useFormContext<SetupWizardFormData>();
    const { domainStep, licenseKeyStep } = useWatch({ control });
    const { setupWizardService } = useServices();
    const { licenseInfo } = licenseKeyStep;
    const [domainsOptions, setDomainsOptions] = useState<SelectOption[]>(
        Object.keys(licenseInfo?.userDomainsWithIps?.domains ?? []).map((domain) => ({
            value: domain,
            label: domain,
        }))
    );

    useDomainFormSideEffects();

    const hasDnsRecords =
        domainStep.domain && Object.keys(licenseInfo?.userDomainsWithIps?.domains ?? []).includes(domainStep.domain);

    const asyncCheckDomainAvailability = useAsyncCallback(async (domain: string) => {
        const key = JSON.parse(licenseKeyStep.key) ?? "";

        return setupWizardService.checkDomainAvailability(domain, key);
    });

    const handleDomainAvailability = async (rawDomain: string) => {
        const domain = (rawDomain ?? "").trim();
        const newOption = createNewOption(domain);
        try {
            const domainAvailability = await asyncCheckDomainAvailability.execute(domain);

            if (domainAvailability.Available) {
                // Only add to options when confirmed available
                setDomainsOptions((prev) => {
                    const exists = prev.some((o) => o.value === newOption.value);
                    return exists ? prev : [...prev, newOption];
                });
                clearErrors("domainStep.domain");
                setValue("domainStep.domain", domain);
                return domain;
            } else {
                setDomainsOptions((prev) => prev.filter((option) => option.value !== newOption.value));
                setError("domainStep.domain", {
                    type: "value",
                    message: t("domainStep.domainInUse"),
                });
                return domain;
            }
        } catch (e) {
            setDomainsOptions((prev) => prev.filter((option) => option.value !== newOption.value));
            setError("domainStep.domain", {
                type: "value",
                message: t("domainStep.domainCheckError"),
            });
            console.error("Error", e);
            return domain;
        }
    };

    const rootDomainOptions = (licenseInfo?.userDomainsWithIps?.rootDomains ?? []).map((domain) => ({
        value: domain,
        label: `.${domain}`,
    }));

    return (
        <div>
            <h2 className="mb-1">{t("domainStep.title")}</h2>
            <p className="mb-4 text-muted">
                <StudioTrans
                    ns="setupWizard"
                    i18nKey="domainStep.description"
                    components={{ code: <code />, br: <br /> }}
                    options={{ rootDomain: domainStep.rootDomain ?? "" }}
                />
            </p>
            <FormGroup marginClass="mb-2">
                <FormLabel className="hstack">
                    <div>{t("domainStep.domain")}</div>
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <ul className="mb-0 ps-3">
                                    <li>
                                        <StudioTrans
                                            ns="setupWizard"
                                            i18nKey="domainStep.domainInfo"
                                            components={{ code: <code />, br: <br /> }}
                                            options={{ rootDomain: domainStep.rootDomain ?? "" }}
                                        />
                                    </li>
                                    <li className="mt-1">{t("domainStep.domainReachable")}</li>
                                </ul>
                                <RichAlert icon="info" variant="info" className="mt-2">
                                    <StudioTrans
                                        ns="setupWizard"
                                        i18nKey="domainStep.domainCharacters"
                                        components={{ code: <code /> }}
                                    />
                                </RichAlert>
                            </>
                        }
                        placement="right"
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelectAutocomplete
                    isLoading={asyncCheckDomainAvailability.loading}
                    onCreateOption={handleDomainAvailability}
                    control={control}
                    controlShouldRenderValue
                    name="domainStep.domain"
                    options={domainsOptions}
                    placeholder={t("domainStep.domainPlaceholder")}
                    onBlur={async () => {
                        const trimmedDomain = (domainStep.domain ?? "").trim();
                        if (!trimmedDomain) {
                            return;
                        }
                        const exists = domainsOptions.some((o) => o.value === trimmedDomain);
                        if (!exists) {
                            await handleDomainAvailability(trimmedDomain);
                        }
                    }}
                    addon={
                        rootDomainOptions.length === 1 ? (
                            <span>{rootDomainOptions[0].label}</span>
                        ) : (
                            <FormSelect
                                control={control}
                                name="domainStep.rootDomain"
                                options={rootDomainOptions}
                                isSearchable={false}
                            />
                        )
                    }
                />
            </FormGroup>
            {hasDnsRecords && (
                <RichAlert icon="info" variant="info">
                    {t("domainStep.hasDnsRecords")}
                </RichAlert>
            )}
            <FormGroup className="mt-3">
                <FormLabel className="hstack">
                    <div>{t("domainStep.email")}</div>
                    <PopoverWithHoverWrapper message={t("domainStep.emailInfo")} placement="right">
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                {/* height is set to 38px to match the height of the input field*/}
                <InputGroupText style={{ height: "38px" }}>{domainStep.email}</InputGroupText>
            </FormGroup>
        </div>
    );
}

const useDomainFormSideEffects = () => {
    const { setValue, control, watch, clearErrors } = useFormContext<SetupWizardFormData>();
    const {
        licenseKeyStep: { licenseInfo },
    } = useWatch({ control });

    useEffect(() => {
        if (licenseInfo?.userDomainsWithIps?.email.length === 1) {
            setValue("domainStep.email", licenseInfo.userDomainsWithIps.email[0]);
        }

        if (licenseInfo?.userDomainsWithIps?.rootDomains.length > 0) {
            setValue("domainStep.rootDomain", licenseInfo.userDomainsWithIps.rootDomains[0]); // select first root domain as default
        }

        if (Object.keys(licenseInfo?.userDomainsWithIps?.domains ?? []).length === 1) {
            setValue("domainStep.domain", Object.keys(licenseInfo.userDomainsWithIps.domains)[0]);
        }
    }, []);

    useEffect(() => {
        const subscription = watch((values, { name }) => {
            if (name === "domainStep.domain") {
                // Clear validation error as soon as user types, but do not modify the value.
                clearErrors("domainStep.domain");
            }
        });

        return () => subscription.unsubscribe();
    }, [watch, clearErrors]);
};

const createNewOption = (label: string) => ({
    label,
    value: label,
});

export function SetupWizardDomainStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { setValue, control } = useFormContext<SetupWizardFormData>();
    const { setupWizardService } = useServices();
    const {
        domainStep,
        licenseKeyStep,
        securityStep: { securityOption },
    } = useWatch({ control });

    const asyncClaimDomain = useAsyncCallback(async () => {
        const domain = domainStep.domain;
        const key = JSON.parse(licenseKeyStep.key);

        if (domain) {
            const domains = licenseKeyStep?.licenseInfo?.userDomainsWithIps?.domains;
            if (!has(domains, domain)) {
                await setupWizardService.claimDomain(domain, key);
            }
            setValue("currentStep", "Node addresses");
        } else {
            messagePublisher.reportError(t("domainStep.domainRequired"));
        }
    });

    const handleBack = () => {
        switch (securityOption) {
            case "ownCertificate":
                setValue("currentStep", "Self-signed certificate");
                break;
            case "letsEncrypt":
            case "none":
                setValue("currentStep", "Security");
                break;
        }
        setValue("domainStep", setupWizardFormDefaultValues["domainStep"]);
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> {t("actions.back")}
            </Button>
            <ButtonWithSpinner
                isSpinning={asyncClaimDomain.loading}
                variant="primary"
                className="rounded-pill"
                onClick={asyncClaimDomain.execute}
            >
                {t("actions.continue")}&nbsp;
                <Icon icon="arrow-right" margin="m-0" />
            </ButtonWithSpinner>
        </div>
    );
}
