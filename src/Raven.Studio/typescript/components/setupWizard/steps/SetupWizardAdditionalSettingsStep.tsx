import { Control, useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { FormGroup, FormInput, FormLabel, FormPathSelector, FormSelect, FormSwitch } from "components/common/Form";
import { RichAlert } from "components/common/RichAlert";
import { setupWizardConstants, setupWizardGA4Prefixes } from "components/setupWizard/utils/setupWizardConstants";
import { HrHeader } from "components/common/HrHeader";
import classNames from "classnames";
import { useEffect, useMemo } from "react";
import { getFullDomain, getLicenseType, sanitizeCommonName } from "components/setupWizard/utils/setupWizardUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useServices } from "hooks/useServices";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { OperatingSystem, useOS } from "hooks/useOS";
import { SetupWizardInfoPopover } from "components/setupWizard/partials/SetupWizardInfoPopover";
import { setupWizardFormDefaultValues } from "components/setupWizard/utils/setupWizardFormDefaultValues";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

export function SetupWizardAdditionalSettingsStep() {
    const t = useStudioTranslation("setupWizard");
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        additionalSettingsStep,
        licenseKeyStep,
        securityStep: { securityOption },
    } = useWatch({ control });

    const { licenseInfo } = licenseKeyStep;

    useAdditionalSettingsFormSideEffects();

    return (
        <div className="setup-wizard-additional-settings">
            <div className="mb-4">
                <h2 className="mb-1">{t("additionalSettingsStep.title")}</h2>
                <p className="mb-4 text-muted">{t("additionalSettingsStep.description")}</p>
            </div>
            {getLicenseType(licenseInfo).isHigherThan("None") && securityOption !== "none" && (
                <div>
                    <ServerEnvironmentSection control={control} licenseInfo={licenseInfo} />
                    <CertificateExpirationSection control={control} />
                    <HrHeader />
                </div>
            )}
            <ExperimentalFeaturesSection control={control} licenseInfo={licenseInfo} />
            <div className="d-flex gap-2 align-items-center">
                <h3 className="mb-1">{t("additionalSettingsStep.advanced.title")}</h3>
                <FormSwitch size="lg" name="additionalSettingsStep.isAdvancedSettingsVisible" control={control} />
            </div>
            <p className="text-muted">{t("additionalSettingsStep.advanced.description")}</p>
            <AdvancedSettingsContent control={control} isVisible={additionalSettingsStep.isAdvancedSettingsVisible} />
        </div>
    );
}

function useAdditionalSettingsFormSideEffects() {
    const { reportEvent } = useEventsCollector();
    const { setValue, watch, control } = useFormContext<SetupWizardFormData>();

    const { licenseKeyStep } = useWatch({ control });

    useEffect(() => {
        if (licenseKeyStep.licenseInfo?.licenseStatus?.HasStudioConfiguration) {
            if (getLicenseType(licenseKeyStep.licenseInfo).isDeveloper()) {
                setValue("additionalSettingsStep.studioEnvironment", "Development", {
                    shouldDirty: true,
                });
            } else if (getLicenseType(licenseKeyStep.licenseInfo).isProfessionalOrHigher()) {
                setValue("additionalSettingsStep.studioEnvironment", "Production", {
                    shouldDirty: true,
                });
            }
        }

        const { unsubscribe } = watch((values, { name }) => {
            if (
                name === "additionalSettingsStep.isAdvancedSettingsVisible" &&
                !values.additionalSettingsStep.isAdvancedSettingsVisible
            ) {
                setValue("additionalSettingsStep.dataDirectory", null);
                setValue("additionalSettingsStep.setupCertificatePath", null);
            }

            switch (name) {
                case "additionalSettingsStep.isAdvancedSettingsVisible": {
                    const state = values.additionalSettingsStep.isAdvancedSettingsVisible ? "enabled" : "disabled";
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "toggle-advanced", state);
                    break;
                }
                case "additionalSettingsStep.studioEnvironment": {
                    const env = values.additionalSettingsStep.studioEnvironment;
                    if (env) {
                        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-environment", env);
                    }
                    break;
                }
                case "additionalSettingsStep.adminCertificateExpirationTime": {
                    const months = values.additionalSettingsStep.adminCertificateExpirationTime;
                    if (months != null) {
                        reportEvent(
                            setupWizardGA4Prefixes.additionalSettingsStep,
                            "set-admin-cert-expiration",
                            String(months)
                        );
                    }
                    break;
                }
                case "additionalSettingsStep.postgresqlIntegration": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-postgresql");
                    break;
                }
                case "additionalSettingsStep.dataDirectory": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-data-dir");
                    break;
                }
                case "additionalSettingsStep.setupCertificatePath": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-setup-cert-path");
                    break;
                }
                case "additionalSettingsStep.logsPath": {
                    reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "set-logs-path");
                    break;
                }
                case "additionalSettingsStep.autoIndexingEngineType": {
                    reportEvent(
                        setupWizardGA4Prefixes.additionalSettingsStep,
                        "set-auto-indexing-engine",
                        values.additionalSettingsStep.autoIndexingEngineType
                    );
                    break;
                }
                case "additionalSettingsStep.staticIndexingEngineType": {
                    reportEvent(
                        setupWizardGA4Prefixes.additionalSettingsStep,
                        "set-static-indexing-engine",
                        values.additionalSettingsStep.staticIndexingEngineType
                    );
                    break;
                }
            }
        });

        return () => unsubscribe();
    }, [watch, setValue]); // eslint-disable-line react-hooks/exhaustive-deps
}

const studioEnvironmentImage = require("Content/img/setupWizard/studioEnvironment.png");

function ServerEnvironmentSection({
    control,
    licenseInfo,
}: {
    control: Control<SetupWizardFormData>;
    licenseInfo: SetupWizardFormData["licenseKeyStep"]["licenseInfo"];
}) {
    const t = useStudioTranslation("setupWizard");

    if (licenseInfo?.licenseStatus?.HasStudioConfiguration) {
        return (
            <FormGroup>
                <FormLabel className="d-flex">
                    <div>{t("additionalSettingsStep.studioEnvironment.label")}</div>
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                <img
                                    src={studioEnvironmentImage}
                                    alt={t("additionalSettingsStep.studioEnvironment.imageAlt")}
                                    className="mb-2 w-100"
                                />
                                <span>{t("additionalSettingsStep.studioEnvironment.info")}</span>
                            </>
                        }
                        placement="right"
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name="additionalSettingsStep.studioEnvironment"
                    options={setupWizardConstants.allStudioEnvironmentOptions}
                />
            </FormGroup>
        );
    }

    return null;
}

function CertificateExpirationSection({ control }: { control: Control<SetupWizardFormData> }) {
    const t = useStudioTranslation("setupWizard");
    const {
        securityStep: { securityOption },
    } = useWatch({ control });

    if (securityOption === "none") {
        return null;
    }

    return (
        <FormGroup>
            <FormLabel className="d-flex">
                {t("additionalSettingsStep.certificateExpiration.label")}
                <PopoverWithHoverWrapper
                    message={<>{t("additionalSettingsStep.certificateExpiration.info")}</>}
                    placement="right"
                >
                    <Icon icon="info-new" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <FormInput
                name="additionalSettingsStep.adminCertificateExpirationTime"
                placeholder={t("additionalSettingsStep.certificateExpiration.placeholder")}
                control={control}
                type="number"
                addon={t("additionalSettingsStep.certificateExpiration.unit")}
            />
        </FormGroup>
    );
}

interface ExperimentalFeaturesSectionProps {
    control: Control<SetupWizardFormData>;
    licenseInfo: SetupWizardFormData["licenseKeyStep"]["licenseInfo"];
}

function ExperimentalFeaturesSection({ control, licenseInfo }: ExperimentalFeaturesSectionProps) {
    const t = useStudioTranslation("setupWizard");

    if (!licenseInfo?.licenseStatus?.HasPostgreSqlIntegration) {
        return null;
    }

    return (
        <div>
            <h4 className="mb-0">{t("additionalSettingsStep.experimentalFeatures.title")}</h4>
            <p className="text-muted">{t("additionalSettingsStep.experimentalFeatures.description")}</p>
            <PostgreSqlIntegrationToggle control={control} />
        </div>
    );
}

function PostgreSqlIntegrationToggle({ control }: { control: Control<SetupWizardFormData> }) {
    const t = useStudioTranslation("setupWizard");

    return (
        <div className="postgresql-integration bg-faded-experimental">
            <FormSwitch name="additionalSettingsStep.postgresqlIntegration" color="experimental" control={control} />
            <div className="d-flex w-100 align-items-center justify-content-between">
                <div className="flex-grow-1 lh-1">
                    <strong className="postgresql-integration__title">
                        {t("additionalSettingsStep.experimentalFeatures.postgreSqlTitle")}
                        <PopoverWithHoverWrapper
                            message={
                                <SetupWizardInfoPopover
                                    description={t("additionalSettingsStep.experimentalFeatures.postgreSqlInfo")}
                                    ravenLinkHash="TRKC2W"
                                />
                            }
                            placement="right"
                        >
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </strong>
                    <small className="postgresql-integration__description">
                        {t("additionalSettingsStep.experimentalFeatures.postgreSqlDescription")}
                    </small>
                </div>
                <Icon className="postgresql-integration__icon" size="lg" icon="integrations" />
            </div>
        </div>
    );
}

interface AdvancedSettingsContentProps {
    control: Control<SetupWizardFormData>;
    isVisible: boolean;
}

const getDefaultPath = (
    t: StudioTranslate<"setupWizard">,
    os: OperatingSystem,
    type: "certificate" | "dataDirectory" | "logs"
) => {
    const isUnix = os === "Linux" || os === "MacOS";

    const paths = {
        certificate: isUnix ? "/etc/ravendb/security/server.pfx" : "C:\\RavenDB\\Certificate\\server.pfx",
        dataDirectory: isUnix ? "/var/lib/ravendb/data" : "C:\\RavenDB\\Data",
        logs: isUnix ? "/logs" : "C:\\RavenDB\\Logs",
    };

    return t("additionalSettingsStep.advanced.pathPlaceholder", { path: paths[type] });
};

const isAbsolutePath = (path: string | null | undefined): boolean => {
    if (!path) {
        return false;
    }

    // Windows absolute path: starts with drive letter (e.g., C:\) or UNC path (\\)
    if (/^[A-Za-z]:\\/.test(path) || /^\\\\/.test(path)) {
        return true;
    }

    // Unix/Mac absolute path: starts with /
    return path.startsWith("/");
};

function AdvancedSettingsContent({ control, isVisible }: AdvancedSettingsContentProps) {
    const t = useStudioTranslation("setupWizard");
    const os = useOS();
    const { resourcesService } = useServices();

    const {
        securityStep: { securityOption },
        additionalSettingsStep: { dataDirectory, logsPath, setupCertificatePath },
        selfSignedCertificateStep,
        domainStep,
    } = useWatch({
        control,
    });

    const getLocalFolderPathsProvider = (path: string) => {
        return async () => {
            const dto = await resourcesService.getFolderPathOptions_ServerLocal(path, true);
            return dto?.List || [];
        };
    };

    const defaultCertificatePathFileName = useMemo(() => {
        switch (securityOption) {
            case "ownCertificate":
                return sanitizeCommonName(`cluster.server.certificate.${selfSignedCertificateStep.cns[0]}.pfx`); // For own certificates, we use the first Common Name (CN) as the main identifier
            case "letsEncrypt":
                return `cluster.server.certificate.${getFullDomain(domainStep)}.pfx`;
            default:
                return undefined;
        }
    }, []);

    return (
        <div
            className={classNames({
                "item-disabled": !isVisible,
            })}
        >
            <FormGroup>
                <FormLabel className="hstack">
                    <div>{t("additionalSettingsStep.advanced.dataDirectory")}</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={
                                        <ul>
                                            <li>{t("additionalSettingsStep.advanced.dataDirectoryInfo")}</li>
                                            <li>
                                                <StudioTrans
                                                    ns="setupWizard"
                                                    i18nKey="additionalSettingsStep.advanced.dataDirectoryDefault"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                        </ul>
                                    }
                                    ravenLinkHash="GUNB5P"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormPathSelector
                    disabled={!isVisible}
                    name="additionalSettingsStep.dataDirectory"
                    control={control}
                    placeholder={getDefaultPath(t, os, "dataDirectory")}
                    getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                    getPathDependencies={(path: string) => [path]}
                />
                {isVisible && isAbsolutePath(dataDirectory) && (
                    <RichAlert variant="warning" className="mt-2">
                        {t("additionalSettingsStep.advanced.dataDirectoryWarning")}
                    </RichAlert>
                )}
            </FormGroup>
            {securityOption !== "none" && (
                <FormGroup>
                    <FormLabel className="vstack">
                        <div className="hstack">
                            <div>{t("additionalSettingsStep.advanced.setupCertificatePath")}</div>
                            <ConditionalPopover
                                conditions={{
                                    isActive: isVisible,
                                    message: (
                                        <SetupWizardInfoPopover
                                            description={
                                                <ul>
                                                    <li>
                                                        <StudioTrans
                                                            ns="setupWizard"
                                                            i18nKey="additionalSettingsStep.advanced.setupCertificatePathInfo"
                                                            components={{ strong: <strong />, code: <code /> }}
                                                        />
                                                    </li>
                                                    <li>
                                                        <StudioTrans
                                                            ns="setupWizard"
                                                            i18nKey="additionalSettingsStep.advanced.setupCertificatePathPermissions"
                                                            components={{ strong: <strong /> }}
                                                        />
                                                    </li>
                                                    <li>
                                                        <StudioTrans
                                                            ns="setupWizard"
                                                            i18nKey="additionalSettingsStep.advanced.setupCertificatePathDefault"
                                                            components={{ code: <code /> }}
                                                        />
                                                    </li>
                                                </ul>
                                            }
                                            ravenLinkHash="OXZ53O"
                                        />
                                    ),
                                }}
                                popoverPlacement="right"
                            >
                                <Icon icon="info-new" />
                            </ConditionalPopover>
                        </div>
                    </FormLabel>
                    <FormPathSelector
                        disabled={!isVisible}
                        name="additionalSettingsStep.setupCertificatePath"
                        control={control}
                        defaultFileName={defaultCertificatePathFileName}
                        placeholder={getDefaultPath(t, os, "certificate")}
                        getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                        getPathDependencies={(path: string) => [path]}
                    />
                    {isVisible && !!setupCertificatePath && (
                        <RichAlert variant="warning" className="mt-2">
                            {t("additionalSettingsStep.advanced.setupCertificatePathWarning")}
                        </RichAlert>
                    )}
                </FormGroup>
            )}
            <FormGroup>
                <FormLabel className="hstack">
                    <div>{t("additionalSettingsStep.advanced.logsDirectory")}</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={
                                        <ul>
                                            <li>{t("additionalSettingsStep.advanced.logsDirectoryInfo")}</li>
                                            <li>
                                                <StudioTrans
                                                    ns="setupWizard"
                                                    i18nKey="additionalSettingsStep.advanced.logsDirectoryDefault"
                                                    components={{ code: <code /> }}
                                                />
                                            </li>
                                        </ul>
                                    }
                                    ravenLinkHash="BHUAJU"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormPathSelector
                    disabled={!isVisible}
                    name="additionalSettingsStep.logsPath"
                    control={control}
                    placeholder={getDefaultPath(t, os, "logs")}
                    getPathsProvider={(path: string) => getLocalFolderPathsProvider(path)}
                    getPathDependencies={(path: string) => [path]}
                />
                {isVisible && isAbsolutePath(logsPath) && (
                    <RichAlert variant="warning" className="mt-2">
                        {t("additionalSettingsStep.advanced.logsDirectoryWarning")}
                    </RichAlert>
                )}
            </FormGroup>
            <FormGroup>
                <FormLabel className="hstack">
                    <div>{t("additionalSettingsStep.advanced.autoIndexingEngine")}</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={t("additionalSettingsStep.advanced.autoIndexingEngineInfo")}
                                    ravenLinkHash="H8IONI"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormSelect
                    className={classNames({ "z-n1": !isVisible })}
                    name="additionalSettingsStep.autoIndexingEngineType"
                    control={control}
                    options={setupWizardConstants.indexingEngineTypeOptions}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel className="hstack">
                    <div>{t("additionalSettingsStep.advanced.staticIndexingEngine")}</div>
                    <ConditionalPopover
                        conditions={{
                            isActive: isVisible,
                            message: (
                                <SetupWizardInfoPopover
                                    description={t("additionalSettingsStep.advanced.staticIndexingEngineInfo")}
                                    ravenLinkHash="H8IONI"
                                />
                            ),
                        }}
                        popoverPlacement="right"
                    >
                        <Icon icon="info-new" />
                    </ConditionalPopover>
                </FormLabel>
                <FormSelect
                    className={classNames({ "z-n1": !isVisible })}
                    name="additionalSettingsStep.staticIndexingEngineType"
                    control={control}
                    options={setupWizardConstants.indexingEngineTypeOptions}
                />
            </FormGroup>
        </div>
    );
}

export function SetupWizardAdditionalSettingsStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { setValue } = useFormContext<SetupWizardFormData>();
    const { reportEvent } = useEventsCollector();

    const handleContinue = () => {
        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "continue");
        setValue("currentStep", "Summary");
    };

    const handleBack = () => {
        reportEvent(setupWizardGA4Prefixes.additionalSettingsStep, "back");
        setValue("additionalSettingsStep", setupWizardFormDefaultValues["additionalSettingsStep"]);
        setValue("currentStep", "Node addresses");
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> {t("actions.back")}
            </Button>
            <Button variant="primary" className="rounded-pill" onClick={handleContinue}>
                {t("actions.continue")} <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}
