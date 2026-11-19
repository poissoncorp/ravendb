import { useFormContext, useWatch } from "react-hook-form";
import { SetupWizardFormData } from "../setupWizardValidation";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import React from "react";
import Card from "react-bootstrap/Card";
import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { getFullDomain } from "components/setupWizard/utils/setupWizardUtils";
import { setupWizardConstants } from "components/setupWizard/utils/setupWizardConstants";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import LicenseType = Raven.Server.Commercial.LicenseType;
import License = Raven.Server.Commercial.License;

export function SetupWizardSummaryStep() {
    const t = useStudioTranslation("setupWizard");
    const { control } = useFormContext<SetupWizardFormData>();

    const {
        nodeAddressStep,
        additionalSettingsStep,
        domainStep,
        securityStep: { securityOption },
        licenseKeyStep,
    } = useWatch({ control });

    const license: License = licenseKeyStep.key ? JSON.parse(licenseKeyStep.key) : ({} as License);

    const hasAdditionalSettings =
        !!additionalSettingsStep?.logsPath ||
        !!additionalSettingsStep?.dataDirectory ||
        !!additionalSettingsStep?.autoIndexingEngineType ||
        !!additionalSettingsStep?.staticIndexingEngineType ||
        !!additionalSettingsStep?.setupCertificatePath ||
        !!(
            licenseKeyStep.licenseInfo?.licenseStatus?.HasStudioConfiguration &&
            additionalSettingsStep?.studioEnvironment
        ) ||
        !!(securityOption !== "none" && additionalSettingsStep?.adminCertificateExpirationTime) ||
        !!additionalSettingsStep?.postgresqlIntegration;

    return (
        <div>
            <div className="mb-5">
                <h2 className="mb-1">{t("summaryStep.title")}</h2>
                <p className="mb-4 text-muted">{t("summaryStep.description")}</p>
            </div>
            <div className="vstack gap-1 mb-2">
                <h5 className="mb-0">{t("summaryStep.securityAndLicense.title")}</h5>
                <Card>
                    <Card.Body>
                        <div className="vstack gap-1">
                            <CardRow
                                label={t("summaryStep.securityAndLicense.certificate")}
                                valueClassName={securityOption === "none" ? "text-warning" : null}
                                value={t("summaryStep.securityAndLicense.certificateValue", {
                                    context: securityOption,
                                })}
                            />
                            {securityOption !== "none" && (
                                <CardRow label={t("summaryStep.securityAndLicense.licenseId")} value={license?.Id} />
                            )}
                            <CardRow
                                label={t("summaryStep.securityAndLicense.licenseType")}
                                value={licenseKeyStep.licenseInfo?.licenseStatus?.Type ?? "AGPLv3"}
                                valueClassName={colorizeLicenseType(licenseKeyStep.licenseInfo?.licenseStatus?.Type)}
                            />
                            {securityOption === "letsEncrypt" && (
                                <CardRow
                                    label={t("summaryStep.securityAndLicense.fullDomain")}
                                    value={getFullDomain(domainStep)}
                                />
                            )}
                        </div>
                    </Card.Body>
                </Card>
                <h5 className="mb-0 mt-4">{t("summaryStep.cluster.title")}</h5>
                <Card>
                    <Card.Body>
                        <LocationDistribution>
                            <DistributionLegend>
                                <div className="top"></div>
                                <div className="node">
                                    {t("summaryStep.cluster.httpPort", {
                                        context: securityOption === "none" ? "http" : "https",
                                    })}
                                </div>
                                <div>{t("summaryStep.cluster.tcpPort")}</div>
                                <div>{t("summaryStep.cluster.ipAddress")}</div>
                            </DistributionLegend>
                            {nodeAddressStep.nodes.map((node, index) => (
                                <NodeDistributionItem key={index} {...node} />
                            ))}
                        </LocationDistribution>
                    </Card.Body>
                </Card>
                {hasAdditionalSettings && (
                    <>
                        <h5 className="mb-0 mt-4">{t("summaryStep.additional.title")}</h5>
                        <Card className="mb-4">
                            <Card.Body>
                                <div className="vstack gap-1">
                                    {licenseKeyStep.licenseInfo?.licenseStatus?.HasStudioConfiguration && (
                                        <CardRow
                                            label={t("summaryStep.additional.studioEnvironment")}
                                            value={
                                                additionalSettingsStep.studioEnvironment &&
                                                setupWizardConstants.getStudioEnvironmentLabel(
                                                    additionalSettingsStep.studioEnvironment
                                                )
                                            }
                                        />
                                    )}
                                    {securityOption !== "none" && (
                                        <CardRow
                                            label={t("summaryStep.additional.certificateExpiration")}
                                            value={t("summaryStep.additional.certificateExpirationValue", {
                                                count: additionalSettingsStep.adminCertificateExpirationTime,
                                            })}
                                        />
                                    )}
                                    {licenseKeyStep.licenseInfo?.licenseStatus?.HasPostgreSqlIntegration && (
                                        <CardRow
                                            label={t("summaryStep.additional.experimentalFeatures")}
                                            value={t("summaryStep.additional.experimentalFeaturesValue", {
                                                context: additionalSettingsStep.postgresqlIntegration
                                                    ? "enabled"
                                                    : "disabled",
                                            })}
                                            valueClassName={
                                                additionalSettingsStep.postgresqlIntegration ? "text-success" : null
                                            }
                                        />
                                    )}
                                    {additionalSettingsStep.dataDirectory && (
                                        <CardRow
                                            label={t("summaryStep.additional.dataDirectory")}
                                            value={additionalSettingsStep.dataDirectory}
                                        />
                                    )}
                                    {additionalSettingsStep.setupCertificatePath && (
                                        <CardRow
                                            label={t("summaryStep.additional.setupCertificatePath")}
                                            value={additionalSettingsStep.setupCertificatePath}
                                        />
                                    )}
                                    {additionalSettingsStep.logsPath && (
                                        <CardRow
                                            label={t("summaryStep.additional.logsPath")}
                                            value={additionalSettingsStep.logsPath}
                                        />
                                    )}
                                    {additionalSettingsStep.autoIndexingEngineType && (
                                        <CardRow
                                            label={t("summaryStep.additional.autoIndexingEngine")}
                                            value={additionalSettingsStep.autoIndexingEngineType}
                                        />
                                    )}
                                    {additionalSettingsStep.staticIndexingEngineType && (
                                        <CardRow
                                            label={t("summaryStep.additional.staticIndexingEngine")}
                                            value={additionalSettingsStep.staticIndexingEngineType}
                                        />
                                    )}
                                </div>
                            </Card.Body>
                        </Card>
                    </>
                )}
            </div>
        </div>
    );
}

function colorizeLicenseType(licenseType: LicenseType) {
    switch (licenseType) {
        case "Developer":
            return "text-developer";
        case "Enterprise":
            return "text-enterprise";
        case "Professional":
            return "text-professional";
        case "Community":
        case "Essential":
            return "text-info";
        default:
            return "";
    }
}

interface CardRowProps {
    label: string;
    value: string | number;
    valueClassName?: string;
}

function CardRow({ label, value, valueClassName }: CardRowProps) {
    return (
        <div className="d-flex justify-content-between">
            <span>{label}</span>
            <span className={valueClassName}>{value}</span>
        </div>
    );
}

function NodeDistributionItem({
    nodeTag,
    httpPort,
    tcpPort,
    ipAddress,
    nodeUrl,
    externalHttpPort,
    externalTcpPort,
    externalIpAddress,
}: SetupWizardFormData["nodeAddressStep"]["nodes"][number]) {
    const t = useStudioTranslation("setupWizard");

    return (
        <DistributionItem>
            <div className="top node">
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            <StudioTrans
                                ns="setupWizard"
                                i18nKey="summaryStep.cluster.nodeUrl"
                                components={{ a: <a href="#" /> }}
                                options={{ nodeTag, nodeUrl }}
                            />
                        </div>
                    }
                >
                    <Icon icon="node" /> {nodeTag}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            <StudioTrans
                                ns="setupWizard"
                                i18nKey="summaryStep.cluster.httpPortInfo"
                                components={{ strong: <strong /> }}
                                options={{
                                    port: httpPort,
                                    externalPort: externalHttpPort,
                                    context: externalHttpPort ? "external" : undefined,
                                }}
                            />
                        </div>
                    }
                >
                    {httpPort} {externalHttpPort && <span> → {externalHttpPort}</span>}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            <StudioTrans
                                ns="setupWizard"
                                i18nKey="summaryStep.cluster.tcpPortInfo"
                                components={{ b: <b /> }}
                                options={{
                                    port: tcpPort,
                                    externalPort: externalTcpPort,
                                    context: externalTcpPort ? "external" : undefined,
                                }}
                            />
                        </div>
                    }
                >
                    {tcpPort} {externalTcpPort && <span> → {externalTcpPort}</span>}
                </PopoverWithHoverWrapper>
            </div>
            <div>
                <PopoverWithHoverWrapper
                    message={
                        <div>
                            {t("summaryStep.cluster.ipAddressInfo", { count: ipAddress.length })}
                            <ul className="mb-0">
                                {ipAddress.map((ip) => (
                                    <li key={ip.ipAddress}>
                                        <strong>{ip.ipAddress}</strong>
                                    </li>
                                ))}
                            </ul>
                            {externalIpAddress && (
                                <StudioTrans
                                    ns="setupWizard"
                                    i18nKey="summaryStep.cluster.externalIpAddressInfo"
                                    components={{ strong: <strong /> }}
                                    options={{ ipAddress: externalIpAddress }}
                                />
                            )}
                        </div>
                    }
                >
                    <FormatIpAddresses
                        addresses={ipAddress.map((x) => x.ipAddress)}
                        externalIpAddress={externalIpAddress}
                    />
                </PopoverWithHoverWrapper>
            </div>
        </DistributionItem>
    );
}

interface FormatIpAddressesProps {
    addresses: string[];
    externalIpAddress?: string;
}

function FormatIpAddresses({ addresses, externalIpAddress }: FormatIpAddressesProps) {
    const t = useStudioTranslation("setupWizard");

    if (addresses.length <= 1) {
        if (externalIpAddress) {
            return (
                <>
                    {addresses[0]} → {externalIpAddress}
                </>
            );
        }

        return <>{addresses[0]}</>;
    }

    const firstAddress = addresses[0];
    const remainingCount = addresses.length - 1;

    if (externalIpAddress) {
        return (
            <>
                {t("summaryStep.cluster.moreAddressesWithExternal", {
                    count: remainingCount,
                    address: firstAddress,
                    externalIpAddress,
                })}
            </>
        );
    }

    return <>{t("summaryStep.cluster.moreAddresses", { count: remainingCount, address: firstAddress })}</>;
}

export function SetupWizardSummaryStepFooter() {
    const t = useStudioTranslation("setupWizard");
    const { setValue } = useFormContext<SetupWizardFormData>();

    const handleContinue = () => {
        setValue("currentStep", "Finish");
    };

    const handleBack = () => {
        setValue("currentStep", "Additional settings");
    };

    return (
        <div className="hstack justify-content-between">
            <Button variant="secondary" className="rounded-pill" onClick={handleBack}>
                <Icon icon="arrow-left" /> {t("actions.back")}
            </Button>
            <Button variant="primary" className="rounded-pill" onClick={handleContinue}>
                {t("actions.finish")} <Icon icon="arrow-right" margin="m-0" />
            </Button>
        </div>
    );
}
