import Table from "react-bootstrap/Table";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { Icon } from "components/common/Icon";
import React, { ChangeEvent, useMemo, useState } from "react";
import { RadioToggleWithIcon, RadioToggleWithIconInputItem } from "components/common/toggles/RadioToggle";
import classNames from "classnames";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "hooks/useRavenLink";
import Button from "react-bootstrap/Button";
import Tooltip from "react-bootstrap/Tooltip";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import { aiConnectionStringUtils } from "components/pages/database/settings/connectionStrings/editForms/aiConnectionStringUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { createTranslator } from "common/i18n/i18n";
import type { StudioTranslate } from "common/i18n/resources";

export function LicenseDetails() {
    const t = useStudioTranslation("aboutPage");
    const licenseId = useAppSelector(licenseSelectors.statusValue("Id"));
    const licenseTo = useAppSelector(licenseSelectors.statusValue("LicensedTo"));
    const licenseType = useAppSelector(licenseSelectors.licenseType);

    return (
        <React.Fragment key="license-details">
            <div className="bg-faded-primary mb-4">
                {licenseType !== "None" ? (
                    <Row className="text-center py-4">
                        <Col className="px-4">
                            <div className="small text-muted">{t("licenseDetails.licenseId")}</div>
                            <h4 className="fw-bolder text-emphasis m-0">{licenseId}</h4>
                        </Col>
                        <Col className="px-4">
                            <div className="small text-muted">{t("licenseDetails.licenseTo")}</div>
                            <h4 className="fw-bolder text-emphasis m-0">{licenseTo}</h4>
                        </Col>
                    </Row>
                ) : (
                    <div className="text-center py-4">
                        <h3 className="fw-bolder text-warning d-flex align-items-center justify-content-center">
                            <Icon icon="empty-set" className="fs-1" margin="me-3" /> {t("licenseDetails.noLicense")}
                        </h3>
                        <Button
                            variant="success"
                            className="px-4 rounded-pill"
                            size="lg"
                            href={aboutPageUrls.getLicense}
                            target="_blank"
                        >
                            <strong>{t("licenseDetails.getFreeLicense")}</strong>
                            <Icon icon="newtab" margin="ms-2" />
                        </Button>
                    </div>
                )}
            </div>

            <LicenseTable licenseType={licenseType} />
        </React.Fragment>
    );
}

interface LicenseTableProps {
    licenseType: Raven.Server.Commercial.LicenseType;
}

function LicenseTable(props: LicenseTableProps) {
    const t = useStudioTranslation("aboutPage");
    const { licenseType } = props;

    const [searchText, setSearchText] = useState("");
    const [viewMode, setViewMode] = useState<"showDiff" | "showAll">("showAll");

    const licenseStatus = useAppSelector(licenseSelectors.status);
    const { columns, current: currentColumn } = getColumnsData(licenseType, licenseStatus.IsCloud);

    const developerLicenseLink = useRavenLink({ hash: "ZOVGRA", isDocs: false });

    const featureAvailabilityData = useMemo(() => getFeatureAvailabilityData(t), [t]);

    const getEffectiveValue = (feature: FeatureAvailabilityItem, column: LicenseColumn) => {
        if (column !== currentColumn || !feature.fieldInLicense) {
            return feature[column].value;
        }

        const licenseValue = licenseStatus[feature.fieldInLicense];

        if (licenseValue === null) {
            return Infinity;
        }

        return licenseValue;
    };

    const filteredSections = filterFeatureAvailabilitySection(
        featureAvailabilityData,
        searchText,
        viewMode,
        columns,
        getEffectiveValue
    );

    const onSearchTextChange = (searchText: string) => {
        setSearchText(searchText);
    };

    const leftRadioToggleItem: RadioToggleWithIconInputItem<"showDiff"> = {
        label: t("licenseDetails.showDifferences"),
        value: "showDiff",
        iconName: "diff",
    };

    const rightRadioToggleItem: RadioToggleWithIconInputItem<"showAll"> = {
        label: t("licenseDetails.showAll"),
        value: "showAll",
        iconName: "license",
    };

    const showUpgradeButton = licenseStatus.IsCloud ? licenseType !== "Enterprise" : licenseType !== "EnterpriseAi";

    const getLicenseTypeName = (column: LicenseColumn) => {
        if (column === "community" && licenseType === "Essential") {
            return "Essential";
        }

        if (column === "community" && licenseStatus.IsCloud) {
            return "Free";
        }

        if (column === "enterprise" && licenseStatus.IsCloud) {
            return "Production";
        }

        if (column === "enterpriseAi") {
            return "RavenDB AI";
        }

        return column.toLocaleUpperCase();
    };

    return (
        <>
            <div className="px-4 pb-4">
                <div className="clearable-input">
                    <Form.Control
                        type="text"
                        accessKey="/"
                        placeholder={t("licenseDetails.filterPlaceholder")}
                        title={t("licenseDetails.filterTitle")}
                        className="filtering-input"
                        value={searchText}
                        onChange={(e: ChangeEvent<HTMLInputElement>) => onSearchTextChange(e.target.value)}
                    />
                    {searchText && (
                        <div className="clear-button">
                            <Button variant="secondary" size="sm" onClick={() => onSearchTextChange("")}>
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div className="table-responsive license-feature-availability-table">
                <Table borderless hover striped className="text-center feature-availability-table">
                    <thead>
                        <tr>
                            <th>{t("licenseDetails.licenseType")}</th>
                            {columns.map((column) => (
                                <th key={column} className={classNames({ "bg-current": column === currentColumn })}>
                                    <h4 className="fw-bolder text-uppercase m-0">{getLicenseTypeName(column)}</h4>
                                    {column === currentColumn && (
                                        <div className="text-primary">{t("licenseDetails.current")}</div>
                                    )}
                                </th>
                            ))}
                        </tr>
                        {showUpgradeButton && <UpgradeRow columns={columns} current={currentColumn} />}
                    </thead>
                    <tbody>
                        {filteredSections.length === 0 && (
                            <tr>
                                <td>{t("licenseDetails.noMatches")}</td>
                            </tr>
                        )}
                        {filteredSections.map((section) => (
                            <React.Fragment key={section.name ?? "n/a"}>
                                {section.name && (
                                    <tr key={section.name}>
                                        <React.Fragment key={section.name}>
                                            <th scope="row">
                                                {section.link ? (
                                                    <a
                                                        href={section.link}
                                                        target="_blank"
                                                        className={classNames("fw-bold", section.className)}
                                                    >
                                                        {section.name} <Icon icon="newtab" margin="m-0" />
                                                    </a>
                                                ) : (
                                                    <div
                                                        className={classNames("fw-bold text-light", section.className)}
                                                    >
                                                        {section.name}
                                                    </div>
                                                )}
                                            </th>
                                            {columns.map((column) => (
                                                <td
                                                    key={column}
                                                    className={classNames({
                                                        "bg-current": column === currentColumn,
                                                    })}
                                                ></td>
                                            ))}
                                        </React.Fragment>
                                    </tr>
                                )}

                                {section.items.map((feature) => (
                                    <tr key={feature.name}>
                                        <th scope="row">
                                            <span>
                                                {feature.name}
                                                {feature.helperInfo && (
                                                    <OverlayTrigger
                                                        placement="top"
                                                        overlay={
                                                            <Tooltip id={feature.name}>{feature.helperInfo}</Tooltip>
                                                        }
                                                    >
                                                        <div className="d-inline-block">
                                                            <Icon icon="info" color="info" className="ms-1" />
                                                        </div>
                                                    </OverlayTrigger>
                                                )}
                                            </span>
                                        </th>
                                        {columns.map((column) => (
                                            <td
                                                key={column}
                                                className={classNames({
                                                    "bg-current": column === currentColumn,
                                                })}
                                            >
                                                <FeatureValue
                                                    value={getEffectiveValue(feature, column)}
                                                    unit={feature.unit}
                                                />
                                            </td>
                                        ))}
                                    </tr>
                                ))}
                            </React.Fragment>
                        ))}
                    </tbody>
                </Table>
            </div>
            {columns.length > 1 && (
                <div className="p-2">
                    <div className="well rounded-pill text-center p-1">
                        <RadioToggleWithIcon<"showDiff" | "showAll">
                            name="some-name"
                            leftItem={leftRadioToggleItem}
                            rightItem={rightRadioToggleItem}
                            selectedValue={viewMode}
                            setSelectedValue={(x) => setViewMode(x)}
                        />
                    </div>
                </div>
            )}
            {licenseType !== "Developer" && licenseType !== "Quill" && (
                <small className="mt-2 pb-2 text-center text-muted">
                    <Icon icon="info" />
                    <StudioTrans
                        ns="aboutPage"
                        i18nKey="licenseDetails.developerLicenseInfo"
                        components={{
                            a: <a href={developerLicenseLink} target="_blank" className="text-developer" />,
                            icon: <Icon icon="newtab" margin="m-0" />,
                        }}
                    />
                </small>
            )}
        </>
    );
}

const availableEverywhere = {
    agpl: { value: true },
    community: { value: true },
    professional: { value: true },
    enterprise: { value: true },
    developer: { value: true },
    enterpriseAi: { value: true },
    quill: { value: true },
};

function getFeatureAvailabilityData(t: StudioTranslate<"aboutPage">): FeatureAvailabilitySection[] {
    return [
        {
            name: null,
            link: null,
            items: [
                {
                    name: t("licenseDetails.features.general.eligibleForCommercialUse"),
                    agpl: { value: true },
                    community: { value: true },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: false },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.general.numberOfDatabases"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.general.singleDatabaseSize"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.general.managementStudio"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
            ],
        },
        {
            name: "AI",
            className: "ai-gradient",
            link: null,
            items: [
                {
                    name: "GenAI",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: false },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasGenAi",
                },
                {
                    name: t("licenseDetails.features.ai.aiAgent"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: false },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasAiAgent",
                },
                {
                    name: t("licenseDetails.features.ai.aiAssistant"),
                    agpl: { value: false },
                    community: { value: true },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasAiAssistant",
                },
            ],
        },
        {
            name: t("licenseDetails.features.clustering.title"),
            link: "https://ravendb.net/features#clusters",
            items: [
                {
                    name: t("licenseDetails.features.clustering.maxClusterSize"),
                    agpl: { value: "1" },
                    community: { value: "3" },
                    professional: { value: "5" },
                    enterprise: { value: Infinity },
                    developer: { value: "3" },
                    enterpriseAi: { value: Infinity },
                    quill: { value: "1" },
                    fieldInLicense: "MaxClusterSize",
                    unit: "nodes",
                },
                {
                    name: t("licenseDetails.features.clustering.maxCores"),
                    agpl: { value: 3 },
                    community: { value: 3 },
                    professional: { value: 40 },
                    enterprise: { value: Infinity },
                    developer: { value: 9 },
                    enterpriseAi: { value: Infinity },
                    quill: { value: 3 },
                    fieldInLicense: "MaxCores",
                },
                {
                    name: t("licenseDetails.features.clustering.maxMemory"),
                    agpl: { value: "6" },
                    community: { value: "6" },
                    professional: { value: "240" },
                    enterprise: { value: Infinity },
                    developer: { value: "36" },
                    enterpriseAi: { value: Infinity },
                    quill: { value: "6" },
                    fieldInLicense: "MaxMemory",
                    unit: "memory",
                },
                {
                    name: t("licenseDetails.features.clustering.clusterDashboard"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.clustering.clusterTransactions"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.clustering.highlyAvailableTasks"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: false },
                    fieldInLicense: "HasHighlyAvailableTasks",
                },
                {
                    name: t("licenseDetails.features.clustering.dynamicDatabaseDistribution"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: false },
                    fieldInLicense: "HasDynamicNodesDistribution",
                },
            ],
        },
        {
            name: t("licenseDetails.features.indexes.title"),
            link: "https://ravendb.net/features#indexes",
            items: [
                {
                    name: t("licenseDetails.features.indexes.staticAndAuto"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.staticPerDatabase"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfStaticIndexesPerDatabase",
                },
                {
                    name: t("licenseDetails.features.indexes.staticPerCluster"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfStaticIndexesPerCluster",
                },
                {
                    name: t("licenseDetails.features.indexes.autoPerDatabase"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfAutoIndexesPerDatabase",
                },
                {
                    name: t("licenseDetails.features.indexes.autoPerCluster"),
                    agpl: { value: Infinity },
                    community: { value: Infinity },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfAutoIndexesPerCluster",
                },
                {
                    name: "Corax",
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.javaScriptIndexes"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.countersIndexing"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.compareExchangeIndexing"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.additionalAssemblies"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasAdditionalAssembliesFromNuGet",
                },
                {
                    name: t("licenseDetails.features.indexes.attachmentsSupport"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.rollingDeployment"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.indexes.indexCleanup"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasIndexCleanup",
                },
            ],
        },
        {
            name: t("licenseDetails.features.sharding.title"),
            link: "https://ravendb.net/features/clusters/sharding",
            items: [
                {
                    name: t("licenseDetails.features.sharding.singleNode"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.sharding.multiNode"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasMultiNodeSharding",
                },
                {
                    name: t("licenseDetails.features.sharding.maxReplicationFactor"),
                    agpl: { value: 1 },
                    community: { value: 1 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: 1 },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxReplicationFactorForSharding",
                },
            ],
        },
        {
            name: t("licenseDetails.features.timeSeries.title"),
            link: "https://ravendb.net/features#timeseries",
            items: [
                {
                    name: t("licenseDetails.features.timeSeries.timeSeries"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.timeSeries.incremental"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.timeSeries.rollupsAndRetention"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasTimeSeriesRollupsAndRetention",
                },
            ],
        },
        {
            name: t("licenseDetails.features.monitoring.title"),
            link: "https://ravendb.net/features#monitoring",
            items: [
                {
                    name: t("licenseDetails.features.monitoring.clusterDashboard"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: "SNMP",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasSnmpMonitoring",
                },
                {
                    name: t("licenseDetails.features.monitoring.monitoringEndpoints"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasMonitoringEndpoints",
                },
                {
                    name: "OpenTelemetry",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasMonitoringEndpoints",
                },
            ],
        },
        {
            name: t("licenseDetails.features.extensions.title"),
            link: "https://ravendb.net/features#extensions",
            items: [
                {
                    name: t("licenseDetails.features.extensions.counters"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.extensions.countersBulkInsert"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.extensions.documentRevisions"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.extensions.defaultRevisionsConfiguration"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "CanSetupDefaultRevisionsConfiguration",
                },
                {
                    name: t("licenseDetails.features.extensions.maxRevisionsToKeep"),
                    agpl: { value: 2 },
                    community: { value: 2 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfRevisionsToKeep",
                },
                {
                    name: t("licenseDetails.features.extensions.maxRevisionsRetention"),
                    agpl: { value: 45 },
                    community: { value: 45 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfRevisionAgeToKeepInDays",
                },
                {
                    name: t("licenseDetails.features.extensions.expirationAndRefresh"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.extensions.minRefreshFrequency"),
                    agpl: { value: 36 },
                    community: { value: 36 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MinPeriodForRefreshInHours",
                },
                {
                    name: t("licenseDetails.features.extensions.minExpirationFrequency"),
                    agpl: { value: 36 },
                    community: { value: 36 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MinPeriodForExpirationInHours",
                },
                {
                    name: t("licenseDetails.features.extensions.attachments"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.extensions.documentsCompression"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasDocumentsCompression",
                },
                {
                    name: t("licenseDetails.features.extensions.tcpCompression"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasTcpDataCompression",
                },
            ],
        },
        {
            name: t("licenseDetails.features.externalReplication.title"),
            link: "https://ravendb.net/features/replication/external-replication",
            items: [
                {
                    name: t("licenseDetails.features.externalReplication.immediate"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasExternalReplication",
                },
                {
                    name: t("licenseDetails.features.externalReplication.delayed"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasDelayedExternalReplication",
                },
                {
                    name: t("licenseDetails.features.externalReplication.filtered"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPullReplicationAsHub",
                },
                {
                    name: t("licenseDetails.features.externalReplication.replicationSink"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPullReplicationAsSink",
                },
                {
                    name: t("licenseDetails.features.externalReplication.replicationHub"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPullReplicationAsHub",
                },
            ],
        },
        {
            name: t("licenseDetails.features.backups.title"),
            link: "https://ravendb.net/features#backups",
            items: [
                {
                    name: t("licenseDetails.features.backups.local"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.backups.cloudAndRemote"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasCloudBackups",
                },
                {
                    name: t("licenseDetails.features.backups.encrypted"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasEncryptedBackups",
                },
                {
                    name: t("licenseDetails.features.backups.snapshot"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasSnapshotBackups",
                },
                {
                    name: t("licenseDetails.features.backups.periodic"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPeriodicBackup",
                },
            ],
        },
        {
            name: t("licenseDetails.features.embeddingsGeneration.title"),
            link: null,
            items: [
                {
                    name: t("licenseDetails.features.embeddingsGeneration.embeddedModel"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                    helperInfo: "bge-micro-v2",
                },
                {
                    name: t("licenseDetails.features.embeddingsGeneration.externalModels"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasEmbeddingsGeneration",
                    helperInfo: (
                        <ul>
                            {aiConnectionStringUtils.getExternalConnectorOptions().map((x) => (
                                <li key={x.value}>{x.label}</li>
                            ))}
                        </ul>
                    ),
                },
            ],
        },
        {
            name: t("licenseDetails.features.integration.title"),
            link: "https://ravendb.net/features#integration",
            items: [
                {
                    name: "RavenDB ETL",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasRavenEtl",
                },
                {
                    name: "SQL ETL",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasSqlEtl",
                },
                {
                    name: "Snowflake ETL",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasSnowflakeEtl",
                },
                {
                    name: "OLAP ETL",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasOlapEtl",
                },
                {
                    name: "Elasticsearch ETL",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasElasticSearchEtl",
                },
                {
                    name: t("licenseDetails.features.integration.postgreSqlProtocol"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPostgreSqlIntegration",
                },
                {
                    name: "Power BI",
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasPowerBI",
                },
                {
                    name: t("licenseDetails.features.integration.etlToKafka"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueEtl",
                },
                {
                    name: t("licenseDetails.features.integration.etlToRabbitMq"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueEtl",
                },
                {
                    name: t("licenseDetails.features.integration.etlToAzureQueueStorage"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueEtl",
                },
                {
                    name: t("licenseDetails.features.integration.etlToAmazonSqs"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueEtl",
                },
                {
                    name: t("licenseDetails.features.integration.kafkaSink"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueSink",
                },
                {
                    name: t("licenseDetails.features.integration.rabbitMqSink"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasQueueSink",
                },
                {
                    name: t("licenseDetails.features.integration.cdcSink"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasCdcSink",
                },
                {
                    name: t("licenseDetails.features.integration.grafanaPlugin"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
            ],
        },
        {
            name: t("licenseDetails.features.security.title"),
            link: "https://ravendb.net/features#security",
            items: [
                {
                    name: t("licenseDetails.features.security.certificates"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.security.readOnlyCertificates"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasReadOnlyCertificates",
                },
                {
                    name: t("licenseDetails.features.security.encryptionInTransit"),
                    agpl: { value: "TLS 1.3 & X.509" },
                    community: { value: "TLS 1.3 & X.509" },
                    professional: { value: "TLS 1.3 & X.509" },
                    enterprise: { value: "TLS 1.3 & X.509" },
                    developer: { value: "TLS 1.3 & X.509" },
                    enterpriseAi: { value: "TLS 1.3 & X.509" },
                    quill: { value: "TLS 1.3 & X.509" },
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.security.storageEncryption"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasEncryption",
                },
            ],
        },
        {
            name: t("licenseDetails.features.subscriptions.title"),
            link: "https://ravendb.net/features/extensions/subscriptions",
            items: [
                {
                    name: t("licenseDetails.features.subscriptions.dataSubscriptions"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.subscriptions.concurrent"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasConcurrentDataSubscriptions",
                },
                {
                    name: t("licenseDetails.features.subscriptions.revisions"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasRevisionsInSubscriptions",
                },
                {
                    name: t("licenseDetails.features.subscriptions.perDatabase"),
                    agpl: { value: 3 },
                    community: { value: 3 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfSubscriptionsPerDatabase",
                },
                {
                    name: t("licenseDetails.features.subscriptions.perCluster"),
                    agpl: { value: 15 },
                    community: { value: 15 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfSubscriptionsPerCluster",
                },
            ],
        },
        {
            name: t("licenseDetails.features.serverWide.title"),
            link: "https://ravendb.net/features/administration/periodic-backups",
            items: [
                {
                    name: t("licenseDetails.features.serverWide.backups"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasServerWideBackups",
                },
                {
                    name: t("licenseDetails.features.serverWide.externalReplications"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasServerWideExternalReplications",
                },
                {
                    name: t("licenseDetails.features.serverWide.customSorters"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasServerWideCustomSorters",
                },
                {
                    name: t("licenseDetails.features.serverWide.customAnalyzers"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasServerWideAnalyzers",
                },
            ],
        },
        {
            name: t("licenseDetails.features.settings.title"),
            link: null,
            items: [
                {
                    name: t("licenseDetails.features.settings.dataArchival"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasDataArchival",
                },
                {
                    name: t("licenseDetails.features.settings.customSorters"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.settings.customSortersPerDatabase"),
                    agpl: { value: 1 },
                    community: { value: 1 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfCustomSortersPerDatabase",
                },
                {
                    name: t("licenseDetails.features.settings.customSortersPerCluster"),
                    agpl: { value: 5 },
                    community: { value: 5 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfCustomSortersPerCluster",
                },
                {
                    name: t("licenseDetails.features.settings.customAnalyzers"),
                    ...availableEverywhere,
                    fieldInLicense: null,
                },
                {
                    name: t("licenseDetails.features.settings.customAnalyzersPerDatabase"),
                    agpl: { value: 1 },
                    community: { value: 1 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfCustomAnalyzersPerDatabase",
                },
                {
                    name: t("licenseDetails.features.settings.customAnalyzersPerCluster"),
                    agpl: { value: 5 },
                    community: { value: 5 },
                    professional: { value: Infinity },
                    enterprise: { value: Infinity },
                    developer: { value: Infinity },
                    enterpriseAi: { value: Infinity },
                    quill: { value: Infinity },
                    fieldInLicense: "MaxNumberOfCustomAnalyzersPerCluster",
                },
                {
                    name: t("licenseDetails.features.settings.clientConfiguration"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasClientConfiguration",
                },
                {
                    name: t("licenseDetails.features.settings.studioConfiguration"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasStudioConfiguration",
                },
                {
                    name: t("licenseDetails.features.settings.documentSchema"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: true },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasSchemaValidation",
                },
                {
                    name: t("licenseDetails.features.settings.remoteAttachments"),
                    agpl: { value: false },
                    community: { value: false },
                    professional: { value: false },
                    enterprise: { value: true },
                    developer: { value: true },
                    enterpriseAi: { value: true },
                    quill: { value: true },
                    fieldInLicense: "HasRemoteAttachments",
                },
            ],
        },
    ];
}

type LicenseColumn = "agpl" | "community" | "professional" | "enterprise" | "developer" | "enterpriseAi" | "quill";

interface ColumnsData {
    columns: LicenseColumn[];
    current: LicenseColumn;
}

function getColumnsData(license: Raven.Server.Commercial.LicenseType, isCloud: boolean): ColumnsData {
    const columnsData: ColumnsData = {
        columns: [],
        current: null,
    };

    switch (license) {
        case "Invalid":
            columnsData.columns = ["community", "professional", "enterprise", "enterpriseAi"];
            columnsData.current = null;
            break;
        case "None":
            columnsData.columns = ["agpl", "community", "professional", "enterprise", "enterpriseAi"];
            columnsData.current = "agpl";
            break;
        case "Quill":
            columnsData.columns = ["quill"];
            columnsData.current = "quill";
            break;
        case "Developer":
            columnsData.columns = ["community", "professional", "enterprise", "enterpriseAi", "developer"];
            columnsData.current = "developer";
            break;
        case "Professional":
            columnsData.columns = ["professional", "enterprise", "enterpriseAi"];
            columnsData.current = "professional";
            break;
        case "Enterprise":
            columnsData.columns = ["enterprise", "enterpriseAi"];
            columnsData.current = "enterprise";
            break;
        case "EnterpriseAi":
            columnsData.columns = ["enterpriseAi"];
            columnsData.current = "enterpriseAi";
            break;
        default:
            columnsData.columns = ["community", "professional", "enterprise", "enterpriseAi"];
            columnsData.current = "community";
            break;
    }

    if (isCloud) {
        columnsData.columns = columnsData.columns.filter(
            (column) => column !== "professional" && column !== "enterpriseAi"
        );
    }

    return columnsData;
}

function filterFeatureAvailabilitySection(
    features: FeatureAvailabilitySection[],
    searchText: string,
    viewMode: "showDiff" | "showAll",
    columns: LicenseColumn[],
    getEffectiveValue: (feature: FeatureAvailabilityItem, column: LicenseColumn) => any
) {
    const filteredSectionsBySearchText = features
        .map((section) => {
            if (!searchText) {
                return section;
            }

            const searchLower = searchText.toLocaleLowerCase();

            const sectionNameMatch = (section.name ?? "").toLocaleLowerCase().includes(searchLower);
            if (sectionNameMatch) {
                return section;
            }

            const filteredItems = section.items.filter((x) => x.name.toLocaleLowerCase().includes(searchLower));

            if (filteredItems.length === 0) {
                // skip entire section
                return null;
            }

            return {
                ...section,
                items: filteredItems,
            };
        })
        .filter((x) => x);

    if (viewMode === "showAll") {
        return filteredSectionsBySearchText;
    }

    // filter by second condition: 'view mode'
    return filteredSectionsBySearchText
        .map((section) => {
            const filteredItems = section.items.filter((item) => {
                const values = columns.map((x) => getEffectiveValue(item, x));
                // show if has differences
                return !values.every((x) => x === values[0]);
            });
            if (filteredItems.length === 0) {
                // skip entire section
                return null;
            }

            return {
                ...section,
                items: filteredItems,
            };
        })
        .filter((x) => x);
}

export const forTesting = {
    filterFeatureAvailabilitySection,
    get featureAvailabilityData() {
        const t = createTranslator("aboutPage");
        return getFeatureAvailabilityData(t);
    },
};

function FeatureValue(props: { value: AvailabilityValue; unit: FeatureValueUnit }) {
    const t = useStudioTranslation("aboutPage");
    const { value, unit } = props;
    switch (value) {
        case true:
            return <Icon icon="check" color="success" margin="m-0" />;
        case false:
            return <Icon icon="close" color="secondary" margin="m-0" />;
        case Infinity:
            return <Icon icon="infinity" margin="m-0" />;
        default:
            if (unit === "nodes") {
                return t("licenseDetails.nodesValue", { count: Number(value) });
            }
            if (unit === "memory") {
                return t("licenseDetails.memoryValue", { value: String(value) });
            }
            return value + " ";
    }
}

interface FeatureAvailabilitySection {
    name: string;
    link: string;
    className?: string;
    items: FeatureAvailabilityItem[];
}

type DisplayableLicenseField = keyof Omit<LicenseStatus, "Attributes" | "UpgradeRequired">;

interface FeatureAvailabilityItem {
    name: string;
    fieldInLicense: DisplayableLicenseField;
    agpl: ValueData;
    community: ValueData;
    professional: ValueData;
    enterprise: ValueData;
    developer: ValueData;
    enterpriseAi: ValueData;
    quill: ValueData;
    unit?: FeatureValueUnit;
    helperInfo?: React.ReactNode;
}

type FeatureValueUnit = "nodes" | "memory";

type AvailabilityValue = boolean | number | string;

interface ValueData {
    value: AvailabilityValue;
    overwrittenValue?: AvailabilityValue;
}

function UpgradeRow({ columns, current }: ColumnsData) {
    const t = useStudioTranslation("aboutPage");
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const data = columns.map((column) => ({
        column,
        isUpgradeable:
            column !== current && (column === "professional" || column === "enterprise" || column === "enterpriseAi"),
    }));

    return (
        <tr>
            <th className="pt-0"></th>
            {data.map((item) => (
                <th
                    key={item.column}
                    className={classNames(
                        "pt-0",
                        { "bg-current": item.column === current },
                        { "px-3": item.isUpgradeable }
                    )}
                >
                    {item.isUpgradeable && (
                        <>
                            {isCloud ? (
                                <a
                                    href="https://cloud.ravendb.net/pricing"
                                    target="_blank"
                                    className="btn btn-cloud rounded-pill"
                                >
                                    <Icon icon="cloud" />
                                    {t("licenseDetails.cloudPricing")}
                                </a>
                            ) : (
                                <a
                                    href="https://ravendb.net/buy"
                                    target="_blank"
                                    className="btn btn-primary rounded-pill"
                                >
                                    <Icon icon="upgrade-arrow" />
                                    {t("licenseDetails.upgradeLicense")}
                                </a>
                            )}
                        </>
                    )}
                </th>
            ))}
        </tr>
    );
}
