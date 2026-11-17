import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { useAsync } from "react-async-hook";
import { CounterBadge } from "components/common/CounterBadge";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getLicenseLimitReachStatus, LicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { DatabaseCustomAnalyzersInfoHub } from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersInfoHub";
import DatabaseCustomAnalyzersList from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersList";
import { useCustomAnalyzers } from "components/common/customAnalyzers/useCustomAnalyzers";
import DatabaseCustomAnalyzersServerWideList from "components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzersServerWideList";
import RichAlert from "components/common/RichAlert";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DatabaseCustomAnalyzers() {
    const t = useStudioTranslation("databaseCustomAnalyzers");
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { databasesService, manageServerService } = useServices();

    const { analyzers, setAnalyzers, addNewAnalyzer, removeAnalyzer, mapFromDto, markAsSaved } = useCustomAnalyzers();

    // Changing the database causes re-mount
    const asyncGetDatabaseAnalyzers = useAsync(() => databasesService.getCustomAnalyzers(db.name), [], {
        onSuccess(result) {
            setAnalyzers(mapFromDto(result));
        },
    });

    const asyncGetServerWideAnalyzers = useAsync(async () => {
        if (!hasServerWideCustomAnalyzers) {
            return [];
        }
        return await manageServerService.getServerWideCustomAnalyzers();
    }, []);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomAnalyzersPerDatabase"));
    const numberOfCustomAnalyzersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfAnalyzersInCluster;
    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const databaseResultsCount = analyzers.filter((x) => x.isSaved).length;
    const serverWideResultsCount = asyncGetServerWideAnalyzers.result?.length ?? null;

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomAnalyzersInCluster, licenseClusterLimit);

    const isLimitReached = databaseLimitReachStatus === "limitReached" || clusterLimitReachStatus === "limitReached";

    return (
        <div className="content-margin">
            <DatabaseLimitAlert
                databaseLimitReachStatus={databaseLimitReachStatus}
                databaseResultsCount={databaseResultsCount}
                licenseDatabaseLimit={licenseDatabaseLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <ClusterLimitAlert
                clusterLimitReachStatus={clusterLimitReachStatus}
                numberOfCustomAnalyzersInCluster={numberOfCustomAnalyzersInCluster}
                licenseClusterLimit={licenseClusterLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading title={t("title")} icon="custom-analyzers" />
                    {hasDatabaseAdminAccess && (
                        <ConditionalPopover
                            conditions={{
                                isActive: isLimitReached,
                                message: (
                                    <AddButtonLicensePopoverBody
                                        databaseLimitReachStatus={databaseLimitReachStatus}
                                        upgradeLicenseLink={upgradeLicenseLink}
                                    />
                                ),
                            }}
                        >
                            <Button
                                variant="primary"
                                className="mb-3"
                                onClick={addNewAnalyzer}
                                disabled={isLimitReached}
                            >
                                <Icon icon="plus" /> {t("addButton")}
                            </Button>
                        </ConditionalPopover>
                    )}

                    <HrHeader count={databaseLimitReachStatus === "notReached" ? databaseResultsCount : null}>
                        {t("databaseHeading")}
                        {databaseLimitReachStatus !== "notReached" && (
                            <CounterBadge className="ms-2" count={databaseResultsCount} limit={licenseDatabaseLimit} />
                        )}
                    </HrHeader>
                    <DatabaseCustomAnalyzersList
                        analyzers={analyzers}
                        fetchStatus={asyncGetDatabaseAnalyzers.status}
                        reload={asyncGetDatabaseAnalyzers.execute}
                        serverWideAnalyzerNames={asyncGetServerWideAnalyzers.result?.map((x) => x.Name) ?? []}
                        remove={removeAnalyzer}
                        markAsSaved={markAsSaved}
                    />

                    <HrHeader
                        right={
                            <a
                                href={appUrl.forServerWideCustomAnalyzers()}
                                target="_blank"
                                title={t("serverWideLinkTitle")}
                            >
                                <Icon icon="link" />
                                {t("serverWideHeading")}
                            </a>
                        }
                        count={serverWideResultsCount}
                    >
                        {t("serverWideHeading")}
                        {!hasServerWideCustomAnalyzers && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                    </HrHeader>
                    {hasServerWideCustomAnalyzers && (
                        <DatabaseCustomAnalyzersServerWideList asyncGetAnalyzers={asyncGetServerWideAnalyzers} />
                    )}
                </Col>
                <Col sm={12} lg={4}>
                    <DatabaseCustomAnalyzersInfoHub databaseAnalyzersCount={databaseResultsCount} />
                </Col>
            </Row>
        </div>
    );
}

interface DatabaseLimitAlertProps {
    databaseLimitReachStatus: LicenseLimitReachStatus;
    databaseResultsCount: number;
    licenseDatabaseLimit: number;
    upgradeLicenseLink: string;
}

function DatabaseLimitAlert(props: DatabaseLimitAlertProps) {
    const { databaseLimitReachStatus, databaseResultsCount, licenseDatabaseLimit, upgradeLicenseLink } = props;

    if (databaseLimitReachStatus === "notReached") {
        return null;
    }

    return (
        <RichAlert
            variant={databaseLimitReachStatus === "limitReached" ? "danger" : "warning"}
            icon="database"
            iconAddon="warning"
            className="mb-3"
        >
            <StudioTrans
                ns="databaseCustomAnalyzers"
                i18nKey="databaseLimitAlert"
                options={{ context: databaseLimitReachStatus, used: databaseResultsCount, limit: licenseDatabaseLimit }}
                components={{ strong: <strong />, br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </RichAlert>
    );
}

interface ClusterLimitAlertProps {
    clusterLimitReachStatus: LicenseLimitReachStatus;
    numberOfCustomAnalyzersInCluster: number;
    licenseClusterLimit: number;
    upgradeLicenseLink: string;
}

function ClusterLimitAlert(props: ClusterLimitAlertProps) {
    const { clusterLimitReachStatus, numberOfCustomAnalyzersInCluster, licenseClusterLimit, upgradeLicenseLink } =
        props;

    if (clusterLimitReachStatus === "notReached") {
        return null;
    }

    return (
        <RichAlert
            variant={clusterLimitReachStatus === "limitReached" ? "danger" : "warning"}
            icon="cluster"
            iconAddon="warning"
            className="mb-3"
        >
            <StudioTrans
                ns="databaseCustomAnalyzers"
                i18nKey="clusterLimitAlert"
                options={{
                    context: clusterLimitReachStatus,
                    used: numberOfCustomAnalyzersInCluster,
                    limit: licenseClusterLimit,
                }}
                components={{ strong: <strong />, br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </RichAlert>
    );
}

interface AddButtonLicensePopoverProps {
    databaseLimitReachStatus: LicenseLimitReachStatus;
    upgradeLicenseLink: string;
}

function AddButtonLicensePopoverBody({ databaseLimitReachStatus, upgradeLicenseLink }: AddButtonLicensePopoverProps) {
    return (
        <div className="text-center">
            <Icon icon={databaseLimitReachStatus === "limitReached" ? "database" : "cluster"} />
            <StudioTrans
                ns="databaseCustomAnalyzers"
                i18nKey="addButtonLimitPopover"
                options={{ context: databaseLimitReachStatus === "limitReached" ? "database" : "cluster" }}
                components={{ br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </div>
    );
}
