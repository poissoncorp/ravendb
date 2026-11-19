import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { useAsync } from "react-async-hook";
import { CounterBadge } from "components/common/CounterBadge";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { LicenseLimitReachStatus, getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useRavenLink } from "components/hooks/useRavenLink";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { DatabaseCustomSortersInfoHub } from "components/pages/database/settings/customSorters/DatabaseCustomSortersInfoHub";
import DatabaseCustomSortersList from "components/pages/database/settings/customSorters/DatabaseCustomSortersList";
import DatabaseCustomSortersServerWideList from "components/pages/database/settings/customSorters/DatabaseCustomSortersServerWideList";
import { useCustomSorters } from "components/common/customSorters/useCustomSorters";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import RichAlert from "components/common/RichAlert";
import Button from "react-bootstrap/Button";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DatabaseCustomSorters() {
    const t = useStudioTranslation("customSorters");
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { databasesService, manageServerService } = useServices();

    const { sorters, setSorters, addNewSorter, removeSorter, mapFromDto, markAsSaved } = useCustomSorters();

    // Changing the database causes re-mount
    const asyncGetDatabaseSorters = useAsync(() => databasesService.getCustomSorters(db.name), [], {
        onSuccess(result) {
            setSorters(mapFromDto(result));
        },
    });

    const asyncGetServerWideSorters = useAsync(async () => {
        if (!hasServerWideCustomSorters) {
            return [];
        }
        return await manageServerService.getServerWideCustomSorters();
    }, []);

    const { appUrl } = useAppUrls();
    const upgradeLicenseLink = useRavenLink({ hash: "FLDLO4", isDocs: false });

    const licenseClusterLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerCluster"));
    const licenseDatabaseLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfCustomSortersPerDatabase"));
    const numberOfCustomSortersInCluster = useAppSelector(licenseSelectors.limitsUsage).NumberOfCustomSortersInCluster;
    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));

    const databaseResultsCount = sorters.filter((x) => x.isSaved).length;
    const serverWideResultsCount = asyncGetServerWideSorters.result?.length ?? null;

    const databaseLimitReachStatus = getLicenseLimitReachStatus(databaseResultsCount, licenseDatabaseLimit);
    const clusterLimitReachStatus = getLicenseLimitReachStatus(numberOfCustomSortersInCluster, licenseClusterLimit);

    const isLimitReached = databaseLimitReachStatus === "limitReached" || clusterLimitReachStatus === "limitReached";

    if (db.isSharded) {
        return (
            <FeatureNotAvailable>
                <span>
                    <StudioTrans
                        ns="customSorters"
                        i18nKey="shardedNotAvailable"
                        components={{ icon: <Icon icon="sharding" color="shard" margin="m-0" /> }}
                    />
                </span>
            </FeatureNotAvailable>
        );
    }

    return (
        <Row className="gy-sm content-margin">
            <DatabaseLimitAlert
                databaseLimitReachStatus={databaseLimitReachStatus}
                databaseResultsCount={databaseResultsCount}
                licenseDatabaseLimit={licenseDatabaseLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <ClusterLimitAlert
                clusterLimitReachStatus={clusterLimitReachStatus}
                numberOfCustomSortersInCluster={numberOfCustomSortersInCluster}
                licenseClusterLimit={licenseClusterLimit}
                upgradeLicenseLink={upgradeLicenseLink}
            />
            <Col sm={12} lg={8}>
                <AboutViewHeading title={t("title")} icon="custom-sorters" />
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
                        <Button variant="primary" className="mb-3" onClick={addNewSorter} disabled={isLimitReached}>
                            <Icon icon="plus" />
                            {t("addButton")}
                        </Button>
                    </ConditionalPopover>
                )}

                <HrHeader count={databaseLimitReachStatus === "notReached" ? databaseResultsCount : null}>
                    {t("databaseHeading")}
                    {databaseLimitReachStatus !== "notReached" && (
                        <CounterBadge className="ms-2" count={databaseResultsCount} limit={licenseDatabaseLimit} />
                    )}
                </HrHeader>
                <DatabaseCustomSortersList
                    sorters={sorters}
                    fetchStatus={asyncGetDatabaseSorters.status}
                    reload={asyncGetDatabaseSorters.execute}
                    serverWideSorterNames={asyncGetServerWideSorters.result?.map((x) => x.Name) ?? []}
                    takenNames={sorters.map((s) => s.name).filter(Boolean)}
                    remove={removeSorter}
                    markAsSaved={markAsSaved}
                />

                <HrHeader
                    right={
                        hasServerWideCustomSorters ? (
                            <a
                                href={appUrl.forServerWideCustomSorters()}
                                target="_blank"
                                title={t("serverWideLinkTitle")}
                            >
                                <Icon icon="link" />
                                {t("serverWideHeading")}
                            </a>
                        ) : null
                    }
                    count={serverWideResultsCount}
                >
                    {t("serverWideHeading")}
                    {!hasServerWideCustomSorters && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                </HrHeader>
                {hasServerWideCustomSorters && (
                    <DatabaseCustomSortersServerWideList asyncGetSorters={asyncGetServerWideSorters} />
                )}
            </Col>
            <Col sm={12} lg={4}>
                <DatabaseCustomSortersInfoHub databaseSortersCount={databaseResultsCount} />
            </Col>
        </Row>
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
                ns="customSorters"
                i18nKey="databaseLimitAlert"
                options={{ context: databaseLimitReachStatus, used: databaseResultsCount, limit: licenseDatabaseLimit }}
                components={{ strong: <strong />, br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </RichAlert>
    );
}

interface ClusterLimitAlertProps {
    clusterLimitReachStatus: LicenseLimitReachStatus;
    numberOfCustomSortersInCluster: number;
    licenseClusterLimit: number;
    upgradeLicenseLink: string;
}

function ClusterLimitAlert(props: ClusterLimitAlertProps) {
    const { clusterLimitReachStatus, numberOfCustomSortersInCluster, licenseClusterLimit, upgradeLicenseLink } = props;

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
                ns="customSorters"
                i18nKey="clusterLimitAlert"
                options={{
                    context: clusterLimitReachStatus,
                    used: numberOfCustomSortersInCluster,
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
                ns="customSorters"
                i18nKey="addButtonLimitPopover"
                options={{ context: databaseLimitReachStatus === "limitReached" ? "database" : "cluster" }}
                components={{ br: <br />, a: <a href={upgradeLicenseLink} target="_blank" /> }}
            />
        </div>
    );
}
