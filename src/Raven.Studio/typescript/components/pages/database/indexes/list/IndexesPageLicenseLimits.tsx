import { LicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import RichAlert from "components/common/RichAlert";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface IndexesPageLicenseLimitsProps {
    staticClusterLimitStatus: LicenseLimitReachStatus;
    staticClusterCount: number;
    staticClusterLimit: number;
    upgradeLicenseLink: string;
    autoClusterLimitStatus: LicenseLimitReachStatus;
    autoClusterCount: number;
    autoClusterLimit: number;
    staticDatabaseLimitStatus: LicenseLimitReachStatus;
    staticDatabaseCount: number;
    staticDatabaseLimit: number;
    autoDatabaseLimitStatus: LicenseLimitReachStatus;
    autoDatabaseCount: number;
    autoDatabaseLimit: number;
}

export default function IndexesPageLicenseLimits({
    staticClusterLimitStatus,
    staticClusterCount,
    staticClusterLimit,
    upgradeLicenseLink,
    autoClusterLimitStatus,
    autoClusterCount,
    autoClusterLimit,
    staticDatabaseLimitStatus,
    staticDatabaseCount,
    staticDatabaseLimit,
    autoDatabaseLimitStatus,
    autoDatabaseCount,
    autoDatabaseLimit,
}: IndexesPageLicenseLimitsProps) {
    return (
        <>
            {staticClusterLimitStatus !== "notReached" && (
                <RichAlert
                    variant={staticClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="cluster"
                    iconAddon="warning"
                    className="mb-3"
                >
                    <StudioTrans
                        ns="indexesList"
                        i18nKey="licenseLimits.staticCluster"
                        components={{
                            strong: <strong />,
                            a: <a href={upgradeLicenseLink} target="_blank" className="no-decor" />,
                        }}
                        options={{
                            context: staticClusterLimitStatus,
                            used: staticClusterCount,
                            limit: staticClusterLimit,
                        }}
                    />
                </RichAlert>
            )}

            {autoClusterLimitStatus !== "notReached" && (
                <RichAlert
                    variant={autoClusterLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="cluster"
                    iconAddon="warning"
                    className="mb-3"
                >
                    <StudioTrans
                        ns="indexesList"
                        i18nKey="licenseLimits.autoCluster"
                        components={{
                            strong: <strong />,
                            a: <a href={upgradeLicenseLink} target="_blank" className="no-decor" />,
                        }}
                        options={{ context: autoClusterLimitStatus, used: autoClusterCount, limit: autoClusterLimit }}
                    />
                </RichAlert>
            )}

            {staticDatabaseLimitStatus !== "notReached" && (
                <RichAlert
                    variant={staticDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="database"
                    iconAddon="warning"
                    className="mb-3"
                >
                    <StudioTrans
                        ns="indexesList"
                        i18nKey="licenseLimits.staticDatabase"
                        components={{
                            strong: <strong />,
                            a: <a href={upgradeLicenseLink} target="_blank" className="no-decor" />,
                        }}
                        options={{
                            context: staticDatabaseLimitStatus,
                            used: staticDatabaseCount,
                            limit: staticDatabaseLimit,
                        }}
                    />
                </RichAlert>
            )}

            {autoDatabaseLimitStatus !== "notReached" && (
                <RichAlert
                    variant={autoDatabaseLimitStatus === "limitReached" ? "danger" : "warning"}
                    icon="database"
                    iconAddon="warning"
                    className="mb-3"
                >
                    <StudioTrans
                        ns="indexesList"
                        i18nKey="licenseLimits.autoDatabase"
                        components={{
                            strong: <strong />,
                            a: <a href={upgradeLicenseLink} target="_blank" className="no-decor" />,
                        }}
                        options={{
                            context: autoDatabaseLimitStatus,
                            used: autoDatabaseCount,
                            limit: autoDatabaseLimit,
                        }}
                    />
                </RichAlert>
            )}
        </>
    );
}
