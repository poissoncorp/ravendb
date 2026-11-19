import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importFromFile");

type LicenseLimitKey = Extract<keyof LicenseStatus, `MaxNumberOf${string}`>;

export type LicenseLimitedImportItem = "subscriptions" | "customSorters" | "customAnalyzers";

interface LicenseLimitKeys {
    perDatabase: LicenseLimitKey;
    perCluster: LicenseLimitKey;
}

const licenseLimitKeys: Record<LicenseLimitedImportItem, LicenseLimitKeys> = {
    subscriptions: {
        perDatabase: "MaxNumberOfSubscriptionsPerDatabase",
        perCluster: "MaxNumberOfSubscriptionsPerCluster",
    },
    customSorters: {
        perDatabase: "MaxNumberOfCustomSortersPerDatabase",
        perCluster: "MaxNumberOfCustomSortersPerCluster",
    },
    customAnalyzers: {
        perDatabase: "MaxNumberOfCustomAnalyzersPerDatabase",
        perCluster: "MaxNumberOfCustomAnalyzersPerCluster",
    },
};

export function getLicenseLimitWarning(
    licenseStatus: LicenseStatus | null,
    item: LicenseLimitedImportItem
): string | null {
    const { perDatabase, perCluster } = licenseLimitKeys[item];
    const databaseLimit = licenseStatus?.[perDatabase];
    const clusterLimit = licenseStatus?.[perCluster];
    const hasDatabaseLimit = databaseLimit > 0;
    const hasClusterLimit = clusterLimit > 0;

    if (!hasDatabaseLimit && !hasClusterLimit) {
        return null;
    }

    return t(`licenseLimitWarnings.${item}`, {
        context: getLimitScope(hasDatabaseLimit, hasClusterLimit),
        perDatabase: databaseLimit,
        perCluster: clusterLimit,
    });
}

function getLimitScope(hasDatabaseLimit: boolean, hasClusterLimit: boolean) {
    if (hasDatabaseLimit && hasClusterLimit) {
        return "databaseAndCluster";
    }
    return hasDatabaseLimit ? "database" : "cluster";
}
