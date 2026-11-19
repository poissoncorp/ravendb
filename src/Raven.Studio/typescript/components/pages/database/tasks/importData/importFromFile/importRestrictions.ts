import { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import {
    ConnectionStringKey,
    connectionStringKeys,
    DatabaseSettingKey,
    DocumentToggleKey,
    OngoingTaskKey,
    ongoingTaskKeys,
} from "./importFromFileValidation";
import {
    connectionStringIcons,
    connectionStringLabels,
    databaseSettingIcons,
    databaseSettingLabels,
    ongoingTaskIcons,
    ongoingTaskLabels,
} from "./importFromFileLabels";
import IconName from "../../../../../../../typings/server/icons";
import { mergeCapabilities, OngoingTaskTarget } from "components/pages/database/tasks/shared/ongoingTaskCapabilities";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importFromFile");

type LicenseStatusKey = keyof LicenseStatus;

export type ImportRestrictionReason = "license" | "sharding" | "access";

export interface ImportRestriction {
    reason: ImportRestrictionReason;
    label: string;
    icon: IconName;
    licenseRequired?: LicenseBadgeText;
    tooltip: string;
}

interface ImportRestrictionRule {
    label: string;
    icon: IconName;
    licenseFlags?: LicenseStatusKey[];
    licenseRequired?: LicenseBadgeText;
    isShardingSupported?: boolean;
    accessRequired?: accessLevel;
}

export const documentToggleRules: Partial<Record<DocumentToggleKey, ImportRestrictionRule>> = {
    isIncludeArchivedDocuments: {
        get label() {
            return t("labels.archivedDocuments");
        },
        icon: "data-archival",
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
};

export const databaseSettingRules: Partial<Record<DatabaseSettingKey, ImportRestrictionRule>> = {
    revisions: {
        get label() {
            return databaseSettingLabels.revisions;
        },
        icon: databaseSettingIcons.revisions,
        licenseFlags: ["CanSetupDefaultRevisionsConfiguration"],
        licenseRequired: "Professional +",
    },
    documentsCompression: {
        get label() {
            return databaseSettingLabels.documentsCompression;
        },
        icon: databaseSettingIcons.documentsCompression,
        licenseFlags: ["HasDocumentsCompression"],
        licenseRequired: "Enterprise",
    },
    dataArchival: {
        get label() {
            return databaseSettingLabels.dataArchival;
        },
        icon: databaseSettingIcons.dataArchival,
        licenseFlags: ["HasDataArchival"],
        licenseRequired: "Enterprise",
    },
    timeSeries: {
        get label() {
            return databaseSettingLabels.timeSeries;
        },
        icon: databaseSettingIcons.timeSeries,
        licenseFlags: ["HasTimeSeriesRollupsAndRetention"],
        licenseRequired: "Professional +",
    },
    postgreSqlIntegration: {
        get label() {
            return databaseSettingLabels.postgreSqlIntegration;
        },
        icon: databaseSettingIcons.postgreSqlIntegration,
        licenseFlags: ["HasPostgreSqlIntegration"],
        licenseRequired: "Enterprise",
    },
    client: {
        get label() {
            return databaseSettingLabels.client;
        },
        icon: databaseSettingIcons.client,
        licenseFlags: ["HasClientConfiguration"],
        licenseRequired: "Professional +",
    },
    schemaValidation: {
        get label() {
            return databaseSettingLabels.schemaValidation;
        },
        icon: databaseSettingIcons.schemaValidation,
        licenseFlags: ["HasSchemaValidation"],
        licenseRequired: "Professional +",
    },
};

const ongoingTaskTargets: Record<OngoingTaskKey, OngoingTaskTarget[]> = {
    periodicBackups: ["PeriodicBackup"],
    externalReplications: ["ExternalReplication"],
    ravenEtls: ["RavenETL"],
    sqlEtls: ["SqlETL"],
    snowflakeEtls: ["SnowflakeETL"],
    olapEtls: ["OlapETL"],
    elasticSearchEtls: ["ElasticSearchETL"],
    queueEtls: ["KafkaETL", "RabbitMqETL", "AzureQueueStorageETL", "AmazonSqsETL"],
    hubReplications: ["ReplicationHub"],
    sinkReplications: ["ReplicationSink"],
    embeddingsGeneration: ["EmbeddingsGeneration"],
    genAi: ["GenAi"],
    aiAgents: ["AiAgent"],
    cdcSinks: ["CdcSink"],
    remoteAttachments: ["RemoteAttachments"],
};

export const ongoingTaskRules: Partial<Record<OngoingTaskKey, ImportRestrictionRule>> = Object.fromEntries(
    ongoingTaskKeys.map((key) => {
        const { licenseFlags, licenseBadge, isShardingSupported, accessRequired } = mergeCapabilities(
            ongoingTaskTargets[key]
        );

        return [
            key,
            {
                get label() {
                    return ongoingTaskLabels[key];
                },
                icon: ongoingTaskIcons[key],
                licenseFlags,
                licenseRequired: licenseBadge,
                isShardingSupported,
                accessRequired,
            } satisfies ImportRestrictionRule,
        ];
    })
);

const connectionStringTargets: Record<ConnectionStringKey, OngoingTaskTarget[]> = {
    ravenConnectionStrings: ["RavenETL"],
    sqlConnectionStrings: ["SqlETL"],
    snowflakeConnectionStrings: ["SnowflakeETL"],
    olapConnectionStrings: ["OlapETL"],
    elasticSearchConnectionStrings: ["ElasticSearchETL"],
    queueConnectionStrings: ["KafkaETL", "RabbitMqETL", "AzureQueueStorageETL", "AmazonSqsETL", "KafkaSink"],
    aiConnectionStrings: ["GenAi", "AiAgent", "EmbeddingsGeneration"],
};

export const connectionStringRules: Partial<Record<ConnectionStringKey, ImportRestrictionRule>> = Object.fromEntries(
    connectionStringKeys.map((key) => {
        const { licenseFlags, licenseBadge, accessRequired } = mergeCapabilities(connectionStringTargets[key]);

        return [
            key,
            {
                get label() {
                    return connectionStringLabels[key];
                },
                icon: connectionStringIcons[key],
                licenseFlags,
                licenseRequired: licenseBadge,
                accessRequired,
            } satisfies ImportRestrictionRule,
        ];
    })
);

export function getLicenseRestrictionTooltip(label: string) {
    return t("restrictionTooltips.license", { label });
}

export function getShardingRestrictionTooltip(label: string) {
    return t("restrictionTooltips.sharding", { label });
}

export function getAccessRestrictionTooltip(label: string) {
    return t("restrictionTooltips.access", { label });
}

export function resolveRestriction(
    rule: ImportRestrictionRule | undefined,
    context: {
        licenseStatus: LicenseStatus | null;
        isSharded: boolean;
        canHandleOperation: (requiredAccess: accessLevel) => boolean;
        isShardingChecked?: boolean;
    }
): ImportRestriction | null {
    if (!rule) {
        return null;
    }

    const { licenseStatus, isSharded, canHandleOperation, isShardingChecked = true } = context;

    const isLicenseRestricted =
        !!rule.licenseFlags?.length && rule.licenseFlags.every((flag) => !licenseStatus?.[flag]);

    if (isLicenseRestricted) {
        return {
            reason: "license",
            label: rule.label,
            icon: rule.icon,
            licenseRequired: rule.licenseRequired,
            tooltip: getLicenseRestrictionTooltip(rule.label),
        };
    }

    if (isShardingChecked && isSharded && !rule.isShardingSupported) {
        return {
            reason: "sharding",
            label: rule.label,
            icon: rule.icon,
            tooltip: getShardingRestrictionTooltip(rule.label),
        };
    }

    if (rule.accessRequired && !canHandleOperation(rule.accessRequired)) {
        return {
            reason: "access",
            label: rule.label,
            icon: rule.icon,
            tooltip: getAccessRestrictionTooltip(rule.label),
        };
    }

    return null;
}
