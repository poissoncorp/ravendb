import IconName from "../../../../../../../typings/server/icons";
import { ConnectionStringKey, DatabaseSettingKey, OngoingTaskKey } from "./importFromFileValidation";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("importFromFile");

export const databaseSettingLabels: Record<DatabaseSettingKey, string> = {
    get settings() {
        return t("labels.databaseSettings.settings");
    },
    get conflictSolverConfig() {
        return t("labels.databaseSettings.conflictSolverConfig");
    },
    get client() {
        return t("labels.databaseSettings.client");
    },
    get revisions() {
        return t("labels.databaseSettings.revisions");
    },
    get refresh() {
        return t("labels.databaseSettings.refresh");
    },
    get expiration() {
        return t("labels.databaseSettings.expiration");
    },
    get documentsCompression() {
        return t("labels.databaseSettings.documentsCompression");
    },
    get schemaValidation() {
        return t("labels.databaseSettings.schemaValidation");
    },
    get dataArchival() {
        return t("labels.databaseSettings.dataArchival");
    },
    get timeSeries() {
        return t("labels.databaseSettings.timeSeries");
    },
    get sorters() {
        return t("labels.databaseSettings.sorters");
    },
    get analyzers() {
        return t("labels.databaseSettings.analyzers");
    },
    get postgreSqlIntegration() {
        return t("labels.databaseSettings.postgreSqlIntegration");
    },
};

export const ongoingTaskLabels: Record<OngoingTaskKey, string> = {
    get periodicBackups() {
        return t("labels.ongoingTasks.periodicBackups");
    },
    get externalReplications() {
        return t("labels.ongoingTasks.externalReplications");
    },
    get ravenEtls() {
        return t("labels.ongoingTasks.ravenEtls");
    },
    get sqlEtls() {
        return t("labels.ongoingTasks.sqlEtls");
    },
    get snowflakeEtls() {
        return t("labels.ongoingTasks.snowflakeEtls");
    },
    get olapEtls() {
        return t("labels.ongoingTasks.olapEtls");
    },
    get elasticSearchEtls() {
        return t("labels.ongoingTasks.elasticSearchEtls");
    },
    get queueEtls() {
        return t("labels.ongoingTasks.queueEtls");
    },
    get hubReplications() {
        return t("labels.ongoingTasks.hubReplications");
    },
    get sinkReplications() {
        return t("labels.ongoingTasks.sinkReplications");
    },
    get embeddingsGeneration() {
        return t("labels.ongoingTasks.embeddingsGeneration");
    },
    get genAi() {
        return t("labels.ongoingTasks.genAi");
    },
    get cdcSinks() {
        return t("labels.ongoingTasks.cdcSinks");
    },
    get aiAgents() {
        return t("labels.ongoingTasks.aiAgents");
    },
    get remoteAttachments() {
        return t("labels.ongoingTasks.remoteAttachments");
    },
};

export const connectionStringLabels: Record<ConnectionStringKey, string> = {
    get ravenConnectionStrings() {
        return t("labels.connectionStrings.ravenConnectionStrings");
    },
    get sqlConnectionStrings() {
        return t("labels.connectionStrings.sqlConnectionStrings");
    },
    get snowflakeConnectionStrings() {
        return t("labels.connectionStrings.snowflakeConnectionStrings");
    },
    get olapConnectionStrings() {
        return t("labels.connectionStrings.olapConnectionStrings");
    },
    get elasticSearchConnectionStrings() {
        return t("labels.connectionStrings.elasticSearchConnectionStrings");
    },
    get queueConnectionStrings() {
        return t("labels.connectionStrings.queueConnectionStrings");
    },
    get aiConnectionStrings() {
        return t("labels.connectionStrings.aiConnectionStrings");
    },
};

export const databaseSettingIcons: Record<DatabaseSettingKey, IconName> = {
    settings: "database-settings",
    conflictSolverConfig: "conflicts-resolution",
    client: "database-client-configuration",
    revisions: "revisions",
    refresh: "expos-refresh",
    expiration: "document-expiration",
    documentsCompression: "documents-compression",
    schemaValidation: "document-schema",
    dataArchival: "data-archival",
    timeSeries: "timeseries-settings",
    sorters: "custom-sorters",
    analyzers: "custom-analyzers",
    postgreSqlIntegration: "postgresql",
};

export const ongoingTaskIcons: Record<OngoingTaskKey, IconName> = {
    periodicBackups: "periodic-backup",
    externalReplications: "external-replication",
    ravenEtls: "ravendb-etl",
    sqlEtls: "sql-etl",
    snowflakeEtls: "snowflake-etl",
    olapEtls: "olap-etl",
    elasticSearchEtls: "elastic-search-etl",
    queueEtls: "kafka-etl",
    hubReplications: "pull-replication-hub",
    sinkReplications: "pull-replication-agent",
    embeddingsGeneration: "ai-etl",
    genAi: "genai",
    cdcSinks: "sql-etl",
    aiAgents: "ai-agents",
    remoteAttachments: "remote-attachment",
};

export const connectionStringIcons: Record<ConnectionStringKey, IconName> = {
    ravenConnectionStrings: "ravendb-etl",
    sqlConnectionStrings: "sql-etl",
    snowflakeConnectionStrings: "snowflake-etl",
    olapConnectionStrings: "olap-etl",
    elasticSearchConnectionStrings: "elastic-search-etl",
    queueConnectionStrings: "kafka-etl",
    aiConnectionStrings: "ai",
};
