import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import reactUtils = require("common/reactUtils");
import ManageDatabaseGroupPage = require("components/pages/resources/manageDatabaseGroup/ManageDatabaseGroupPage");
import ClientDatabaseConfiguration = require("components/pages/database/settings/clientConfiguration/ClientDatabaseConfiguration");
import StudioDatabaseConfiguration = require("components/pages/database/settings/studioConfiguration/StudioDatabaseConfiguration");
import DocumentRefresh = require("components/pages/database/settings/documentRefresh/DocumentRefresh");
import DataArchival = require("components/pages/database/settings/dataArchival/DataArchival");
import DocumentExpiration = require("components/pages/database/settings/documentExpiration/DocumentExpiration");
import DocumentRevisions = require("components/pages/database/settings/documentRevisions/DocumentRevisions");
import DatabaseCustomSorters = require("components/pages/database/settings/customSorters/DatabaseCustomSorters");
import DatabaseCustomAnalyzers = require("components/pages/database/settings/customAnalyzers/DatabaseCustomAnalyzers");
import DocumentCompression = require("components/pages/database/settings/documentCompression/DocumentCompression");
import RevertRevisions = require("components/pages/database/settings/documentRevisions/revertRevisions/RevertRevisions");
import ConnectionStrings = require("components/pages/database/settings/connectionStrings/ConnectionStrings");
import DatabaseRecord = require("components/pages/database/settings/databaseRecord/DatabaseRecord");
import ConflictResolution = require("components/pages/database/settings/conflictResolution/ConflictResolution");
import Integrations = require("components/pages/database/settings/integrations/Integrations");
import UnusedDatabaseIds = require("components/pages/database/settings/unusedDatabaseIds/UnusedDatabaseIds");
import RevisionsBinCleaner = require("components/pages/database/settings/revisionsBinCleaner/RevisionsBinCleaner");
import RemoteAttachments = require("components/pages/database/settings/remoteAttachments/RemoteAttachments");
import DocumentSchema = require("components/pages/database/settings/documentSchema/DocumentSchema");
import DocumentSchemaPlayground = require("components/pages/database/settings/documentSchema/partials/DocumentSchemaPlayground");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getSettingsMenuItem;

function getSettingsMenuItem(appUrls: computedAppUrls) {
    
    const settingsItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/settings/databaseSettings',
            moduleId: require('viewmodels/database/settings/databaseSettings'),
            shardingMode: "allShards",
            title: t("settings.databaseSettings"),
            nav: true,
            css: 'icon-database-settings',
            dynamicHash: appUrls.databaseSettings,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/settings/connectionStrings',
            moduleId: reactUtils.bridgeToReact(ConnectionStrings.default, "nonShardedView"),
            title: t("settings.connectionStrings.title"),
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrls.connectionStrings,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    {
                        name: t("settings.connectionStrings.addNew"),
                        alternativeNames: [
                            t("settings.connectionStrings.create"),
                            "RavenDB",
                            "SQL",
                            "Snowflake",
                            "OLAP",
                            "ElasticSearch",
                            "Kafka",
                            "RabbitMQ",
                        ],
                    },
                    {
                        name: t("settings.connectionStrings.delete"),
                        alternativeNames: [t("settings.connectionStrings.remove")]
                    },
                    { name: t("settings.connectionStrings.edit") },
                    { name: t("settings.connectionStrings.test") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/conflictResolution',
            moduleId: reactUtils.bridgeToReact(ConflictResolution.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.conflictResolution"),
            nav: true,
            css: 'icon-conflicts-resolution',
            dynamicHash: appUrls.conflictResolution,
        }),
        new leafMenuItem({
            route: 'databases/settings/clientConfiguration',
            moduleId: reactUtils.bridgeToReact(ClientDatabaseConfiguration.default, "nonShardedView"),
            search: {
                innerActions: [
                    { name: t("settings.clientConfiguration.identityPartsSeparator") },
                    { name: t("settings.clientConfiguration.maxNumberOfRequestsPerSession") },
                    { name: t("settings.clientConfiguration.loadBalanceBehavior") },
                    { name: t("settings.clientConfiguration.seed") },
                    { name: t("settings.clientConfiguration.readBalanceBehavior") },
                ],
            },
            shardingMode: "allShards",
            title: t("settings.clientConfiguration.title"),
            nav: true,
            css: 'icon-database-client-configuration',
            dynamicHash: appUrls.clientConfiguration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/studioConfiguration',
            search: {
                innerActions: [
                    { name: t("settings.studioConfiguration.databaseEnvironment") },
                    { name: t("settings.studioConfiguration.disableCreatingNewAutoIndexes") }
                ],
            },
            moduleId: reactUtils.bridgeToReact(StudioDatabaseConfiguration.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.studioConfiguration.title"),
            nav: true,
            css: 'icon-database-studio-configuration',
            dynamicHash: appUrls.studioConfiguration,
            requiredAccess: "DatabaseAdmin"
        }),
        new leafMenuItem({
            route: 'databases/settings/refresh',
            moduleId: reactUtils.bridgeToReact(DocumentRefresh.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.documentRefresh.title"),
            nav: true,
            css: 'icon-expos-refresh',
            dynamicHash: appUrls.refresh,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: t("settings.documentRefresh.enable") },
                    { name: t("settings.documentRefresh.setCustomFrequency") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/expiration',
            moduleId: reactUtils.bridgeToReact(DocumentExpiration.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.documentExpiration.title"),
            nav: true,
            css: 'icon-document-expiration',
            dynamicHash: appUrls.expiration,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: t("settings.documentExpiration.enable") },
                    { name: t("settings.documentExpiration.setCustomFrequency") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/documentsCompression',
            moduleId: reactUtils.bridgeToReact(DocumentCompression.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.documentCompression.title"),
            nav: true,
            css: 'icon-documents-compression',
            dynamicHash: appUrls.documentsCompression,
            search: {
                innerActions: [
                    { name: t("settings.documentCompression.compressAllRevisions") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/documentSchema',
            moduleId: reactUtils.bridgeToReact(DocumentSchema.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.documentSchema"),
            nav: true,
            requiredAccess: "DatabaseAdmin",
            css: 'icon-document-schema',
            dynamicHash: appUrls.documentSchema,
        }),
        new leafMenuItem({
            route: 'databases/settings/documentSchema/playground',
            moduleId: reactUtils.bridgeToReact(DocumentSchemaPlayground.default, "nonShardedView"),
            title: t("settings.documentSchemaPlayground"),
            nav: false,
            css: 'icon-document',
            dynamicHash: appUrls.documentSchemaPlayground,
            requiredAccess: "DatabaseAdmin",
            itemRouteToHighlight: "databases/settings/documentSchema",
        }),
        new leafMenuItem({
            route: 'databases/settings/revisions',
            moduleId: reactUtils.bridgeToReact(DocumentRevisions.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.documentRevisions.title"),
            search: {
                innerActions: [
                    { name: t("settings.documentRevisions.enforceConfiguration") },
                    { name: t("settings.documentRevisions.addNew"), alternativeNames: [t("settings.documentRevisions.create")] },
                    { name: t("settings.documentRevisions.delete"), alternativeNames: [t("settings.documentRevisions.remove")] },
                    { name: t("settings.documentRevisions.edit") },
                    { name: t("settings.documentRevisions.enable") },
                    { name: t("settings.documentRevisions.disable") },
                ],
            },
            nav: true,
            css: 'icon-revisions',
            dynamicHash: appUrls.revisions
        }),
        new leafMenuItem({
            route: 'databases/settings/revisionsBinCleaner',
            moduleId: reactUtils.bridgeToReact(RevisionsBinCleaner.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.revisionsBinCleaner.title"),
            nav: true,
            css: 'icon-revisions-bin',
            dynamicHash: appUrls.revisionsBinCleaner,
            search: {
                innerActions: [
                    { name: t("settings.revisionsBinCleaner.setAutomaticConfiguration") },
                    { name: t("settings.revisionsBinCleaner.configureRetentionPolicies") },
                    { name: t("settings.revisionsBinCleaner.viewCurrentConfiguration") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/revertRevisions',
            moduleId: reactUtils.bridgeToReact(RevertRevisions.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.revertRevisions"),
            nav: false,
            css: 'icon-revert-revisions',
            dynamicHash: appUrls.revertRevisions,
            itemRouteToHighlight: "databases/settings/revisions",
        }),
        new leafMenuItem({
            route: 'databases/settings/dataArchival',
            moduleId: reactUtils.bridgeToReact(DataArchival.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.dataArchival.title"),
            nav: true,
            css: 'icon-data-archival',
            dynamicHash: appUrls.dataArchival,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    { name: t("settings.dataArchival.enable") },
                    { name: t("settings.dataArchival.setCustomFrequency") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/remoteAttachments',
            moduleId: reactUtils.bridgeToReact(RemoteAttachments.default, 'nonShardedView'),
            shardingMode: "allShards",
            title: t("settings.remoteAttachments.title"),
            nav: true,
            css: "icon-remote-attachment",
            dynamicHash: appUrls.remoteAttachments,
            requiredAccess: "DatabaseAdmin",
            search: {
                innerActions: [
                    {
                        name: t("settings.remoteAttachments.enable"),
                        alternativeNames: [t("settings.remoteAttachments.turnOn"), t("settings.remoteAttachments.activate")]
                    },
                    {
                        name: t("settings.remoteAttachments.addNewDestination"),
                        alternativeNames: [t("settings.remoteAttachments.createDestination"), t("settings.remoteAttachments.addRemoteDestination"), t("settings.remoteAttachments.newAttachmentDestination")]
                    },
                    {
                        name: t("settings.remoteAttachments.deleteDestination"),
                        alternativeNames: [t("settings.remoteAttachments.removeDestination"), t("settings.remoteAttachments.removeRemoteDestination"), t("settings.remoteAttachments.deleteRemoteAttachment")]
                    },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/timeSeries',
            moduleId: require('viewmodels/database/settings/timeSeries'),
            shardingMode: "allShards",
            title: t("settings.timeSeries.title"),
            nav: true, 
            css: 'icon-timeseries-settings',
            dynamicHash: appUrls.timeSeries,
            search: {
                innerActions: [
                    { name: t("settings.timeSeries.enable") },
                    { name: t("settings.timeSeries.disable") },
                    { name: t("settings.timeSeries.policyCheckFrequency") },
                    { name: t("settings.timeSeries.delete"), alternativeNames: [t("settings.timeSeries.remove")] },
                    { name: t("settings.timeSeries.edit") },
                    { name: t("settings.timeSeries.addNewCollectionConfiguration"), alternativeNames: [t("settings.timeSeries.createCollectionConfiguration")] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/customSorters',
            moduleId: reactUtils.bridgeToReact(DatabaseCustomSorters.default, "nonShardedView"),
            title: t("settings.customSorters.title"),
            shardingMode: "allShards",
            nav: true,
            css: 'icon-custom-sorters',
            dynamicHash: appUrls.customSorters,
            search: {
                innerActions: [
                    { name: t("settings.customSorters.addNew"), alternativeNames: [t("settings.customSorters.create")] },
                    { name: t("settings.customSorters.delete"), alternativeNames: [t("settings.customSorters.remove")] },
                    { name: t("settings.customSorters.edit") },
                    { name: t("settings.customSorters.test") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/customAnalyzers',
            moduleId: reactUtils.bridgeToReact(DatabaseCustomAnalyzers.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.customAnalyzers.title"),
            nav: true,
            css: 'icon-custom-analyzers',
            dynamicHash: appUrls.customAnalyzers,
            search: {
                innerActions: [
                    { name: t("settings.customAnalyzers.addNew"), alternativeNames: [t("settings.customAnalyzers.create")] },
                    { name: t("settings.customAnalyzers.delete"), alternativeNames: [t("settings.customAnalyzers.remove")] },
                    { name: t("settings.customAnalyzers.edit") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/manageDatabaseGroup',
            moduleId: reactUtils.bridgeToReact(ManageDatabaseGroupPage.ManageDatabaseGroupPage, "nonShardedView"),
            title: t("settings.manageDatabaseGroup.title"),
            nav: true,
            css: 'icon-manage-dbgroup',
            dynamicHash: appUrls.manageDatabaseGroup,
            search: {
                innerActions: [
                    { name: t("settings.manageDatabaseGroup.addNode") },
                    { name: t("settings.manageDatabaseGroup.reorderNodes") },
                    { name: t("settings.manageDatabaseGroup.allowDynamicDistribution") },
                    { name: t("settings.manageDatabaseGroup.addShard") },
                    { name: t("settings.manageDatabaseGroup.deleteFromGroup"), alternativeNames: [t("settings.manageDatabaseGroup.removeFromGroup")] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/settings/integrations',
            moduleId: reactUtils.bridgeToReact(Integrations.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.integrations.title"),
            nav: true,
            css: 'icon-integrations',
            dynamicHash: appUrls.integrations,
            requiredAccess: "DatabaseAdmin",
            search: {
                alternativeTitles: [t("settings.integrations.postgreSqlCredentials")],
                innerActions: [
                    { name: t("settings.integrations.addNewCredentials"), alternativeNames: [t("settings.integrations.createCredentials")] },
                    { name: t("settings.integrations.deleteCredentials"), alternativeNames: [t("settings.integrations.removeCredentials")] },
                ]
            }
        }),
        new separatorMenuItem(),
        new separatorMenuItem(t("settings.advanced")),
        new leafMenuItem({
            route: 'databases/advanced/databaseRecord',
            moduleId: reactUtils.bridgeToReact(DatabaseRecord.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.databaseRecord"),
            nav: true,
            css: 'icon-database-record',
            dynamicHash: appUrls.databaseRecord,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'databases/advanced/databaseIDs',
            moduleId: reactUtils.bridgeToReact(UnusedDatabaseIds.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("settings.unusedDatabaseIds"),
            nav: true,
            css: 'icon-database-id',
            dynamicHash: appUrls.databaseIDs,
            requiredAccess: "Operator"
        })
    ];

    return new intermediateMenuItem(t("settings.title"), settingsItems, 'icon-settings');
}
