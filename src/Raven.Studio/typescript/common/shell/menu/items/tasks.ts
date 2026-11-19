import reactUtils = require("common/reactUtils");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import BackupsPage = require("components/pages/database/tasks/backups/BackupsPage");
import CreateSampleData = require("components/pages/database/tasks/createSampleData/CreateSampleData");
import EditCdcSinkTask = require("components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask");
import EditGenAiTask = require("components/pages/database/tasks/ongoingTasks/editTasks/editGenAiTask/EditGenAiTask");
import ImportDataOptions = require("components/pages/database/tasks/importData/ImportDataOptions");
import ImportDatabaseFromFile = require("components/pages/database/tasks/importData/importFromFile/ImportDatabaseFromFile");
import OngoingTasksPage = require("components/pages/database/tasks/ongoingTasks/OngoingTasksPage");
import AddNewOngoingTask = require("components/pages/database/tasks/ongoingTasks/AddNewOngoingTask");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import TasksErrorsPage = require("components/pages/database/tasks/tasksErrors/TasksErrorsPage");
import footer = require("common/shell/footer")
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getTasksMenuItem;

function getTasksMenuItem(appUrls: computedAppUrls) {
    const tasksItems: menuItem[] = [
        new leafMenuItem({
            route: 'databases/tasks/backups',
            moduleId: reactUtils.bridgeToReact(BackupsPage.BackupsPage, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.backups"),
            nav: true,
            css: 'icon-backups',
            dynamicHash: appUrls.backupsUrl,
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: require('viewmodels/database/tasks/editPeriodicBackupTask'),
            shardingMode: "allShards",
            title: t("tasks.backupTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editPeriodicBackupTask("Backups", true),
            itemRouteToHighlight: 'databases/tasks/backups',
            search: {
                overrideTitle: t("tasks.backupTask.addNew"),
                alternativeTitles: [t("tasks.backupTask.createManual")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editPeriodicBackupTask',
            moduleId: require('viewmodels/database/tasks/editPeriodicBackupTask'),
            shardingMode: "allShards",
            title: t("tasks.backupTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editPeriodicBackupTask("Backups", false),
            itemRouteToHighlight: 'databases/tasks/backups',
            search: {
                overrideTitle: t("tasks.backupTask.addNewPeriodic"),
                alternativeTitles: [t("tasks.backupTask.createPeriodic")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/ongoingTasks',
            moduleId: reactUtils.bridgeToReact(OngoingTasksPage.OngoingTasksPage, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.ongoingTasks.title"),
            nav: true,
            css: 'icon-manage-ongoing-tasks',
            dynamicHash: appUrls.ongoingTasksUrl,
            search: {
                alternativeTitles:[
                    "ETL",
                    t("tasks.ongoingTasks.hub"),
                    t("tasks.ongoingTasks.sink"),
                    t("tasks.ongoingTasks.subscription"),
                    t("tasks.ongoingTasks.replication"),
                ],
                innerActions: [
                    { name: t("tasks.ongoingTasks.enableTask") },
                    { name: t("tasks.ongoingTasks.disableTask") },
                    { name: t("tasks.ongoingTasks.editTask") },
                    { name: t("tasks.ongoingTasks.deleteTask"), alternativeNames: [t("tasks.ongoingTasks.removeTask")] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/addNewOngoingTasks',
            moduleId: reactUtils.bridgeToReact(AddNewOngoingTask.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.addNewOngoingTask"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.addNewOngoingTaskUrl(false),
            itemRouteToHighlight: 'databases/tasks/addNewOngoingTasks',
        }),
        new leafMenuItem({
            route: 'databases/tasks/importOptions',
            moduleId: reactUtils.bridgeToReact(ImportDataOptions.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.importData.title"),
            nav: true,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDataOptionsUrl,
            requiredAccess: "DatabaseReadWrite",
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/file',
            moduleId: reactUtils.bridgeToReact(ImportDatabaseFromFile.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromFileUrl,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            requiredAccess: "DatabaseReadWrite",
            search: {
                overrideTitle: t("tasks.importData.fromFile"),
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import*details',
            moduleId: require('viewmodels/database/tasks/importParent'),
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromFileUrl,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            requiredAccess: "DatabaseReadWrite",
            search: {
                isExcluded: true,
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/migrateRavenDB',
            moduleId: require('viewmodels/database/tasks/migrateRavenDbDatabase'),
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.migrateRavenDbDatabaseUrl,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            search: {
                overrideTitle: t("tasks.importData.fromRavenDbServer"),
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/csv',
            moduleId: require('viewmodels/database/tasks/importCollectionFromCsv'),
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importCollectionFromCsv,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            search: {
                overrideTitle: t("tasks.importData.fromCsv"),
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/import/sql',
            moduleId: require('viewmodels/database/tasks/importDatabaseFromSql'),
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.importDatabaseFromSql,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            search: {
                overrideTitle: t("tasks.importData.fromSql"),
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/migrate',
            moduleId: require('viewmodels/database/tasks/migrateDatabase'),
            title: t("tasks.importData.title"),
            nav: false,
            css: 'icon-import-database',
            dynamicHash: appUrls.migrateDatabaseUrl,
            itemRouteToHighlight: 'databases/tasks/importOptions',
            search: {
                overrideTitle: t("tasks.importData.migrateDatabase"),
            },
        }),
        new leafMenuItem({
            route: 'databases/tasks/exportDatabase',
            moduleId: require('viewmodels/database/tasks/exportDatabase'),
            shardingMode: "allShards",
            title: t("tasks.exportDatabase"),
            nav: true,
            css: 'icon-export-database',
            dynamicHash: appUrls.exportDatabaseUrl
        }),
        new leafMenuItem({
            route: 'databases/tasks/sampleData',
            moduleId: reactUtils.bridgeToReact(CreateSampleData.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.createSampleData"),
            nav: true,
            css: 'icon-create-sample-data',
            dynamicHash: appUrls.sampleDataUrl,
            requiredAccess: "DatabaseReadWrite"
        }),
        new leafMenuItem({
            route: 'databases/tasks/editExternalReplicationTask',
            moduleId: require('viewmodels/database/tasks/editExternalReplicationTask'),
            shardingMode: "allShards",
            title: t("tasks.externalReplicationTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editExternalReplicationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.externalReplicationTask.addNew"),
                alternativeTitles: [t("tasks.externalReplicationTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationHubTask',
            moduleId: require('viewmodels/database/tasks/editReplicationHubTask'),
            shardingMode: "allShards",
            title: t("tasks.replicationHubTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editReplicationHubTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.replicationHubTask.addNew"),
                alternativeTitles: [t("tasks.replicationHubTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editReplicationSinkTask',
            moduleId: require('viewmodels/database/tasks/editReplicationSinkTask'),
            shardingMode: "allShards",
            title: t("tasks.replicationSinkTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editReplicationSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.replicationSinkTask.addNew"),
                alternativeTitles: [t("tasks.replicationSinkTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSubscriptionTask',
            moduleId: require('viewmodels/database/tasks/editSubscriptionTask'),
            shardingMode: "allShards",
            title: t("tasks.subscriptionTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSubscriptionTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.subscriptionTask.addNew"),
                alternativeTitles: [t("tasks.subscriptionTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRavenEtlTask',
            moduleId: require('viewmodels/database/tasks/editRavenEtlTask'),
            shardingMode: "allShards",
            title: t("tasks.ravenEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRavenEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.ravenEtlTask.addNew"),
                alternativeTitles: [t("tasks.ravenEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSqlEtlTask',
            moduleId: require('viewmodels/database/tasks/editSqlEtlTask'),
            shardingMode: "allShards",
            title: t("tasks.sqlEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSqlEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.sqlEtlTask.addNew"),
                alternativeTitles: [t("tasks.sqlEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editSnowflakeEtlTask',
            moduleId: require('viewmodels/database/tasks/editSnowflakeEtlTask'),
            shardingMode: "allShards",
            title: t("tasks.snowflakeEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editSnowflakeEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.snowflakeEtlTask.addNew"),
                alternativeTitles: [t("tasks.snowflakeEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editOlapEtlTask',
            moduleId: require('viewmodels/database/tasks/editOlapEtlTask'),
            shardingMode: "allShards",
            title: t("tasks.olapEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editOlapEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.olapEtlTask.addNew"),
                alternativeTitles: [t("tasks.olapEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editElasticSearchEtlTask',
            moduleId: require('viewmodels/database/tasks/editElasticSearchEtlTask'),
            shardingMode: "allShards",
            title: t("tasks.elasticSearchEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editElasticSearchEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.elasticSearchEtlTask.addNew"),
                alternativeTitles: [t("tasks.elasticSearchEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaEtlTask',
            moduleId: require('viewmodels/database/tasks/editKafkaEtlTask'),
            title: t("tasks.kafkaEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editKafkaEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.kafkaEtlTask.addNew"),
                alternativeTitles: [t("tasks.kafkaEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqEtlTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqEtlTask'),
            title: t("tasks.rabbitMqEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRabbitMqEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.rabbitMqEtlTask.addNew"),
                alternativeTitles: [t("tasks.rabbitMqEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editAzureQueueStorageEtlTask',
            moduleId: require('viewmodels/database/tasks/editAzureQueueStorageEtlTask'),
            title: t("tasks.azureQueueStorageEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAzureQueueStorageEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.azureQueueStorageEtlTask.addNew"),
                alternativeTitles: [t("tasks.azureQueueStorageEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editAmazonSqsEtlTask',
            moduleId: require('viewmodels/database/tasks/editAmazonSqsEtlTask'),
            title: t("tasks.amazonSqsEtlTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAmazonSqsEtlTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.amazonSqsEtlTask.addNew"),
                alternativeTitles: [t("tasks.amazonSqsEtlTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editEmbeddingsGenerationTask',
            moduleId: require('viewmodels/database/tasks/editEmbeddingsGenerationTask'),
            shardingMode: "allShards",
            title: t("tasks.embeddingsGenerationTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editEmbeddingsGenerationTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.embeddingsGenerationTask.addNew"),
                alternativeTitles: [t("tasks.embeddingsGenerationTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editGenAiTask',
            moduleId: reactUtils.bridgeToReact(EditGenAiTask.default, "nonShardedView"),
            title: t("tasks.genAiTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editGenAiTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.genAiTask.addNew"),
                alternativeTitles: [t("tasks.genAiTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editKafkaSinkTask',
            moduleId: require('viewmodels/database/tasks/editKafkaSinkTask'),
            title: t("tasks.kafkaSinkTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editKafkaSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.kafkaSinkTask.addNew"),
                alternativeTitles: [t("tasks.kafkaSinkTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editRabbitMqSinkTask',
            moduleId: require('viewmodels/database/tasks/editRabbitMqSinkTask'),
            title: t("tasks.rabbitMqSinkTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editRabbitMqSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.rabbitMqSinkTask.addNew"),
                alternativeTitles: [t("tasks.rabbitMqSinkTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'databases/tasks/editAzureServiceBusSinkTask',
            moduleId: require('viewmodels/database/tasks/editAzureServiceBusSinkTask'),
            title: t("tasks.azureServiceBusSinkTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editAzureServiceBusSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.azureServiceBusSinkTask.addNew"),
                alternativeTitles: [t("tasks.azureServiceBusSinkTask.create")],
            }
        }),
        new separatorMenuItem(),
        new leafMenuItem({
            route: 'databases/tasks/tasksErrors',
            moduleId: reactUtils.bridgeToReact(TasksErrorsPage.default, "nonShardedView"),
            shardingMode: "allShards",
            title: t("tasks.taskErrors"),
            nav: true,
            css: 'icon-tasks-errors',
            dynamicHash: appUrls.tasksError,
            requiredAccess: "DatabaseReadWrite",
            badgeData: ko.pureComputed(() => { 
                return footer.default.stats() ? footer.default.stats().countOfEtlTasksErrors() + footer.default.stats().countOfAiTasksErrors() + footer.default.stats().countOfCdcSinkTasksErrors() : null;
            })
        }),
        new leafMenuItem({
            route: 'databases/tasks/editCdcSinkTask',
            moduleId: reactUtils.bridgeToReact(EditCdcSinkTask.default, "nonShardedView"),
            title: t("tasks.cdcSinkTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrls.editCdcSinkTaskUrl,
            itemRouteToHighlight: 'databases/tasks/ongoingTasks',
            search: {
                overrideTitle: t("tasks.cdcSinkTask.addNew"),
                alternativeTitles: [t("tasks.cdcSinkTask.create")],
            }
        }),
    ];

    return new intermediateMenuItem(t("tasks.title"), tasksItems, 'icon-tasks-menu');
}
