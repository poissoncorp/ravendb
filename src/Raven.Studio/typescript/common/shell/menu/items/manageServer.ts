import appUrl = require("common/appUrl");
import reactUtils = require("common/reactUtils");
import intermediateMenuItem = require("common/shell/menu/intermediateMenuItem");
import leafMenuItem = require("common/shell/menu/leafMenuItem");
import separatorMenuItem = require("common/shell/menu/separatorMenuItem");
import AdminJsConsole = require("components/pages/resources/manageServer/adminJsConsole/AdminJsConsole");
import ClientGlobalConfiguration = require("components/pages/resources/manageServer/clientConfiguration/ClientGlobalConfiguration");
import StudioGlobalConfiguration = require("components/pages/resources/manageServer/studioConfiguration/StudioGlobalConfiguration");
import GatherDebugInfo = require("components/pages/resources/manageServer/gatherDebugInfo/GatherDebugInfo");
import DebugPackage = require("components/pages/resources/manageServer/debugPackageAnalyzer/DebugPackage");
import ServerWideCustomAnalyzers = require("components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzers");
import ServerWideCustomSorters = require("components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSorters");
import ServerSettings = require("components/pages/resources/manageServer/serverSettings/ServerSettings");
import AdminLogs = require("components/pages/resources/manageServer/adminLogs/AdminLogs");
import Certificates = require("components/pages/resources/manageServer/certificates/Certificates");
import ServerWideConnectionStrings = require("components/pages/resources/manageServer/serverWideConnectionStrings/ServerWideConnectionStrings");
import ServerWideTasks = require("components/pages/resources/manageServer/serverWideTasks/ServerWideTasks");
import AddServerWideTask = require("components/pages/resources/manageServer/serverWideTasks/AddServerWideTask");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("menu");

export = getManageServerMenuItem;

function getManageServerMenuItem() {
    const items: menuItem[] = [
        new leafMenuItem({
            route: 'admin/settings/cluster',
            moduleId: require("viewmodels/manage/cluster"),
            title: t("manageServer.cluster.title"),
            nav: true,
            css: 'icon-cluster',
            dynamicHash: appUrl.forCluster,
            search: {
                alternativeTitles: [t("manageServer.cluster.clusterTopology")],
                innerActions: [
                    { name: t("manageServer.cluster.deleteNode"), alternativeNames: [t("manageServer.cluster.removeNode")] },
                    { name: t("manageServer.cluster.reassignCores") },
                    { name: t("manageServer.cluster.forceTimeout") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/addClusterNode',
            moduleId: require("viewmodels/manage/addClusterNode"),
            title: t("manageServer.addClusterNode.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forAddClusterNode,
            itemRouteToHighlight: 'admin/settings/cluster',
            search: {
                overrideTitle: t("manageServer.addClusterNode.addNew"),
                alternativeTitles: [t("manageServer.addClusterNode.create")],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/clientConfiguration',
            moduleId: reactUtils.bridgeToReact(ClientGlobalConfiguration.default, "nonShardedView"),
            title: t("manageServer.clientConfiguration.title"),
            nav: true,
            css: 'icon-client-configuration',
            dynamicHash: appUrl.forGlobalClientConfiguration,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.clientConfiguration.identityPartsSeparator") },
                    { name: t("manageServer.clientConfiguration.maxNumberOfRequestsPerSession") },
                    { name: t("manageServer.clientConfiguration.loadBalanceBehavior") },
                    { name: t("manageServer.clientConfiguration.seed") },
                    { name: t("manageServer.clientConfiguration.readBalanceBehavior") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/studioConfiguration',
            moduleId: reactUtils.bridgeToReact(StudioGlobalConfiguration.default, "nonShardedView"),
            title: t("manageServer.studioConfiguration.title"),
            nav: true,
            css: 'icon-studio-configuration',
            dynamicHash: appUrl.forGlobalStudioConfiguration,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.studioConfiguration.serverEnvironment") },
                    { name: t("manageServer.studioConfiguration.defaultReplicationFactor") },
                    { name: t("manageServer.studioConfiguration.collapseDocuments") },
                    { name: t("manageServer.studioConfiguration.gatherUsageStatistics") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverSettings',
            moduleId: reactUtils.bridgeToReact(ServerSettings.default, "nonShardedView"),
            title: t("manageServer.serverSettings"),
            nav: true,
            css: 'icon-server-settings',
            dynamicHash: appUrl.forServerSettings,
            requiredAccess: "ClusterAdmin"
        }),
        new leafMenuItem({
            route: 'admin/settings/adminJsConsole',
            moduleId: reactUtils.bridgeToReact(AdminJsConsole.default, "nonShardedView"),
            title: t("manageServer.adminJsConsole"),
            nav: true,
            css: 'icon-administrator-js-console',
            dynamicHash: appUrl.forAdminJsConsole,
            requiredAccess: "ClusterAdmin"
        }),
        new leafMenuItem({
            route: 'admin/settings/certificates',
            moduleId: reactUtils.bridgeToReact(Certificates.default, "nonShardedView"),
            title: t("manageServer.userAccessManagement.title"),
            nav: true,
            css: 'icon-certificate',
            dynamicHash: appUrl.forCertificates,
            requiredAccess: "Operator",
            search: {
                alternativeTitles: [t("manageServer.userAccessManagement.certificates")],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideTasks',
            moduleId: reactUtils.bridgeToReact(ServerWideTasks.default, "nonShardedView"),
            title: t("manageServer.serverWideTasks.title"),
            nav: true,
            css: 'icon-server-wide-tasks',
            dynamicHash: appUrl.forServerWideTasks,
            requiredAccess: "ClusterAdmin",
            search: {
                innerActions: [
                    { name: t("manageServer.serverWideTasks.externalReplication") },
                    { name: t("manageServer.serverWideTasks.periodicBackup") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideTasks/add',
            moduleId: reactUtils.bridgeToReact(AddServerWideTask.default, "nonShardedView"),
            title: t("manageServer.addServerWideTask.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forAddServerWideTask,
            itemRouteToHighlight: 'admin/settings/serverWideTasks',
            requiredAccess: "ClusterAdmin",
            search: {
                overrideTitle: t("manageServer.addServerWideTask.addNew"),
                alternativeTitles: [t("manageServer.addServerWideTask.create")],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/editServerWideBackup',
            moduleId: require("viewmodels/manage/editServerWideBackup"),
            title: t("manageServer.editServerWideBackup.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forEditServerWideBackup,
            itemRouteToHighlight: 'admin/settings/serverWideTasks',
            requiredAccess: "ClusterAdmin",
            search: {
                overrideTitle: t("manageServer.editServerWideBackup.addNew"),
                alternativeTitles: [t("manageServer.editServerWideBackup.create")],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/editServerWideExternalReplication',
            moduleId: require("viewmodels/manage/editServerWideExternalReplication"),
            title: t("manageServer.editServerWideExternalReplication.title"),
            nav: false,
            css: "icon-plus",
            dynamicHash: appUrl.forEditServerWideExternalReplication,
            itemRouteToHighlight: 'admin/settings/serverWideTasks',
            requiredAccess: "ClusterAdmin",
            search: {
                overrideTitle: t("manageServer.editServerWideExternalReplication.addNew"),
                alternativeTitles: [t("manageServer.editServerWideExternalReplication.create")],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideCustomAnalyzers',
            moduleId: reactUtils.bridgeToReact(ServerWideCustomAnalyzers.default, "nonShardedView"),
            title: t("manageServer.serverWideAnalyzers.title"),
            nav: true,
            css: 'icon-server-wide-custom-analyzers',
            dynamicHash: appUrl.forServerWideCustomAnalyzers,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.serverWideAnalyzers.addNew"), alternativeNames: [t("manageServer.serverWideAnalyzers.create")] },
                    { name: t("manageServer.serverWideAnalyzers.edit") },
                    { name: t("manageServer.serverWideAnalyzers.delete"), alternativeNames: [t("manageServer.serverWideAnalyzers.remove")] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideCustomSorters',
            moduleId: reactUtils.bridgeToReact(ServerWideCustomSorters.default, "nonShardedView"),
            title: t("manageServer.serverWideSorters.title"),
            nav: true,
            css: 'icon-server-wide-custom-sorters',
            dynamicHash: appUrl.forServerWideCustomSorters,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.serverWideSorters.addNew"), alternativeNames: [t("manageServer.serverWideSorters.create")] },
                    { name: t("manageServer.serverWideSorters.edit") },
                    { name: t("manageServer.serverWideSorters.delete"), alternativeNames: [t("manageServer.serverWideSorters.remove")] },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/serverWideConnectionStrings',
            moduleId: reactUtils.bridgeToReact(ServerWideConnectionStrings.default, "nonShardedView"),
            title: t("manageServer.serverWideConnectionStrings.title"),
            nav: true,
            css: 'icon-manage-connection-strings',
            dynamicHash: appUrl.forServerWideConnectionStrings,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.serverWideConnectionStrings.addNew"), alternativeNames: [t("manageServer.serverWideConnectionStrings.create")] },
                    { name: t("manageServer.serverWideConnectionStrings.edit") },
                    { name: t("manageServer.serverWideConnectionStrings.delete"), alternativeNames: [t("manageServer.serverWideConnectionStrings.remove")] },
                ],
            },
        }),
        new separatorMenuItem(),
        new separatorMenuItem(t("manageServer.debug")),
        new leafMenuItem({
            route: 'admin/settings/adminLogs',
            moduleId: reactUtils.bridgeToReact(AdminLogs.default, "nonShardedView"),
            title: t("manageServer.adminLogs.title"),
            nav: true,
            css: 'icon-admin-logs',
            dynamicHash: appUrl.forAdminLogs,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.adminLogs.exportLogs") },
                    { name: t("manageServer.adminLogs.downloadLogs") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/trafficWatch',
            moduleId: require("viewmodels/manage/trafficWatch"),
            title: t("manageServer.trafficWatch.title"),
            nav: true,
            css: 'icon-traffic-watch',
            dynamicHash: appUrl.forTrafficWatch,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.trafficWatch.exportLogs") }
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/debugPackage',
            moduleId: reactUtils.bridgeToReact(GatherDebugInfo.default, "nonShardedView"),
            title: t("manageServer.debugPackage.title"),
            nav: true,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugPackage,
            requiredAccess: "Operator",
            search: {
                alternativeTitles: [t("manageServer.debugPackage.create"), t("manageServer.debugPackage.gatherDebugInfo")],
                innerActions: [
                    { name: t("manageServer.debugPackage.create") },
                    { name: t("manageServer.debugPackage.download") },
                ],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debugPackageAnalyzer',
            moduleId: reactUtils.bridgeToReact(DebugPackage.default, "nonShardedView"),
            title: t("manageServer.debugPackageAnalyzer.title"),
            nav: false,
            css: 'icon-gather-debug-information',
            dynamicHash: appUrl.forDebugPackageAnalyzer,
            itemRouteToHighlight: 'admin/settings/debugPackage',
            requiredAccess: "Operator",
            search: {
                alternativeTitles: [t("manageServer.debugPackageAnalyzer.analyze")],
                innerActions: [
                    { name: t("manageServer.debugPackageAnalyzer.analyze") },
                    { name: t("manageServer.debugPackageAnalyzer.upload") },
                ],
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/storageReport',
            moduleId: require('viewmodels/manage/storageReport'),
            title: t("manageServer.storageReport"),
            tooltip: t("manageServer.storageReport"),
            nav: true,
            css: 'icon-system-storage',
            dynamicHash: appUrl.forSystemStorageReport,
            requiredAccess: "Operator"
        }),
        new leafMenuItem({
            route: 'admin/settings/ioStats',
            moduleId: require('viewmodels/manage/serverWideIoStats'),
            title: t("manageServer.ioStats.title"),
            tooltip: t("manageServer.ioStats.tooltip"),
            nav: true,
            css: 'icon-manage-server-io-test',
            dynamicHash: appUrl.forSystemIoStats,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.ioStats.export") },
                    { name: t("manageServer.ioStats.import") },
                ],
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/captureStackTraces',
            moduleId: require('viewmodels/manage/captureStackTraces'),
            title: t("manageServer.stackTraces.title"),
            nav: true,
            css: 'icon-stack-traces', 
            dynamicHash: appUrl.forCaptureStackTraces,
            requiredAccess: "Operator",
            search: {
                innerActions: [
                    { name: t("manageServer.stackTraces.capture") },
                    { name: t("manageServer.stackTraces.export") },
                    { name: t("manageServer.stackTraces.import") },
                ]
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/runningQueries',
            moduleId: require('viewmodels/manage/runningQueries'),
            title: t("manageServer.runningQueries"),
            nav: true,
            css: 'icon-manage-server-running-queries',
            dynamicHash: appUrl.forRunningQueries
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced*details',
            moduleId: require('viewmodels/manage/debugAdvancedParent'),
            title: t("manageServer.advanced.title"),
            nav: true,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedThreadsRuntime,
            requiredAccess: "Operator",
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/threadsRuntime',
            moduleId: require('viewmodels/manage/debugAdvancedThreadsRuntime'),
            title: t("manageServer.advanced.title"),
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedThreadsRuntime,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: t("manageServer.advanced.threadsRuntime"),
            },
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/memoryMappedFiles',
            moduleId: require('viewmodels/manage/debugAdvancedMemoryMappedFiles'),
            title: t("manageServer.advanced.title"),
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedMemoryMappedFiles,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: t("manageServer.advanced.memoryMappedFiles"),
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/observerLog',
            moduleId: require('viewmodels/manage/debugAdvancedObserverLog'),
            title: t("manageServer.advanced.title"),
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedObserverLog,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: t("manageServer.advanced.observerLog"),
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/recordTransactionCommands',
            moduleId: require('viewmodels/manage/debugAdvancedRecordTransactionCommands'),
            title: t("manageServer.advanced.title"),
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedRecordTransactionCommands,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: t("manageServer.advanced.recordTransactionCommands"),
            }
        }),
        new leafMenuItem({
            route: 'admin/settings/debug/advanced/replayTransactionCommands',
            moduleId: require('viewmodels/manage/debugAdvancedReplayTransactionCommands'),
            title: t("manageServer.advanced.title"),
            nav: false,
            css: 'icon-debug-advanced',
            dynamicHash: appUrl.forDebugAdvancedReplayTransactionCommands,
            itemRouteToHighlight: 'admin/settings/debug/advanced*details',
            requiredAccess: "Operator",
            search: {
                overrideTitle: t("manageServer.advanced.replayTransactionCommands"),
            }
        }),
    ];

    return new intermediateMenuItem(t("manageServer.title"), items, 'icon-manage-server', null);
}
