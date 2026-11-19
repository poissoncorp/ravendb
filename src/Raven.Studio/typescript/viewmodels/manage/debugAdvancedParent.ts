import appUrl = require("common/appUrl");
import durandalRouter = require("plugins/router");
import reactUtils = require("common/reactUtils");
import ClusterDebug = require("components/pages/resources/manageServer/advanced/clusterDebug/ClusterDebug");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugAdvancedParent");

class debugAdvanced {
    
    view = require("views/manage/debugAdvancedParent.html");
    
    getView() {
        return this.view;
    }
    
    router: DurandalRootRouter;
    
    growContainer = ko.observable<boolean>(true);

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'admin/settings/debug/advanced/threadsRuntime',
                    moduleId: require('viewmodels/manage/debugAdvancedThreadsRuntime'),
                    title: t("tabs.threadsRuntime"),
                    tabName: t("tabs.threadsRuntime"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedThreadsRuntime(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/memoryMappedFiles',
                    moduleId: require('viewmodels/manage/debugAdvancedMemoryMappedFiles'),
                    title: t("tabs.memoryMappedFiles"),
                    tabName: t("tabs.memoryMappedFiles"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedMemoryMappedFiles(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/observerLog',
                    moduleId: require('viewmodels/manage/debugAdvancedObserverLog'),
                    title: t("tabs.observerLog"),
                    tabName: t("tabs.observerLog"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedObserverLog(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/clusterDebug',
                    // forcing bootstrap 3 as this is child router
                    moduleId: reactUtils.bridgeToReact(ClusterDebug.default, "nonShardedView", false),
                    title: t("tabs.clusterDebug"),
                    tabName: t("tabs.clusterDebug"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedClusterDebug(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/recordTransactionCommands',
                    moduleId: require('viewmodels/manage/debugAdvancedRecordTransactionCommands'),
                    title: t("tabs.recordTransactionCommands"),
                    tabName: t("tabs.recordTransactionCommands"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedRecordTransactionCommands(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/replayTransactionCommands',
                    moduleId: require('viewmodels/manage/debugAdvancedReplayTransactionCommands'),
                    title: t("tabs.replayTransactionCommands"),
                    tabName: t("tabs.replayTransactionCommands"),
                    nav: true,
                    hash: appUrl.forDebugAdvancedReplayTransactionCommands(),
                    requiredAccess: "Operator"
                }
            ])
            .buildNavigationModel();
    }
}

export = debugAdvanced; 
