import viewModelBase = require("viewmodels/viewModelBase");
import appUrl = require("common/appUrl");
import durandalRouter = require("plugins/router");
import accessManager = require("common/shell/accessManager");

class debugAdvanced {
    router: DurandalRootRouter;
    
    growContainer = ko.observable<boolean>(true);

    constructor() {
        this.router = durandalRouter.createChildRouter()
            .map([
                {
                    route: 'admin/settings/debug/advanced/threadsRuntime',
                    moduleId: require('viewmodels/manage/debugAdvancedThreadsRuntime'),
                    title: 'Threads Runtime Info',
                    tabName: "Threads Runtime Info",
                    nav: true,
                    hash: appUrl.forDebugAdvancedThreadsRuntime(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/memoryMappedFiles',
                    moduleId: require('viewmodels/manage/debugAdvancedMemoryMappedFiles'),
                    title: 'Memory Mapped Files',
                    tabName: "Memory Mapped Files",
                    nav: true,
                    hash: appUrl.forDebugAdvancedMemoryMappedFiles(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/observerLog',
                    moduleId: require('viewmodels/manage/debugAdvancedObserverLog'),
                    title: 'Cluster Observer Log',
                    tabName: "Cluster Observer Log",
                    nav: true,
                    hash: appUrl.forDebugAdvancedObserverLog(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/recordTransactionCommands',
                    moduleId: require('viewmodels/manage/debugAdvancedRecordTransactionCommands'),
                    title: 'Record Transaction Commands',
                    tabName: "Record Transaction Commands",
                    nav: true,
                    hash: appUrl.forDebugAdvancedRecordTransactionCommands(),
                    requiredAccess: "Operator"
                },
                {
                    route: 'admin/settings/debug/advanced/replayTransactionCommands',
                    moduleId: require('viewmodels/manage/debugAdvancedReplayTransactionCommands'),
                    title: 'Replay Transaction Commands',
                    tabName: "Replay Transaction Commands",
                    nav: true,
                    hash: appUrl.forDebugAdvancedReplayTransactionCommands(),
                    requiredAccess: "Operator"
                }
            ])
            .buildNavigationModel();

        this.router.on("router:navigation:attached", (viewModel: viewModelBase) => {
            const preventGrow = !!(viewModel.constructor as any).preventParentGrow;
            this.growContainer(!preventGrow);
        });
    }
}

export = debugAdvanced; 
