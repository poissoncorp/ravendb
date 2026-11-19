import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterDashboard");

interface widgetToAdd {
    name: string;
    icon: "welcome-icon" | "text-icon" | "graph-icon" | "list-icon" | "chart-icon";
    type: widgetType;
}

class addWidgetModal extends dialogViewModelBase {

    view = require("views/resources/addWidgetModal.html");
    
    private readonly existingTypes: widgetType[];
    private readonly onWidgetSelected: (type: widgetType) => void;
    
    widgets: widgetToAdd[] = [
        {
            name: t("addWidget.widgets.welcome"),
            icon: require("../../../wwwroot/Content/img/widgets/welcome-icon.svg"),
            type: "Welcome"
        }, {
            name: t("addWidget.widgets.license"),
            icon: require("../../../wwwroot/Content/img/widgets/text-icon.svg"),
            type: "License"
        }, {
            name: t("addWidget.widgets.clusterOverview"),
            icon: require("../../../wwwroot/Content/img/widgets/chart-icon.svg"),
            type: "ClusterOverview"            
        }, {
            name: t("addWidget.widgets.storage"),
            icon: require("../../../wwwroot/Content/img/widgets/chart-icon.svg"),
            type: "StorageUsage"
        }, {
            name: t("addWidget.widgets.cpu"),
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "CpuUsage"
        }, {
            name: t("addWidget.widgets.memory"),
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "MemoryUsage"
        }, {
            name: t("addWidget.widgets.indexing"),
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "Indexing"
        }, {
            name: t("addWidget.widgets.ioStats"),
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "IoStats"
        }, {
            name: t("addWidget.widgets.traffic"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "Traffic"
        }, {
            name: t("addWidget.widgets.databaseIndexing"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseIndexing"
        }, {
            name: t("addWidget.widgets.databaseTraffic"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseTraffic"
        }, {
            name: t("addWidget.widgets.databaseStorage"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseStorageUsage"
        }, {
            name: t("addWidget.widgets.databaseOverview"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabaseOverview"
        }, {
            name: t("addWidget.widgets.ongoingTasks"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "OngoingTasks"
        }, {
            name: t("addWidget.widgets.gc"),
            icon: require("../../../wwwroot/Content/img/widgets/graph-icon.svg"),
            type: "GcInfo"
        }, {
            name: t("addWidget.widgets.databaseNotifications"),
            icon: require("../../../wwwroot/Content/img/widgets/list-icon.svg"),
            type: "DatabasesNotifications"
        }
    ]
    
    constructor(
        existingTypes: widgetType[], 
        onWidgetSelected: (type: widgetType) => void
    ) {
        super();
        
        this.existingTypes = existingTypes;
        this.onWidgetSelected = onWidgetSelected;
        
        this.bindToCurrentInstance("addWidget");
    }
    
    addWidget(type: widgetType) {
        if (!this.canAddWidget(type)) {
            return;
        }
        this.onWidgetSelected(type);
        this.close();
    }

    addAllWidgets() {
        this.widgets.forEach(x => {
           if (this.canAddWidget(x.type)) {
               this.onWidgetSelected(x.type);
           } 
        });
        this.close();
    }
    
    compositionComplete(view?: any, parent?: any) {
        super.compositionComplete(view, parent);
        
        $(".widget-item.disabled").tooltip({
            title: t("addWidget.alreadyAdded")
        });
    }

    canAddWidget(type: widgetType) {
        return !_.includes(this.existingTypes, type);
    }
}

export = addWidgetModal;
