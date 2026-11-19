import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import trafficWatchItem = require("models/resources/widgets/trafficWatchItem");
import generalUtils = require("common/generalUtils");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import widget = require("viewmodels/resources/widgets/widget");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterDashboard");

class databaseTrafficWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload, 
    perNodeStatItems<trafficWatchItem>, trafficWatchItem> {

    view = require("views/resources/widgets/databaseTrafficWidget.html");
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseTraffic";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<trafficWatchItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected createNoDataItem(nodeTag: string, databaseName: string): trafficWatchItem {
        return trafficWatchItem.noData(nodeTag, databaseName);
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload): trafficWatchItem[] {
        return data.Items.map(x => new trafficWatchItem(nodeTag, x));
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<trafficWatchItem>(grid, x => x.hideDatabaseName && !grid.sortEnabled() ? "" : DatabaseUtils.default.formatName(x.database), t("widget.databaseColumn"), "30%"),
            new nodeTagColumn<trafficWatchItem>(grid, item => this.prepareUrl(item, t("databaseTraffic.targetDescription"))),
            new textColumn<trafficWatchItem>(grid, x => x.requestsPerSecond, t("databaseTraffic.columns.requestsPerSecond"), "12%", {
                headerTitle: t("databaseTraffic.columns.requestsPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<trafficWatchItem>(grid, x => x.writesPerSecond, t("databaseTraffic.columns.writesPerSecond"), "12%", {
                headerTitle: t("databaseTraffic.columns.writesPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<trafficWatchItem>(grid, x => x.noData ? -1 : x.dataWritesPerSecond, t("databaseTraffic.columns.dataWrittenPerSecond"), "12%", {
                headerTitle: t("databaseTraffic.columns.dataWrittenPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => x === -1 ? "-" : generalUtils.formatBytesToSize(x)

            }),
            new textColumn<trafficWatchItem>(grid, x => x.noData ? -1 : t("databaseTraffic.columns.avgRequestTimeValue", { duration: Math.round(x.averageDuration) }), t("databaseTraffic.columns.avgRequestTime"), "12%", {
                headerTitle: t("databaseTraffic.columns.avgRequestTimeTitle"),
                sortable: "number",
                transformValue: (x: number) => x === -1 ? "-" : x.toLocaleString()
            }),
        ];
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forTrafficWatch(database);
    }
}

export = databaseTrafficWidget;
