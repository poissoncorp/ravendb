import textColumn = require("widgets/virtualGrid/columns/textColumn");
import indexingSpeedItem = require("models/resources/widgets/indexingSpeedItem");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import widget = require("viewmodels/resources/widgets/widget");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterDashboard");

class databaseIndexingWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload, perNodeStatItems<indexingSpeedItem>, indexingSpeedItem> {

    view = require("views/resources/widgets/databaseIndexingWidget.html");
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseIndexing";
    }
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<indexingSpeedItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected createNoDataItem(nodeTag: string, databaseName: string): indexingSpeedItem {
        return indexingSpeedItem.noData(nodeTag, databaseName);
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseIndexingSpeedPayload): indexingSpeedItem[] {
        return data.Items.map(x => new indexingSpeedItem(nodeTag, x));
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<indexingSpeedItem>(grid, x => x.hideDatabaseName && !grid.sortEnabled() ? "" : DatabaseUtils.default.formatName(x.database), t("widget.databaseColumn"), "35%"),
            new nodeTagColumn<indexingSpeedItem>(grid, item => this.prepareUrl(item, t("databaseIndexing.targetDescription"))),
            new textColumn<indexingSpeedItem>(grid, x => x.indexedPerSecond, t("databaseIndexing.columns.indexedPerSecond"), "15%", {
                headerTitle: t("databaseIndexing.columns.indexedPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<indexingSpeedItem>(grid, x => x.mappedPerSecond, t("databaseIndexing.columns.mappedPerSecond"), "15%", {
                headerTitle: t("databaseIndexing.columns.mappedPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<indexingSpeedItem>(grid, x => x.reducedPerSecond, t("databaseIndexing.columns.reducedPerSecond"), "15%", {
                headerTitle: t("databaseIndexing.columns.reducedPerSecondTitle"),
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            })
        ];
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forIndexPerformance(database);
    }
}


export = databaseIndexingWidget;
