import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import databaseDiskUsage = require("models/resources/widgets/databaseDiskUsage");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterDashboard");

class databaseStorageWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload, perNodeStatItems<databaseDiskUsage>, databaseDiskUsage> {

    view = require("views/resources/widgets/databaseStorageWidget.html");
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseStorageUsage";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<databaseDiskUsage>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected createNoDataItem(nodeTag: string, databaseName: string): databaseDiskUsage {
        return databaseDiskUsage.noData(nodeTag, databaseName);
    }
    
    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload): databaseDiskUsage[] {
        return data.Items.map(x => new databaseDiskUsage(nodeTag, x));
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<databaseDiskUsage>(grid, x => x.hideDatabaseName && !grid.sortEnabled() ? "" : DatabaseUtils.default.formatName(x.database), t("widget.databaseColumn"), "35%"),
            new nodeTagColumn<databaseDiskUsage>(grid, item => this.prepareUrl(item, t("databaseStorage.targetDescription"))),
            new textColumn<databaseDiskUsage>(grid, x => x.noData ? "-" : generalUtils.formatBytesToSize(x.size), t("databaseStorage.columns.data"), "15%", {
                headerTitle: t("databaseStorage.columns.dataTitle"),
                sortable: x => x.noData ? -1 : x.size,
            }),
            new textColumn<databaseDiskUsage>(grid, x => x.noData ? "-" : generalUtils.formatBytesToSize(x.tempBuffersSize), t("databaseStorage.columns.temp"), "15%", {
                headerTitle: t("databaseStorage.columns.tempTitle"),
                sortable: x => x.noData ? -1 : x.tempBuffersSize,
            }),
            new textColumn<databaseDiskUsage>(grid, x => x.noData ? "-" : generalUtils.formatBytesToSize(x.total), t("databaseStorage.columns.total"), "15%", {
                headerTitle: t("databaseStorage.columns.totalTitle"),
                sortable: x => x.noData ? -1 : x.total,
            }),
        ];
    }

    protected generateLocalLink(database: string): string {
        return database === "<System>" ? appUrl.forSystemStorageReport() : appUrl.forStatusStorageReport(database);
    }
}

export = databaseStorageWidget;
