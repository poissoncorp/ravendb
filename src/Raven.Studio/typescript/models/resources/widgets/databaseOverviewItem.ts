
import moment = require("moment");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("widgetsModels");

class databaseOverviewItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    relevant: boolean;
    disabled: boolean;
    online: boolean;
    documents: number;
    alerts: number;
    performanceHints: number;
    indexes: number;
    erroredIndexes: number;
    indexingErrors: number;
    ongoingTasks: number;
    backupInfo: Raven.Client.ServerWide.Operations.BackupInfo;
    
    noData: boolean;
    
    hideDatabaseName: boolean;
    even = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.DatabaseInfoItem) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.relevant = !data.Irrelevant;
            this.disabled = data.Disabled;
            this.online = data.Online
            this.documents = data.DocumentsCount;
            this.alerts = data.AlertsCount;
            this.performanceHints = data.PerformanceHintsCount;
            this.indexes = data.IndexesCount;
            this.erroredIndexes = data.ErroredIndexesCount;
            this.indexingErrors = data.IndexingErrorsCount;
            this.ongoingTasks = data.OngoingTasksCount;
            this.backupInfo = data.BackupInfo;
        } else {
            this.noData = true;
        }
    }

    static noData(nodeTag: string, database: string): databaseOverviewItem {
        const item = new databaseOverviewItem(nodeTag, null);
        item.database = database;
        return item;
    }

    static commonData(item: databaseOverviewItem) {
        const commonItem = new databaseOverviewItem(null, null);
        commonItem.relevant = true; 
        
        commonItem.database = item.database;
        commonItem.documents = item.documents;
        commonItem.indexes = item.indexes;
        commonItem.ongoingTasks = item.ongoingTasks;
        commonItem.backupInfo = item.backupInfo;
        commonItem.disabled = item.disabled;
        
        return commonItem;
    }

    erroredIndexesDataForHtml(): iconPlusText[] {
        if (!this.erroredIndexes) {
            return [];
        }

        const textValue = this.erroredIndexes.toLocaleString();

        return [{
            title: t("databaseOverview.erroredIndexes", { value: textValue, count: this.erroredIndexes }),
            text: textValue,
            iconClass: "icon-danger",
            textClass: "text-danger"
        }];
    }
    
    indexingErrorsDataForHtml(): iconPlusText[] {
        if (!this.indexingErrors) {
            return [];
        }
        
        const textValue = this.indexingErrors.toLocaleString();
        
        return [{
            title: t("databaseOverview.indexingErrors", { value: textValue, count: this.indexingErrors }),
            text: textValue,
            iconClass: "icon-danger",
            textClass: "text-danger"
        }];
    }

    alertsDataForHtml(): iconPlusText[] {
        if (!this.alerts && !this.performanceHints) {
            return [];
        }
        
        const alertsData: iconPlusText[] = [];
        
        if (this.alerts) {
            const textValue = this.alerts.toLocaleString();
            alertsData.push({
                title: t("databaseOverview.alerts", { value: textValue, count: this.alerts }),
                text: textValue,
                iconClass: "icon-warning",
                textClass: "text-warning"
            });
        }
       
        if (this.performanceHints) {
            const textValue = this.performanceHints.toLocaleString();
            alertsData.push({
                title: t("databaseOverview.performanceHints", { value: textValue, count: this.performanceHints }),
                text: textValue,
                iconClass: "icon-info",
                textClass: "text-info"
            });
        }
        
        return alertsData;
    }
    
    backupDataForHtml(): iconPlusText[] {
        if (!this.backupInfo) {
            return [{
               title: t("databaseOverview.noBackupTask"),
               text: "",
               iconClass: "icon-cancel",
               textClass: "text-danger" 
            }];
        } 
        
        if (!this.backupInfo.LastBackup) {
            return [{
                title: t("databaseOverview.neverBackedUp"),
                text: "",
                iconClass: "icon-warning",
                textClass: "text-warning"
            }];
        }
        
        return [{
            title: t("databaseOverview.backupCreated"),
            text: generalUtils.formatDurationByDate(moment.utc(this.backupInfo.LastBackup), true),
            iconClass: "icon-check",
            textClass: "text-success"
        }];
    }

    stateDataForHtml(nodeTag: string): iconPlusText[] {
        if (this.disabled) {
            if (!nodeTag) {
                return [{
                    text: t("databaseOverview.disabled"),
                    iconClass: "icon-database-cutout icon-addon-cancel",
                    textClass: "text-danger"
                }];
            }
        } else if (nodeTag) {
            if (!this.online) {
                return [{
                    text: t("databaseOverview.offline"),
                    iconClass: "icon-database-cutout icon-addon-clock",
                    textClass: null
                }];
           } else {
                return [{
                    text: t("databaseOverview.online"),
                    iconClass: "icon-database-cutout icon-addon-check",
                    textClass: "text-success"
                }];
            }
        }

        return [];
    }
}

export = databaseOverviewItem;
