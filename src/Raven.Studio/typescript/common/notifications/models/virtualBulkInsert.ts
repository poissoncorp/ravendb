/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import moment = require("moment");
import groupedVirtualNotification = require("common/notifications/models/groupedVirtualNotification");
import typeUtils = require("common/typeUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("notifications");

class virtualBulkInsert extends groupedVirtualNotification<virtualBulkOperationItem> {
    
    static readonly Id = "virtual$$bulkInsert";

    constructor(db: database) {
        super(db, {
            Id: virtualBulkInsert.Id,
            IsPersistent: false,
            Type: "CumulativeBulkInsert",
            Database: db.name,
            
            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });
        
        this.title(t("bulkInsert.title"));
        this.severity("Success");
    }
    
    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);
        
        const progress = dto.State.Progress as Raven.Client.Documents.Operations.BulkInsertProgress;
        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.BulkOperationResult;
        const bulkInsertInfo = bulkResult || progress;
        
        const item: virtualBulkOperationItem = {
            id: dto.Id,
            operationId: dto.OperationId,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            totalItemsProcessed: bulkInsertInfo.Total,
            documentsProcessed: bulkInsertInfo.DocumentsProcessed,
            attachmentsProcessed: bulkInsertInfo.AttachmentsProcessed,
            countersProcessed: bulkInsertInfo.CountersProcessed,
            timeSeriesProcessed: bulkInsertInfo.TimeSeriesProcessed,
        };
        
        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);
        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }
        
        const totalItemsCount = typeUtils.sumBy(this.operations(), x => x.totalItemsProcessed);
        const operationsCount = this.operations().length;
        this.message(
            t("bulkInsert.completed", {
                count: operationsCount,
                formattedCount: operationsCount.toLocaleString(),
                databaseName: this.databaseName,
            }) +
                " " +
                t("bulkInsert.itemsInserted", {
                    count: totalItemsCount,
                    formattedCount: totalItemsCount.toLocaleString(),
                })
        );
    }
}

export = virtualBulkInsert;
