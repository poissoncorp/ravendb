/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import moment = require("moment");
import groupedVirtualNotification = require("common/notifications/models/groupedVirtualNotification");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("notifications");

class virtualUpdateByQuery extends groupedVirtualNotification<queryBasedVirtualBulkOperationItem> {

    static readonly Id = "virtual$$updateByQuery";

    constructor(db: database) {
        super(db, {
            Id: virtualUpdateByQuery.Id,
            IsPersistent: false,
            Type: "CumulativeUpdateByQuery",
            Database: db.name,

            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });

        this.title(t("updateByQuery.title"));
        this.severity("Success");
    }

    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);

        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);

        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.BulkOperationResult;

        const item = {
            id: dto.Id,
            operationId: dto.OperationId,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            totalItemsProcessed: bulkResult.Total,
            indexOrCollectionUsed: dto.Message,
            query: (dto.DetailedDescription as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query
        } as queryBasedVirtualBulkOperationItem;

        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }

        const operationsCount = this.operations().length;
        this.message(
            t("updateByQuery.completed", { count: operationsCount, formattedCount: operationsCount.toLocaleString() })
        );
    }
}

export = virtualUpdateByQuery;
