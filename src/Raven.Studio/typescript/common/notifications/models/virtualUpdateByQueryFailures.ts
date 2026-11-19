/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import moment = require("moment");
import groupedVirtualNotification = require("common/notifications/models/groupedVirtualNotification");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("notifications");

class virtualUpdateByQueryFailures extends groupedVirtualNotification<queryBasedVirtualBulkOperationFailureItem> {

    static readonly Id = "virtual$$updateByQueryFailures";

    constructor(db: database) {
        super(db, {
            Id: virtualUpdateByQueryFailures.Id,
            IsPersistent: false,
            Type: "CumulativeUpdateByQueryFailures",
            Database: db.name,

            // properties below will be initialized later
            Message: null,
            CreatedAt: null,
            Title: null,
            Severity: null,
        });

        this.title(t("updateByQuery.title"));
        this.severity("Error");
    }

    merge(dto: Raven.Server.NotificationCenter.Notifications.OperationChanged) {
        this.createdAt(dto.CreatedAt ? moment.utc(dto.CreatedAt) : null);

        const existingItemIndex = this.operations().findIndex(x => x.id === dto.Id);

        const bulkResult = dto.State.Result as Raven.Client.Documents.Operations.OperationExceptionResult;

        const item = {
            id: dto.Id,
            operationId: dto.OperationId,
            date: dto.StartTime,
            duration: moment.utc(dto.EndTime).diff(moment.utc(dto.StartTime)),
            error: bulkResult.Error,
            errorMsg: bulkResult.Message,
            query: (dto.DetailedDescription as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query
        } as queryBasedVirtualBulkOperationFailureItem;

        if (existingItemIndex !== -1) {
            this.operations.splice(existingItemIndex, 1, item);
        } else {
            this.operations.unshift(item);
        }

        const operationsCount = this.operations().length;
        this.message(
            t("updateByQuery.failed", { count: operationsCount, formattedCount: operationsCount.toLocaleString() })
        );
    }
}

export = virtualUpdateByQueryFailures;
