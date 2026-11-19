import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import deleteTimeSeriesCommand = require("commands/database/documents/timeSeries/deleteTimeSeriesCommand");
import messagePublisher = require("common/messagePublisher");
import generalUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("editTimeSeries");

class deleteTimeSeries extends dialogViewModelBase {

    view = require("views/database/timeSeries/deleteTimeSeries.html");

    spinners = {
        delete: ko.observable<boolean>(false)
    };

    deletingAllHtml: string;
    deletingSelectionHtml: string;

    constructor(private timeSeriesName: string, private documentId: string, private db: database, private criteria: timeSeriesDeleteCriteria) {
        super();
        criteria.selection = criteria.selection || [];

        const name = generalUtils.escapeHtml(timeSeriesName);
        const count = criteria.selection.length;
        this.deletingAllHtml = t("deleteDialog.deletingAll", { name });
        this.deletingSelectionHtml = t("deleteDialog.deletingSelection", { count, formattedCount: count.toLocaleString(), name });
    }

    private createDto(): Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.DeleteOperation[] {
        switch (this.criteria.mode) {
            case "all":
                return [
                    {
                        From: null,
                        To: null
                    }
                ];
            case "selection":
                return this.criteria.selection.map(x => ({
                    From: x.Timestamp,
                    To: x.Timestamp
                }));
        }
    }

    deleteItems() {
        const dto = this.createDto();

        this.spinners.delete(true);

        new deleteTimeSeriesCommand(this.documentId, this.timeSeriesName, dto, this.db)
            .execute()
            .done(() => {
                const postDelete: postTimeSeriesDeleteAction = this.criteria.mode === "all" ? "changeTimeSeries" : "reloadCurrent";
                messagePublisher.reportSuccess(t("deletedValues"));
                dialog.close(this, postDelete);
            })
            .always(() => this.spinners.delete(false));
    }

    cancel() {
        dialog.close(this, "doNothing" as postTimeSeriesDeleteAction);
    }

    deactivate() {
        super.deactivate(null);
    }
}

export = deleteTimeSeries;
