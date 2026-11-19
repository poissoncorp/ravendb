import dialog = require("plugins/dialog");
import database = require("models/resources/database");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import clearIndexErrorsCommand = require("commands/database/index/clearIndexErrorsCommand");
import messagePublisher = require("common/messagePublisher");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clearIndexErrorsConfirm");

class clearIndexErrorsConfirm extends dialogViewModelBase {

    view = require("views/database/indexes/clearIndexErrorsConfirm.html");
    
    title: string;
    subTitleHtml: string;
    errorStateInfoHtml = t("errorStateInfo");
    clearErrorsTask = $.Deferred<boolean>();
    indexesToClear = ko.observableArray<string>();
    clearAllIndexes = ko.observable<boolean>();
    
    spinners = {
        clear: ko.observable<boolean>(false)
    }
    
    constructor(indexesToClear: Array<string>, private db: database, private locations: databaseLocationSpecifier[]) {
        super();
        this.indexesToClear(indexesToClear);
        this.clearAllIndexes(!indexesToClear);
       
        if (this.clearAllIndexes()) {
            this.title = t("title", { context: "all" });
            this.subTitleHtml = t("subTitle", { context: "all" });

        } else if (this.indexesToClear() && this.indexesToClear().length === 1) {
            this.title = t("title", { context: "single" });
            this.subTitleHtml = t("subTitle", { context: "single" });

        } else {
            this.title = t("title", { context: "multiple" });
            this.subTitleHtml = t("subTitleMultiple", { count: this.indexesToClear().length });
        }
    }

    async clearIndexes() {
        const arrayOfTasks = this.locations.map(location => this.clearTask(location).promise());
        
        this.spinners.clear(true);
        
        const results = await Promise.allSettled(arrayOfTasks);
        const rejectedCount = results.filter(x => x.status === "rejected").length;

        if (rejectedCount === 0) {
            messagePublisher.reportSuccess(t("clearSucceeded"));
        } else {
            messagePublisher.reportError(t("clearFailed", { failed: rejectedCount, total: results.length }));
        }
        this.spinners.clear(false);

        this.clearErrorsTask.resolve(true);
        dialog.close(this);
    }

    private clearTask(location: databaseLocationSpecifier): JQueryPromise<any> {
        return new clearIndexErrorsCommand(this.indexesToClear(), this.db, location)
            .execute();
    }

    cancel() {
        this.clearErrorsTask.resolve(false);
        dialog.close(this);
    }
}

export = clearIndexErrorsConfirm;
