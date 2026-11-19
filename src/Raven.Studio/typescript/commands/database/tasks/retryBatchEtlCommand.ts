import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class retryBatchEtlCommand extends commandBase {
    constructor(private db: database | string, private etlProcessName: string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = { ...this.location, name: this.etlProcessName };
        
        const url = endpoints.databases.etl.etlRetryBatch + this.urlEncodeArgs(args);

        return this.post<void>(url, null, this.db, { dataType: "text" })
            .done(() => this.reportSuccess(t("etlBatchRetried")))
            .fail((response: JQueryXHR) => this.reportError(t("etlBatchRetryFailed"), response.responseText, response.statusText));
    }
}

export = retryBatchEtlCommand;

