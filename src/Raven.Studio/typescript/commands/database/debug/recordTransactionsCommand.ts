import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class recordTransactionsCommand extends commandBase {

    constructor(private db: database | string, private targetPath: string) {
        super();
    }
    
    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.transactionsRecording.adminTransactionsStartRecording;
        const payload: Raven.Client.Documents.Operations.TransactionsRecording.StartTransactionsRecordingOperation.Parameters = {
            File: this.targetPath
        };
        
        return this.post<operationIdDto>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .done(() => this.reportSuccess(t("transactionsRecording.started", { databaseName: typeof this.db === "string" ? this.db : this.db.name })))
            .fail((response: JQueryXHR) => this.reportError(t("transactionsRecording.startFailed"), response.responseText, response.statusText));
    }

}

export = recordTransactionsCommand;
