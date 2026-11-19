import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class stopRecordingTransactionsCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }
    
    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.transactionsRecording.adminTransactionsStopRecording;
        
        return this.post<void>(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess(t("transactionsRecording.stopped")))
            .fail((response: JQueryXHR) => this.reportError(t("transactionsRecording.stopFailed"), response.responseText, response.statusText));
    }

}

export = stopRecordingTransactionsCommand;
