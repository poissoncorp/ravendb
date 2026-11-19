import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class replayTransactionsCommand extends commandBase {

    constructor(private db: database | string, private operationId: number, private file: File,
                private isUploading: KnockoutObservable<boolean>, private uploadStatus: KnockoutObservable<number>) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TransactionsRecording.ReplayTxOperationResult> {
        const urlArgs = {
            operationId: this.operationId
        };
        
        const url = endpoints.databases.transactionsRecording.transactionsReplay + this.urlEncodeArgs(urlArgs);
        
        const formData = new FormData();
        formData.append("file", this.file);
        
        return this.post<Raven.Client.Documents.Operations.TransactionsRecording.ReplayTxOperationResult>(url, formData, this.db, commandBase.getOptionsForImport(this.isUploading, this.uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError(t("replayTransactionsFailed"), response.responseText, response.statusText));
    }

}

export = replayTransactionsCommand;
