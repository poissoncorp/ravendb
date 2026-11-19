import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class saveDocumentCommand extends commandBase {
    constructor(
        private id: string,
        private document: document,
        private db: database | string,
        private reportSaveProgress: boolean = true,
        private transactionMode: Raven.Client.Documents.Session.TransactionMode = "SingleNode"
    ) {
        super();
    }

    execute(): JQueryPromise<saveDocumentResponseDto> {
        this.document.__metadata.id = this.id;
        
        const commands: Array<Raven.Server.Documents.Handlers.Batches.BatchRequestParser.CommandData> = [
            this.document.toBulkDoc("PUT")
        ];

        const args = JSON.stringify({
            Commands: commands,
            TransactionMode: this.transactionMode,
        });
        const url = endpoints.databases.batch.bulk_docs;
        
        const saveTask = this.post<saveDocumentResponseDto>(url, args, this.db);

        if (this.reportSaveProgress) {
            saveTask.done((result: saveDocumentResponseDto) => this.reportSuccess(t("documentSaved", { id: result.Results[0]["@id"] })));
            saveTask.fail((response: JQueryXHR) => this.reportError(t("saveDocumentFailed", { id: this.id }), response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = saveDocumentCommand;
