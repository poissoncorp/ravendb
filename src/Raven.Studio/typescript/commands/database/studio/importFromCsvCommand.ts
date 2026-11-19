import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommands");

class importFromCsvCommand extends commandBase {

    constructor(private db: database | string, private operationId: number, private file: File, private collectionName: string,
                private isUploading: KnockoutObservable<boolean>, private uploadStatus: KnockoutObservable<number>,
                private csvConfig: Raven.Server.Smuggler.Documents.CsvImportOptions) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const urlArgs = {
            operationId: this.operationId,
            collection: this.collectionName || undefined
        };

        const url = endpoints.databases.smuggler.smugglerImportCsv + this.urlEncodeArgs(urlArgs);

        const formData = new FormData();
        
        formData.append("csvImportOptions", JSON.stringify(this.csvConfig));
        formData.append("file", this.file);

        return this.post(url, formData, this.db, commandBase.getOptionsForImport(this.isUploading, this.uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError(t("uploadFailed"), response.responseText, response.statusText))
            .done(() => this.reportSuccess(t("csvImported")))
    }
}

export = importFromCsvCommand; 
