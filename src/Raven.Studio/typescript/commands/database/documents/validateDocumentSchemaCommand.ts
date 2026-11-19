import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class validateDocumentSchemaCommand extends commandBase {
    constructor(private db: database | string, private document: documentDto) {
        super();
    }

    execute(): JQueryPromise<ValidateDocumentResult> {
        const url = endpoints.databases.studioDocument.studioValidateSchema;

        return this.post<ValidateDocumentResult>(url, JSON.stringify(this.document), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError(t("validateSchemaFailed"), response.responseText, response.statusText)
            )
    }
}

export = validateDocumentSchemaCommand;
