import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class deleteDocsMatchingQueryCommand extends commandBase {
    constructor(private queryText: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            Query: this.queryText,
            WaitForNonStaleResults: true
        };

        const url = endpoints.databases.queries.queries;
        return this.del<operationIdDto>(url, JSON.stringify(args), this.db)
            .fail((response: JQueryXHR) => this.reportError(t("deleteDocsMatchingQueryFailed"), response.responseText, response.statusText));
    }

}

export = deleteDocsMatchingQueryCommand; 
