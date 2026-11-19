import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class explainQueryCommand extends commandBase {

    constructor(private queryText: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<explainQueryResponse> {
        const args = {
            debug: "explain"
        };
        
        const payload: Partial<Raven.Client.Documents.Queries.IndexQuery<any>> = {
            Query: this.queryText
        };

        const url = endpoints.databases.queries.queries + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("explainQueryFailed"), response.responseText, response.statusText);
            });
    }
}

export = explainQueryCommand;
