import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class deleteIndexCommand extends commandBase {
    private readonly indexName: string;
    private readonly db: database | string;

    constructor(indexName: string, db: database | string) {
        super();
        this.db = db;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<any> {
        const args = {
            name: this.indexName
        };

        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);

        return this.del(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("deleteFailed", { indexName: this.indexName }), response.responseText))
            .done(() => this.reportSuccess(t("deleted", { indexName: this.indexName })));
    }
}

export = deleteIndexCommand;
