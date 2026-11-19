import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexStalenessReasonsCommand extends commandBase {

    private indexName: string;

    private db: database | string;

    private location: databaseLocationSpecifier;

    constructor(indexName: string, db: database | string, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<indexStalenessReasonsResponse> {
        const args = {
            name: this.indexName,
            ...this.location
        };
        const url = endpoints.databases.index.indexesStaleness;

        return this.query<indexStalenessReasonsResponse>(url, args, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getStalenessReasonsFailed"), response.responseText, response.statusText);
            });
    }
}

export = getIndexStalenessReasonsCommand;
