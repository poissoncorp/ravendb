import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexEntriesFieldsCommand extends commandBase {

    private readonly indexName: string;
    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;
    private readonly reportFailure: boolean;

    constructor(indexName: string, db: database | string, location: databaseLocationSpecifier, reportFailure = true) {
        super();
        this.indexName = indexName;
        this.db = db;
        this.location = location;
        this.reportFailure = reportFailure;
    }

    execute(): JQueryPromise<getIndexEntriesFieldsCommandResult[]> {
        const task = this.getIndexEntries();

        if (this.reportFailure) {
            task.fail((response: JQueryXHR) => {
                this.reportError(t("getEntriesFailed"), response.responseText, response.statusText);
            });
        }

        return task;
    }

    private getIndexEntries(): JQueryPromise<getIndexEntriesFieldsCommandResult[]> {
        const args = {
            name: this.indexName,
            op: "entries-fields",
            ...this.location
        };

        const url = endpoints.databases.index.indexesDebug;

        return this.query(url, args, this.db);
    }
}

export = getIndexEntriesFieldsCommand;
