import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexHistoryCommand extends commandBase {

    constructor(private db: database | string, private indexName: string) {
        super();
    }

    execute(): JQueryPromise<indexHistoryCommandResult> {
        const args = { name: this.indexName };
        const url = endpoints.databases.index.indexesHistory;
        
        return this.query<indexHistoryCommandResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getHistoryFailed"), response.responseText, response.statusText));
    }
} 

export = getIndexHistoryCommand;
