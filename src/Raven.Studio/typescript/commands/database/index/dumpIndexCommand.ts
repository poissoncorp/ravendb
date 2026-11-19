import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class dumpIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database | string, private dumpDirectoryPath: string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            path: this.dumpDirectoryPath,
            ...this.location
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesDump + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(t("dumpCreated", { indexName: this.indexName })))
            .fail((response: JQueryXHR) => this.reportError(t("dumpFailed"), response.responseText));
    }
}

export = dumpIndexCommand; 
