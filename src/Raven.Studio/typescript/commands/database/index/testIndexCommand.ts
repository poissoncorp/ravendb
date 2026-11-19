import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class testIndexCommand extends commandBase {

    constructor(private testRequest: Raven.Server.Documents.Indexes.Test.TestIndexParameters, private db: database | string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.Test.TestIndexResult> {
        const url = endpoints.databases.adminIndex.indexesTest + this.urlEncodeArgs(this.location);
        
        return this.post(url, JSON.stringify(this.testRequest), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("testFailed"), response.responseText, response.statusText);
            });
    }
}

export = testIndexCommand; 
