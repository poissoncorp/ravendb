import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class optimizeIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            name: this.indexName,
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesOptimize + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(t("optimizeStarted", { indexName: this.indexName })))
            .fail((response: JQueryXHR) => this.reportError(t("optimizeFailed"), response.responseText));
    }
}

export = optimizeIndexCommand; 
