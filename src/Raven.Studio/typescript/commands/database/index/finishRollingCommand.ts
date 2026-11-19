import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class finishRollingCommand extends commandBase {
    constructor(private db: database | string, private indexName: string, private nodeTag: string = null) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            node: this.nodeTag || undefined
        };
        const url = endpoints.databases.index.indexesFinishRolling + this.urlEncodeArgs(args);
        
        return this.post<void>(url, null, this.db)
            .done(() => this.reportSuccess(
                t("rollingFinished", {
                    indexName: this.indexName,
                    nodeTag: this.nodeTag,
                    context: this.nodeTag ? "onNode" : undefined,
                })
            ));
    }
}

export = finishRollingCommand;
