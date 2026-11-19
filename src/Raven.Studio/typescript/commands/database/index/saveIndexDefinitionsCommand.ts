import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class saveIndexDefinitionCommand extends commandBase {
    private readonly indexes: Raven.Client.Documents.Indexes.IndexDefinition[];
    private readonly isJsIndex: boolean;
    private readonly db: database | string;

    constructor(indexes: Raven.Client.Documents.Indexes.IndexDefinition[], isJsIndex: boolean, db: database | string) {
        super();
        this.indexes = indexes;
        this.isJsIndex = isJsIndex;
        this.db = db;
    }

    execute(): JQueryPromise<string> {

        const count = this.indexes.length;

        return this.saveDefinition()
            .fail((response: JQueryXHR) => {
                this.reportError(t("saveFailed", { count }), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("saved", { count }));
            });
    }

    private saveDefinition(): JQueryPromise<string> {
        const url = this.isJsIndex ? endpoints.databases.index.indexes : endpoints.databases.adminIndex.adminIndexes;
        const saveTask = $.Deferred<string>();

        const payload = {
            Indexes: this.indexes
        };

        this.put(url, JSON.stringify(payload), this.db)
            .done((results: any) => {
                saveTask.resolve(results["Results"][0].Index);
            })
            .fail(response => saveTask.reject(response));

        return saveTask;
    }
}

export = saveIndexDefinitionCommand; 
