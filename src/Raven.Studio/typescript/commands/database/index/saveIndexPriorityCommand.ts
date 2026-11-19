import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class saveIndexPriorityCommand extends commandBase {

    private indexName: string;

    private priority: Raven.Client.Documents.Indexes.IndexPriority;

    private db: database | string;

    constructor(indexName: string, priority: Raven.Client.Documents.Indexes.IndexPriority, db: database | string) {
        super();
        this.db = db;
        this.priority = priority;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<void> {
        const payload: Raven.Client.Documents.Operations.Indexes.SetIndexesPriorityOperation.Parameters = {
            Priority: this.priority,
            IndexNames: [this.indexName]
        };
        
        const url = endpoints.databases.index.indexesSetPriority;
        
        return this.post<void>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess(t("prioritySet", { indexName: this.indexName, priority: this.priority }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("setPriorityFailed"), response.responseText));
    }
}

export = saveIndexPriorityCommand; 
