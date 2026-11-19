import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class detectIndexTypeCommand extends commandBase {

    constructor(private index: Raven.Client.Documents.Indexes.IndexDefinition, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.Processors.IndexTypeInfo> {
        const url = endpoints.databases.studioIndex.studioIndexType;
        
        const task = $.Deferred<Raven.Server.Web.Studio.Processors.IndexTypeInfo>();
        
        this.post(url, JSON.stringify(this.index), this.db)
            .done((result: Raven.Server.Web.Studio.Processors.IndexTypeInfo) => {
                task.resolve(result);
            }).fail((response: JQueryXHR) => {
                this.reportError(t("detectTypeFailed"), response.responseText, response.statusText);
                task.reject(response);
            });
        
        return task;
    }
}

export = detectIndexTypeCommand; 
