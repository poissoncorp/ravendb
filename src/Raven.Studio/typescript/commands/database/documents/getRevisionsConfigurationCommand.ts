import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getRevisionsConfigurationCommand extends commandBase {
    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration>();
        const url = endpoints.databases.revisions.revisionsConfig;
        this.query(url, null, this.db)
            .done((revisionsConfig: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration) => deferred.resolve(revisionsConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("getRevisionsConfigurationFailed"), xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getRevisionsConfigurationCommand;
