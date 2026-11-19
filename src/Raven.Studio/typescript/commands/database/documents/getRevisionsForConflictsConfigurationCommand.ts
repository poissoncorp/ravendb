import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getRevisionsForConflictsConfigurationCommand extends commandBase {
    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration> {
        const deferred = $.Deferred<Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration>();
        const url = endpoints.databases.revisions.revisionsConflictsConfig;
        this.query(url, null, this.db)
            .done((revisionsConfig: Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration) => deferred.resolve(revisionsConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("getRevisionsForConflictsConfigurationFailed"), xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getRevisionsForConflictsConfigurationCommand;
