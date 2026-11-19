import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getDataArchivalConfigurationCommand extends commandBase {

    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration>();
        const url = endpoints.databases.dataArchival.dataArchivalConfig;
        this.query(url, null, this.db)
            .done((expirationConfig: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration) => deferred.resolve(expirationConfig))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("getDataArchivalConfigurationFailed"), xhr.responseText, xhr.statusText);
                }
            });

        return deferred;
    }
}

export = getDataArchivalConfigurationCommand;
