import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");
import database = require("models/resources/database");

class getDatabaseStudioConfigurationCommand extends commandBase {
    
    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.StudioConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationStudio;
        const loadTask = $.Deferred<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>();

        this.query<Raven.Client.Documents.Operations.Configuration.StudioConfiguration>(url, null, this.db)
            .done(dto => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                if (response.status !== 404) {
                    this.reportError(t("getDatabaseStudioConfigurationFailed"), response.responseText, response.statusText);
                    loadTask.reject(response);
                } else {
                    loadTask.resolve(null);
                }
            });

        return loadTask;
    }
}

export = getDatabaseStudioConfigurationCommand;
