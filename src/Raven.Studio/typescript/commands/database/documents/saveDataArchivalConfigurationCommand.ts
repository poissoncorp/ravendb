import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class saveDataArchivalConfigurationCommand extends commandBase {
    private readonly db: database | string;
    private readonly configuration: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
    

    constructor(db: database | string, configuration: Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration) {
        super();
        this.db = db;
        this.configuration = configuration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.dataArchival.adminDataArchivalConfig;
        const args = ko.toJSON(this.configuration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("saveDataArchivalConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveDataArchivalConfigurationCommand;
