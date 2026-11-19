import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import ExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;

const t = i18nModule.createTranslator("documentsCommands");

class saveExpirationConfigurationCommand extends commandBase {

    private readonly db: database | string;
    private readonly expirationConfiguration: ExpirationConfiguration

    constructor(db: database | string, expirationConfiguration: ExpirationConfiguration) {
        super();
        this.db = db;
        this.expirationConfiguration = expirationConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.expiration.adminExpirationConfig;
        const args = ko.toJSON(this.expirationConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("saveExpirationConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveExpirationConfigurationCommand;
