import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveIntegrationsPostgreSqlCredentialsCommand extends commandBase {

    constructor(private db: database | string, private username: string, private password: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUser;
        const payload = {
            Username: this.username,
            Password: this.password
        }
        
        return this.put<void>(url, JSON.stringify(payload), this.db)
            .done(() => this.reportSuccess(t("savePostgreSqlCredentialsSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("savePostgreSqlCredentialsFailed"), response.responseText, response.statusText));
    }
}

export = saveIntegrationsPostgreSqlCredentialsCommand;
