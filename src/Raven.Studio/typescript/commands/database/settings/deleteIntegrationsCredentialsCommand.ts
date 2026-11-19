import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class deleteIntegrationsCredentialsCommand extends commandBase {

    constructor(private db: database | string, private username: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            username: this.username
        };
        
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUser + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(t("deleteIntegrationsCredentialsSucceeded", { username: this.username })))
            .fail((response: JQueryXHR) => this.reportError(t("deleteIntegrationsCredentialsFailed", { username: this.username }), response.responseText, response.statusText));
    }
}

export = deleteIntegrationsCredentialsCommand;
