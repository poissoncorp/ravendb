import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class deleteIntegrationsPostgreSqlCredentialsCommand extends commandBase {

    constructor(private db: database | string, private username: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            username: this.username
        };
        
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUser + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(t("deletePostgreSqlCredentialsSucceeded", { username: genUtils.escapeHtml(this.username) })))
            .fail((response: JQueryXHR) => this.reportError(t("deletePostgreSqlCredentialsFailed", { username: genUtils.escapeHtml(this.username) }), response.responseText, response.statusText));
    }
}

export = deleteIntegrationsPostgreSqlCredentialsCommand;
