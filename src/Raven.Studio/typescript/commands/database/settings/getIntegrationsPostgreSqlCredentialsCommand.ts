import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getIntegrationsPostgreSqlCredentialsCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames> {
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUsers;

        return this.query<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getPostgreSqlCredentialsFailed"), response.responseText, response.statusText));
    }
}

export = getIntegrationsPostgreSqlCredentialsCommand;
