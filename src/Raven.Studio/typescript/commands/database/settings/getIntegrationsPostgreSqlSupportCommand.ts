import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getIntegrationsPostgreSqlSupportCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlServerStatus> {
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlServerStatus;

        return this.query<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlServerStatus>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getPostgreSqlSupportFailed"), response.responseText, response.statusText));
    }
}

export = getIntegrationsPostgreSqlSupportCommand;
