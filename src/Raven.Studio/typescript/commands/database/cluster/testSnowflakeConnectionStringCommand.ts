import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class testSnowflakeConnectionStringCommand extends commandBase {

    private readonly db: database | string;
    private readonly connectionString: string;

    constructor(db: database | string, connectionString: string) {
        super();
        this.db = db;
        this.connectionString = connectionString;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.databases.snowflakeEtl.adminEtlSnowflakeTestConnection;

        return this.post(url, this.connectionString, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.snowflake"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.snowflake"), result.Error);
                }
            });
    }
}

export = testSnowflakeConnectionStringCommand;
