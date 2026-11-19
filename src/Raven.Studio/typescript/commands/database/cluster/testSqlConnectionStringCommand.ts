import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class testSqlConnectionStringCommand extends commandBase {

    private readonly db: database | string;
    private readonly connectionString: string;
    private readonly factoryName: string;

    constructor(db: database | string, connectionString: string, factoryName: string) {
        super();
        this.db = db;
        this.connectionString = connectionString;
        this.factoryName = factoryName;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            factoryName: this.factoryName
        };
        const url = endpoints.databases.sqlEtl.adminEtlSqlTestConnection + this.urlEncodeArgs(args);

        return this.post(url, this.connectionString, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.sql"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.sql"), result.Error);
                }
            });
    }
}

export = testSqlConnectionStringCommand;
