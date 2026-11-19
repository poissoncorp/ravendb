import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

export default class testServerWideSqlConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: string, private readonly factoryName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            factoryName: this.factoryName,
        };
        const url = endpoints.global.sqlEtlServerWide.adminEtlSqlTestConnection + this.urlEncodeArgs(args);

        return this.post(url, this.connectionString)
            .fail((response: JQueryXHR) => this.reportError(t("connectionStrings.testFailed.sql"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("connectionStrings.testFailed.sql"), result.Error);
                }
            });
    }
}
