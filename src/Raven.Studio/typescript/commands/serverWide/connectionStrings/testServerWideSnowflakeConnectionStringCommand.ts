import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

export default class testServerWideSnowflakeConnectionStringCommand extends commandBase {
    constructor(private readonly connectionString: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.snowflakeEtlServerWide.adminEtlSnowflakeTestConnection;

        return this.post(url, this.connectionString)
            .fail((response: JQueryXHR) => this.reportError(t("connectionStrings.testFailed.snowflake"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("connectionStrings.testFailed.snowflake"), result.Error);
                }
            });
    }
}
