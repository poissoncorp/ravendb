import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import Authentication = Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

export default class testServerWideElasticSearchNodeConnectionCommand extends commandBase {
    constructor(private readonly serverUrl: string, private readonly authenticationDto: Authentication) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            url: this.serverUrl,
        };

        const url =
            endpoints.global.elasticSearchEtlServerWideConnection.adminEtlElasticsearchTestConnection +
            this.urlEncodeArgs(args);
        const payload = this.authenticationDto;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), undefined, {
            dataType: undefined,
        })
            .fail((response: JQueryXHR) => this.reportError(t("connectionStrings.testFailed.elasticSearch"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("connectionStrings.testFailed.elasticSearch"), result.Error);
                }
            });
    }
}
