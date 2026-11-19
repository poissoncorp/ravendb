import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import Authentication = Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class testElasticSearchNodeConnectionCommand extends commandBase {

    private readonly db: database | string;
    private readonly serverUrl: string;
    private readonly authenticationDto: Authentication;

    constructor(db: database | string, serverUrl: string, authenticationDto: Authentication) {
        super();
        this.db = db;
        this.serverUrl = serverUrl;
        this.authenticationDto = authenticationDto;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            url: this.serverUrl
        };
        
        const url = endpoints.databases.elasticSearchEtlConnection.adminEtlElasticsearchTestConnection + this.urlEncodeArgs(args);
        const payload = this.authenticationDto;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.elasticSearch"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.elasticSearch"), result.Error);
                }
            });
    }
}

export = testElasticSearchNodeConnectionCommand;
