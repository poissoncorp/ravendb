import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class testClusterNodeConnectionCommand extends commandBase {

    private readonly serverUrl: string;
    private readonly databaseName?: string;
    private readonly bidirectional: boolean = true;

    constructor(serverUrl: string, databaseName?: string, bidirectional = true) {
        super();
        this.serverUrl = serverUrl;
        this.databaseName = databaseName;
        this.bidirectional = bidirectional;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            url: this.serverUrl,
            database: this.databaseName,
            bidirectional: this.bidirectional
        };
        const url = endpoints.global.testConnection.adminTestConnection + this.urlEncodeArgs(args);

        return this.post(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.clusterNode"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.clusterNode"), result.Error);
                }
            });
    }
}

export = testClusterNodeConnectionCommand;
