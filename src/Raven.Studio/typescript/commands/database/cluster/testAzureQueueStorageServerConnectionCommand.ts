import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class testAzureQueueStorageServerConnectionCommand extends commandBase {
    private readonly db: database | string;
    private readonly authentication: Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings; 

    constructor(db: database | string, authentication: Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings) {
        super();
        this.db = db;
        this.authentication = authentication;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.databases.queueEtlConnection.adminEtlQueueAzurequeuestorageTestConnection;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(this.authentication), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.azureQueueStorage"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.azureQueueStorage"), result.Error);
                }
            });
    }
}

export = testAzureQueueStorageServerConnectionCommand;
