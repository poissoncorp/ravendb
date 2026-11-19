import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class saveReplicationHubTaskCommand extends commandBase {

    constructor(private db: database | string, private replicationSettings: Raven.Client.Documents.Operations.Replication.PullReplicationDefinition) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const url = endpoints.databases.pullReplication.adminTasksPullReplicationHub;

        return this.put<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>(url, JSON.stringify(this.replicationSettings), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("hubTaskSaveFailed"), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("hubTaskSaved"));
            });
    }
}

export = saveReplicationHubTaskCommand; 

