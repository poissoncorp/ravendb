import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class saveReplicationSinkTaskCommand extends commandBase {

    constructor(private db: database | string, private replicationSettings: Raven.Client.Documents.Operations.Replication.PullReplicationAsSink) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.PullReplicationAsSink> {
        const url = endpoints.databases.pullReplication.adminTasksSinkPullReplication;
        const payload = {
            PullReplicationAsSink: this.replicationSettings
        };

        return this.post<Raven.Client.Documents.Operations.Replication.PullReplicationAsSink>(url, JSON.stringify(payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("sinkTaskSaveFailed"), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("sinkTaskSaved"));
            });
    }
}

export = saveReplicationSinkTaskCommand; 

