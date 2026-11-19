import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class saveExternalReplicationTaskCommand extends commandBase {
   
    constructor(private db: database | string, private replicationSettings: Raven.Client.Documents.Operations.Replication.ExternalReplication) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        return this.updateReplication()
            .fail((response: JQueryXHR) => {
                    this.reportError(t("replicationSaveFailed"), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("replicationSaved"));
            });
    }

    private updateReplication(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {

        const url = endpoints.databases.ongoingTasks.adminTasksExternalReplication;
        
        const addRepTask = $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>();

        const payload = {          
            Watcher: this.replicationSettings
        };

        this.post(url, JSON.stringify(payload), this.db)
            .done((results: Array<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>) => {
                addRepTask.resolve(results[0]);
            })
            .fail(response => addRepTask.reject(response));

        return addRepTask;
    }
}

export = saveExternalReplicationTaskCommand; 

