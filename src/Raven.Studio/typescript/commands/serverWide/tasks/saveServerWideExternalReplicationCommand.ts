import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class saveServerWideExternalReplicationCommand extends commandBase {
    constructor(private configuration: Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplication) {
        super();
    } 
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideExternalReplication;
        
        const isNewTask = this.configuration.TaskId === 0;
        
        return this.put<Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplicationResponse>(url, JSON.stringify(this.configuration))
            .done(() => {
                this.reportSuccess(t("tasks.externalReplicationSaved", { context: isNewTask ? "created" : "updated" }));
            })
            .fail(response => this.reportError(t("tasks.saveExternalReplicationFailed", { name: this.configuration.Name }), response.responseText, response.statusText));
    }
}

export = saveServerWideExternalReplicationCommand; 

