import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class saveServerWideBackupCommand extends commandBase {
    constructor(private configuration: Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration) {
        super();
    } 
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideBackup;
        const isNewTask = this.configuration.TaskId === 0;
        
        return this.put<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse>(url, JSON.stringify(this.configuration))
            .done(() => {
                this.reportSuccess(t("tasks.backupSaved", { context: isNewTask ? "created" : "updated" }));
            })
            .fail(response => this.reportError(t("tasks.saveBackupFailed", { name: this.configuration.Name }), response.responseText, response.statusText));
    }
}

export = saveServerWideBackupCommand; 

