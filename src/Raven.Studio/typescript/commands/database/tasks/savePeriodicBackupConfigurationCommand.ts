import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class savePeriodicBackupConfigurationCommand extends commandBase {
    constructor(private db: database | string, private configuration: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const url = endpoints.databases.ongoingTasks.adminPeriodicBackup;

        const isNewTask = this.configuration.TaskId === 0;
        return this.post(url, JSON.stringify(this.configuration), this.db)
            .done(() => {
                this.reportSuccess(t("backupConfigurationSaved", { context: isNewTask ? "created" : "updated" }));
            })
            .fail(response => {
                const errorMessage = isNewTask ?
                    t("newPeriodicBackupSaveFailed") :
                    t("periodicBackupSaveFailed", { taskId: this.configuration.TaskId });
                this.reportError(errorMessage, response.responseText, response.statusText);
            });
    }
}

export = savePeriodicBackupConfigurationCommand; 

