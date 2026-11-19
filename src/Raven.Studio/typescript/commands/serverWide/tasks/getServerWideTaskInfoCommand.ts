import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class getServerWideTaskInfoCommand<T extends Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplication |
                                             Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration> extends commandBase {

    private constructor(private taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private taskName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.ServerWideTasksResult<T>> {
        return this.getServerWideTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(t("tasks.getInfoFailed", { type: this.taskType, name: this.taskName }), response.responseText, response.statusText);
            });
    }

    private getServerWideTaskInfo(): JQueryPromise<Raven.Server.Web.System.ServerWideTasksResult<T>> {
        const args = {
            type: this.taskType,
            name: this.taskName
        };

        const url = endpoints.global.adminServerWide.adminConfigurationServerWideTasks + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.System.ServerWideTasksResult<T>>(url, null);
    }

    static forExternalReplication(taskName: string) {
        return new getServerWideTaskInfoCommand<Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideExternalReplication>("Replication", taskName);
    }

    static forBackup(taskName: string) {
        return new getServerWideTaskInfoCommand<Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration>("Backup", taskName);
    }
}

export = getServerWideTaskInfoCommand;
