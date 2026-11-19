import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class deleteServerWideTaskCommand extends commandBase {
    constructor(private type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private name: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.OngoingTasks.ServerWideTaskResponse> {
        const args = {
            type: this.type,
            name: this.name
        };
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideTask + this.urlEncodeArgs(args);

        return this.del<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("tasks.deleteFailed", { type: this.type, name: this.name }), response.responseText));
    }
}

export = deleteServerWideTaskCommand;
