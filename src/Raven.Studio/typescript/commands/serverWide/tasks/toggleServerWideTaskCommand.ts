import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class toggleServerWideTaskCommand extends commandBase {

    constructor(private type: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private name: string, private disable: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Configuration.PutServerWideBackupConfigurationResponse> {
        const args = {
            type: this.type,
            name: this.name,
            disable: this.disable
        };
        
        const url = endpoints.global.adminServerWide.adminConfigurationServerWideState;

        return this.post(url + this.urlEncodeArgs(args), null)
            .fail((response: JQueryXHR) => this.reportError(t("tasks.toggleFailed", { context: this.disable ? "disable" : "enable", name: this.name }), response.responseText));
    }
}

export = toggleServerWideTaskCommand; 
