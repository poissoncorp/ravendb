import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class getAllServerWideTasksCommand extends commandBase {

    constructor(private taskName?: string, private type?: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult> {
        const args = {
            name: this.taskName,
            type: this.type
        };

        const url = endpoints.global.adminStudioServerWide.adminServerWideTasks + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult>(url, null)
            .fail((response: JQueryXHR) => {
                if (this.taskName) {
                    this.reportError(t("tasks.getFailed", { name: this.taskName }), response.responseText, response.statusText);
                } else {
                    this.reportError(t("tasks.getAllFailed"), response.responseText, response.statusText);
                }
            });
    }
}

export = getAllServerWideTasksCommand;
