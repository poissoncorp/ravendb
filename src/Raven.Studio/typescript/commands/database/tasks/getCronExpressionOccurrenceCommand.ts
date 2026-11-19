import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getCronExpressionOccurrenceCommand extends commandBase {
    constructor(private cronExpression: string,
                private databaseName?: string,
                private taskId?: number,
                private isFull?: boolean) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence> {
        const args = {
            expression: this.cronExpression,
            database: this.databaseName,
            taskId: this.taskId,
            isFull: this.isFull
        };

        const url = endpoints.global.studioTasks.studioTasksNextCronExpressionOccurrence +
            this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.StudioTasksHandler.NextCronExpressionOccurrence>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("cronOccurrenceFailed"), response.responseText, response.statusText));
    }
}

export = getCronExpressionOccurrenceCommand;
