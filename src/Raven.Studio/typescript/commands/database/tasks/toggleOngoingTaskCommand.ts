import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class toggleOngoingTaskCommand extends commandBase {

    private db: database | string;

    private taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

    private taskId: number;

    private taskName: string;

    private disable: boolean;

    constructor(db: database | string, taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, taskId: number, taskName: string, disable: boolean) {
        super();
        this.disable = disable;
        this.taskName = taskName;
        this.taskId = taskId;
        this.taskType = taskType;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const args = { key: this.taskId, type: this.taskType, disable: this.disable, taskName: this.taskName };

        // Subscription is the only task that needs only *User* authenication for toggling state
        // All others use *Admin* authentication
        const url = this.taskType === "Subscription" ? endpoints.databases.ongoingTasks.subscriptionTasksState :
                                                       endpoints.databases.ongoingTasks.adminTasksState;

        const toggleContext = this.disable ? "disable" : "enable";
     
        return this.post(url + this.urlEncodeArgs(args), null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("toggleTaskFailed", { taskType: this.taskType, context: toggleContext }), response.responseText));
    }
}

export = toggleOngoingTaskCommand; 
