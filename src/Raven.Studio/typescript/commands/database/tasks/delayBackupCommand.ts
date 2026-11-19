import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class delayBackupCommand extends commandBase {
    private databaseName: string;

    private readonly taskId: number;

    private readonly duration: string;

    constructor(db: database | string, taskId: number, duration: string) {
        super();
        this.duration = duration;
        this.taskId = taskId;
        this.databaseName = (typeof db === "string" ? db : db.name);
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Backups.StartBackupOperationResult> {
        const args = {
            taskId: this.taskId,
            duration: this.duration,
            database: this.databaseName
        }
        const url = endpoints.global.adminDatabases.adminBackupTaskDelay + this.urlEncodeArgs(args);
        
        return this.post(url, null, null,  { dataType: undefined })
            .done(() => this.reportSuccess(t("backupDelayed")))
            .fail(response => this.reportError(t("backupDelayFailed"), response.responseText, response.statusText));
    }
}

export = delayBackupCommand;
