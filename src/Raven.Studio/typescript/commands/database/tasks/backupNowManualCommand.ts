import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class backupNowManualCommand extends commandBase {
    constructor(private db: database | string, private manualBackupDto: Raven.Client.Documents.Operations.Backups.BackupConfiguration) {
        super();
    }
 
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Backups.StartBackupOperationResult> {
        const url = endpoints.databases.ongoingTasks.adminBackup;
        
        const payload = this.manualBackupDto;

        return this.post(url, JSON.stringify(payload), this.db)
            .done(() => this.reportSuccess(t("manualBackupStarted")))
            .fail(response => this.reportError(t("manualBackupStartFailed"), response.responseText, response.statusText));
    }
}

export = backupNowManualCommand;
