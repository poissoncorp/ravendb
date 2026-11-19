import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class testPeriodicBackupCredentialsCommand extends commandBase {
    private readonly type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType;
    private readonly connectionConfiguration: Raven.Client.Documents.Operations.Backups.BackupSettings;

    constructor(type: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType, connectionConfiguration: Raven.Client.Documents.Operations.Backups.BackupSettings) {
        super();
        this.type = type;
        this.connectionConfiguration = connectionConfiguration;
    }
 
    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.studioTasks.studioTasksPeriodicBackupTestCredentials +
            this.urlEncodeArgs({
                type: this.type
            });

        return this.post(url, JSON.stringify(this.connectionConfiguration), null, { dataType: undefined })
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("backupCredentials.testFailed"), result.Error);
                }
            })
            .fail(response => this.reportError(t("backupCredentials.connectionFailed", { type: this.type }),
                    response.responseText, response.statusText));
    }
}

export = testPeriodicBackupCredentialsCommand; 

