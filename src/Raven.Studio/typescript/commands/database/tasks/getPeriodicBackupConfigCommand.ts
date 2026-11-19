import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getPeriodicBackupConfigCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<periodicBackupServerLimitsResponse> {
        const url = endpoints.databases.ongoingTasks.adminPeriodicBackupConfig;

        return this.query<periodicBackupServerLimitsResponse>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("periodicBackupConfigFailed"), response.responseText, response.statusText));
    }
}

export = getPeriodicBackupConfigCommand;
