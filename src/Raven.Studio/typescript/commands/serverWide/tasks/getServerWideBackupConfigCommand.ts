import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class getServerWideBackupConfigCommand extends commandBase {

    execute(): JQueryPromise<periodicBackupServerLimitsResponse> {
        const url = endpoints.global.adminServerWide.adminConfigurationServerWide;

        return this.query<periodicBackupServerLimitsResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("tasks.getBackupConfigFailed"), response.responseText, response.statusText));
    }
}

export = getServerWideBackupConfigCommand;
