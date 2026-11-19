import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class getServerSettingsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Config.SettingsResult> {
        const url = endpoints.global.adminConfiguration.adminConfigurationSettings;

        return this.query<Raven.Server.Config.SettingsResult>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getServerSettingsFailed"), response.responseText, response.statusText));
    }
}

export = getServerSettingsCommand;
