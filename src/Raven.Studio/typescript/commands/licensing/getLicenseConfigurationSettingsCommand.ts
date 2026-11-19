import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class getLicenseConfigurationSettingsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Config.Categories.LicenseConfiguration> {
        const url = endpoints.global.license.licenseConfiguration;
        
        return this.query<Raven.Server.Config.Categories.LicenseConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getConfigurationSettingsFailed"), response.responseText));
    }
}

export = getLicenseConfigurationSettingsCommand;
