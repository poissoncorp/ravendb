import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class getTwoFactorServerConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Server.Web.Authentication.TwoFactorAuthenticationHandler.TotpServerConfiguration> {
        const url = endpoints.global.twoFactorAuthentication.authentication2faConfiguration;
        
        return this.query<Raven.Server.Web.Authentication.TwoFactorAuthenticationHandler.TotpServerConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getTwoFactorConfigurationFailed"), response.responseText, response.statusText));
    }
}

export = getTwoFactorServerConfigurationCommand;
