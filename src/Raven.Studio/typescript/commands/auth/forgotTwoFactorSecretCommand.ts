import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class forgotTwoFactorSecretCommand extends commandBase {
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.twoFactorAuthentication.authentication2fa;
        
        return this.del<void>(url, undefined, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("forgotTwoFactorSecretFailed"), response.responseText, response.statusText));
    }
}

export = forgotTwoFactorSecretCommand;
