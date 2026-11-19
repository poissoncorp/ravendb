import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class validateTwoFactorSecretCommand extends commandBase {

    constructor(private secret: string, private limits: boolean, private sessionDuration: number) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.twoFactorAuthentication.authentication2fa + this.urlEncodeArgs({ hasLimits: this.limits, sessionDurationInMin: this.sessionDuration });
        
        const payload = {
            Token: this.secret
        }
        
        return this.post<{ Secret: string }>(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("validateTwoFactorSecretFailed"), response.responseText, response.statusText));
    }
}

export = validateTwoFactorSecretCommand;
