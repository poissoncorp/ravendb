import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class generateTwoFactorSecretCommand extends commandBase {

    execute(): JQueryPromise<{ Secret: string  }> {
        const url = endpoints.global.adminCertificates.adminCertificates2faGenerate;
        
        return this.query<{ Secret: string }>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError(t("generateTwoFactorSecretFailed"), response.responseText, response.statusText));
    }
}

export = generateTwoFactorSecretCommand;
