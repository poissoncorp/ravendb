import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class forceRenewServerCertificateCommand extends commandBase {
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificatesLetsencryptForceRenew;  
                
        return this.post<void>(url, null)
            .done(() => this.reportSuccess(t("forceRenewRequested")))
            .fail((response: JQueryXHR) => this.reportError(t("forceRenewFailed"), response.responseText, response.statusText));
    }
}

export = forceRenewServerCertificateCommand;
