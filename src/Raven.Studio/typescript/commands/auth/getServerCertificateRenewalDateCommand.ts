import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class getServerCertificateRenewalDateCommand extends commandBase { 
    
    execute(): JQueryPromise<string> {
       
        const url = endpoints.global.adminCertificates.adminCertificatesLetsencryptRenewalDate;
        
        return this.query(url, null, null, x => x.EstimatedRenewal)
            .fail((response: JQueryXHR) => this.reportError(t("getRenewalDateFailed"), response.responseText, response.statusText));
    }
}

export = getServerCertificateRenewalDateCommand;
