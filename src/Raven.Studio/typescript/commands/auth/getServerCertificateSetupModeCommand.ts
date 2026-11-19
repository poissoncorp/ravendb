import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class getServerCertificateSetupModeCommand extends commandBase { 
    
    execute(): JQueryPromise<Raven.Server.Commercial.SetupMode> {
       
        const url = endpoints.global.adminCertificates.adminCertificatesMode;
        
        return this.query(url, null, null, x => x.SetupMode)
            .fail((response: JQueryXHR) => this.reportError(t("getSetupModeFailed"), response.responseText, response.statusText));
    }
}

export = getServerCertificateSetupModeCommand;
