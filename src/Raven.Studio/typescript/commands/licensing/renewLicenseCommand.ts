import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class renewLicenseCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseRenewalResult> {
        const url = endpoints.global.license.adminLicenseRenew;
        
        return this.post(url, null, null)
            .fail((response: JQueryXHR) => {
                this.reportError(t("renewFailed"), response.responseText, response.statusText);
            });
    }
}

export = renewLicenseCommand;
