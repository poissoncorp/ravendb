import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class getConnectivityToLicenseServerCommand extends commandBase {
 
    execute(): JQueryPromise<Raven.Server.Web.Studio.LicenseHandler.ConnectivityToLicenseServer> {
        const url = endpoints.global.license.licenseServerConnectivity;

        return this.query<any>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(t("noLicenseServerConnection"), response.responseText, response.statusText);
            });
    }
}

export = getConnectivityToLicenseServerCommand;
