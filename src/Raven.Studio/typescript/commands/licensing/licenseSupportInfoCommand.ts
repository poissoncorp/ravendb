import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class licenseSupportInfoCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseSupportInfo> {
        const url = endpoints.global.license.licenseSupport;

        return this.query<Raven.Server.Commercial.LicenseSupportInfo>(url, null)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getSupportInfoFailed"), response.responseText, response.statusText);
            });
    }
}

export = licenseSupportInfoCommand; 
