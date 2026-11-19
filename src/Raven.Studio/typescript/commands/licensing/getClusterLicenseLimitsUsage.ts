import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class getClusterLicenseLimitsUsage extends commandBase {

    execute(): JQueryPromise<Raven.Server.Commercial.LicenseLimitsUsage> {
        const url = endpoints.global.license.licenseLimitsUsage;
        
        return this.query<Raven.Server.Commercial.LicenseLimitsUsage>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getLimitsUsageFailed"), response.responseText));
    }
}

export = getClusterLicenseLimitsUsage;
