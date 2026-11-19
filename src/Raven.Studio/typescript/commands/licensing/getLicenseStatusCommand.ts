import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class getLicenseStatusCommand extends commandBase {

    execute(): JQueryPromise<LicenseStatus> {
        const url = endpoints.global.license.licenseStatus;
        return this.query<LicenseStatus>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getStatusFailed"), response.responseText));
    }
}

export = getLicenseStatusCommand;
