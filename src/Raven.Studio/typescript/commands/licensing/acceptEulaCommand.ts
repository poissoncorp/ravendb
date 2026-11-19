import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class acceptEulaCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.adminLicenseEulaAccept;
        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("saveLicenseSettingsFailed"), response.responseText));
    }
}

export = acceptEulaCommand;
