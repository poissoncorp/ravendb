import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getSystemStorageReportCommand extends commandBase {

    execute(): JQueryPromise<detailedSystemStorageReportItemDto> {
        const args = {
            detailed: false
        };
        const url = endpoints.global.adminStorage.adminDebugStorageEnvironmentReport + this.urlEncodeArgs(args);
        return this.query<detailedSystemStorageReportItemDto>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getSystemStorageReportFailed"), response.responseText, response.statusText));
    }
}

export = getSystemStorageReportCommand;
