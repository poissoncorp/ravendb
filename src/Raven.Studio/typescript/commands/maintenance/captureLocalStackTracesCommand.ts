import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class captureLocalStackTracesCommand extends commandBase {

    execute(): JQueryPromise<stackTracesResponseDto> {
        const url = endpoints.global.threads.adminDebugThreadsStackTrace;
        return this.query<stackTracesResponseDto>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError(t("fetchStackTracesFailed"), response.responseText, response.statusText));
    }
}

export = captureLocalStackTracesCommand;
