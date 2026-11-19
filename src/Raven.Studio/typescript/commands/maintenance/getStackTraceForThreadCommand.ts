import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class getStackTraceForThreadCommand extends commandBase {

    constructor(private threadId: number) {
        super();
    }
    
    execute(): JQueryPromise<threadStackTraceResponseDto> {
        const args = {
            threadId: this.threadId
        };
        
        const url = endpoints.global.threads.adminDebugThreadsStackTrace + this.urlEncodeArgs(args);
        
        return this.query<threadStackTraceResponseDto>(url, null, null)
            .fail((response: JQueryXHR) => this.reportError(t("fetchThreadStackTraceFailed", { threadId: this.threadId }), response.responseText, response.statusText));
    }
}

export = getStackTraceForThreadCommand;
