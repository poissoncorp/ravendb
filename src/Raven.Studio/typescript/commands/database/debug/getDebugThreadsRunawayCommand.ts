import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getDebugThreadsRunawayCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Dashboard.ThreadInfo[]> {
        const url = endpoints.global.threads.adminDebugThreadsRunaway;
        return this.query<Raven.Server.Dashboard.ThreadInfo[]>(url, null, null,  x => x["Runaway Threads"].List)
            .fail((response: JQueryXHR) => this.reportError(t("loadThreadsRuntimeInfoFailed"), response.responseText, response.statusText));
    }

}

export = getDebugThreadsRunawayCommand;
