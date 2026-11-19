import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getDebugMemoryStatsCommand extends commandBase {

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfo> {
        const url = endpoints.global.memoryDebug.adminDebugMemoryStats;
        return this.query<Raven.Server.Documents.Handlers.Debugging.MemoryDebugHandler.MemoryInfo>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("loadMemoryStatsFailed"), response.responseText, response.statusText));
    }

}

export = getDebugMemoryStatsCommand;
