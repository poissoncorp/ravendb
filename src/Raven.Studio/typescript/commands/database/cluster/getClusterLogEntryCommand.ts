import endpoints = require("endpoints");
import commandBase = require("commands/commandBase");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class getClusterLogEntryCommand extends commandBase {
    
    constructor(private readonly nodeTag: string, private readonly index: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry> { 
        const url = endpoints.global.rachisAdmin.adminClusterLogEntry + this.urlEncodeArgs({
            index: this.index,
            nodeTag: this.nodeTag,
        })

        return this.query<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getLogEntryFailed"), response.responseText, response.statusText));
    }
}

export = getClusterLogEntryCommand;
