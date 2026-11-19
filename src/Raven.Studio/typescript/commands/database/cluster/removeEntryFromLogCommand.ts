import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class removeEntryFromLogCommand extends commandBase {
    
    constructor(private readonly nodeTag: string, private readonly index: number) {
        super();
    }

    execute(): JQueryPromise<void> { 
        const url = endpoints.global.rachisAdmin.adminClusterRemoveEntryFromLog + this.urlEncodeArgs({
            nodeTag: this.nodeTag,
            index: this.index
        });

        return this.post<void>(url, null)
            .done(() => this.reportSuccess(t("removeLogEntrySucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("removeLogEntryFailed"), response.responseText, response.statusText));
    }
}

export = removeEntryFromLogCommand;
