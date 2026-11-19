import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class demoteClusterNodeCommand extends commandBase {

    constructor(private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag
        };
    
        const url = endpoints.global.rachisAdmin.adminClusterDemote + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("demoteNodeFailed"), response.responseText));
    }
}

export = demoteClusterNodeCommand;
