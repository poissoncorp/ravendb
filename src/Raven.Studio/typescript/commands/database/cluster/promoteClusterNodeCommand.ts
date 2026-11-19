import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class promoteClusterNodeCommand extends commandBase {

    constructor(private nodeTag: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            nodeTag: this.nodeTag
        };
        const url = endpoints.global.rachisAdmin.adminClusterPromote + this.urlEncodeArgs(args);

        return this.post<void>(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("promoteNodeFailed"), response.responseText));
    }
}

export = promoteClusterNodeCommand;
