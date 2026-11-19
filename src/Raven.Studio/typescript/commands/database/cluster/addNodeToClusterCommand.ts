import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class addNodeToClusterCommand extends commandBase {

    constructor(private serverUrl: string, private addAsWatcher: boolean, private maxUtilizedCores?: number, private nodeTag?: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            url: this.serverUrl,
            watcher: this.addAsWatcher,
            maxUtilizedCores: this.maxUtilizedCores,
            tag: this.nodeTag,
        };
        const url = endpoints.global.rachisAdmin.adminClusterNode + this.urlEncodeArgs(args);

        return this.put<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess(t("addNodeSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("addNodeFailed"), response.responseText, response.statusText));
    }
}

export = addNodeToClusterCommand;
