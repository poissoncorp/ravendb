import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class forceLeaderTimeoutCommand extends commandBase {

    constructor(private targetNode: string) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterTimeout;

        return this.post<void>(url, null, null, { dataType: undefined }, 9000, this.targetNode)
            .done(() => this.reportSuccess(t("forceTimeoutSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("forceTimeoutFailed"), response.responseText));
    }
}

export = forceLeaderTimeoutCommand;
