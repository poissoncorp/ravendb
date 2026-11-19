import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class bootstrapClusterCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.rachisAdmin.adminClusterBootstrap;

        return this.post<void>(url, null, null, { dataType: undefined })
            .done(() => this.reportSuccess(t("bootstrapSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("bootstrapFailed"), response.responseText, response.statusText));
    }
}

export = bootstrapClusterCommand;
