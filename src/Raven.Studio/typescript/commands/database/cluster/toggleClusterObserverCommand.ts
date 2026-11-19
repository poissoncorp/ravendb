import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

class toggleClusterObserverCommand extends commandBase {

    constructor(private suspend: boolean) {
        super();
    }

    execute(): JQueryPromise<void> {
        
        const args = {
            value: this.suspend
        };
        const basicUrl = endpoints.global.rachisAdmin.adminClusterObserverSuspend + this.urlEncodeArgs(args);

        return this.post(basicUrl, null, null, {  dataType: undefined })
            .done(() => {
                const actionPerformed = this.suspend ? "suspended" : "resumed";
                this.reportSuccess(t("observerToggled", { context: actionPerformed }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("toggleObserverFailed"), response.responseText));
    }
}

export = toggleClusterObserverCommand;
