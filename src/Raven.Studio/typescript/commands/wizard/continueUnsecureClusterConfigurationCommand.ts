import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class continueUnsecureClusterConfigurationCommand extends commandBase {

    constructor(private operationId: number, private dto: Raven.Server.Commercial.ContinueSetupInfo) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            operationId: this.operationId
        }
        const url = endpoints.global.setup.setupContinueUnsecured + this.urlEncodeArgs(args);

        return this.post<operationIdDto>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("configureClusterNodeFailed"), response.responseText, response.statusText));
    }
}

export = continueUnsecureClusterConfigurationCommand;
