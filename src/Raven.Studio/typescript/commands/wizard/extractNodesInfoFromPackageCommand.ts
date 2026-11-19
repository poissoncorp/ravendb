import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class extractNodesInfoFromPackageCommand extends commandBase {

    constructor(private zipContents: string) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Server.Web.System.ConfigurationNodeInfo>> {
        const url = endpoints.global.setup.setupContinueExtract;
        const payload: Partial<Raven.Server.Commercial.ContinueSetupInfo> = {
            Zip: this.zipContents,
        };

        return this.post<Raven.Server.Web.System.ConfigurationNodeInfo>(url, JSON.stringify(payload))
            .fail((response: JQueryXHR) => this.reportError(t("extractNodesInfoFailed"), response.responseText, response.statusText));
    }
}

export = extractNodesInfoFromPackageCommand;
