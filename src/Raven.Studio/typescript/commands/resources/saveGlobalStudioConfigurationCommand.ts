import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class saveGlobalStudioConfigurationCommand extends commandBase {
    
    constructor(private dto: Raven.Client.ServerWide.Operations.Configuration.ServerWideStudioConfiguration) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminConfiguration.adminConfigurationStudio;
        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(t("saveGlobalStudioConfigurationFailed"), response.responseText, response.statusText)) 
            .done(() => this.reportSuccess(t("saveGlobalStudioConfigurationSucceeded")));
    }
}

export = saveGlobalStudioConfigurationCommand;
