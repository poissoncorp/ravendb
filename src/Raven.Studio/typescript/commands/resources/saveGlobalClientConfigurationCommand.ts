import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;

class saveGlobalClientConfigurationCommand extends commandBase {
    private dto: ClientConfiguration;

    constructor(dto: ClientConfiguration) {
        super();
        this.dto = dto;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminConfiguration.adminConfigurationClient;
        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined})
            .fail((response: JQueryXHR) => this.reportError(t("saveClientConfigurationFailed"), response.responseText, response.statusText)) 
            .done(() => this.reportSuccess(t("saveGlobalClientConfigurationSucceeded")));
    }
}

export = saveGlobalClientConfigurationCommand;
