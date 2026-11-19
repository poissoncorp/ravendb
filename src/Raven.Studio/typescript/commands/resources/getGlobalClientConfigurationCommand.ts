import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getGlobalClientConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.Configuration.ClientConfiguration> {
        const url = endpoints.global.adminConfiguration.configurationClient;
        const loadTask = $.Deferred<Raven.Client.Documents.Operations.Configuration.ClientConfiguration>(); 
        
        this.query<Raven.Client.Documents.Operations.Configuration.ClientConfiguration>(url, null)
            .done(dto => loadTask.resolve(dto))
            .fail((response: JQueryXHR) => {
                if (response.status === 404) {
                    loadTask.resolve(null);
                } else {
                    this.reportError(t("getClientConfigurationFailed"), response.responseText, response.statusText);
                    loadTask.reject(response);
                }
            });
        
        return loadTask;
        
    }
    
}

export = getGlobalClientConfigurationCommand;
