import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class saveTrafficWatchConfigurationCommand extends commandBase {
    
    private configuration: Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters;
    
    constructor(configuration: Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters) {
        super();
        
        this.configuration = configuration;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.trafficWatch.adminTrafficWatchConfiguration;

        return this.post<void>(url, JSON.stringify(this.configuration), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("saveTrafficWatchConfigurationSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("saveTrafficWatchConfigurationFailed"), response.responseText, response.statusText));
    }
}

export = saveTrafficWatchConfigurationCommand;
