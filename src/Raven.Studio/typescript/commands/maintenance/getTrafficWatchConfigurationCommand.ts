import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

type TrafficWatchConfiguration = Omit<Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters, "Persist">;

class getTrafficWatchConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<TrafficWatchConfiguration> {
        const url = endpoints.global.trafficWatch.adminTrafficWatchConfiguration;
        
        return this.query<TrafficWatchConfiguration>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getTrafficWatchConfigurationFailed"), response.responseText, response.statusText)) 
    }
}

export = getTrafficWatchConfigurationCommand;
