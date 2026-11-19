import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

class getAdminLogsConfigurationCommand extends commandBase {
    
    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult> {
        const url = endpoints.global.adminLogs.adminLogsConfiguration;
        
        return this.query<Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getAdminLogsConfigurationFailed"), response.responseText, response.statusText)) 
    }
}

export = getAdminLogsConfigurationCommand;
