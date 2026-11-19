import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

type RequestDto = Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters>;

class saveAdminLogsConfigurationCommand extends commandBase {
    private readonly dto: RequestDto; 

    constructor(dto: RequestDto) {
        super();
        this.dto = dto;
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminLogsConfiguration;

        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("saveAdminLogsConfigurationSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("saveAdminLogsConfigurationFailed"), response.responseText, response.statusText));
    }
}

export = saveAdminLogsConfigurationCommand;
