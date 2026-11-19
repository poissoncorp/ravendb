import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

type EventListenerConfigurationDto = Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration;

class saveAdminLogsEventListenerConfigurationCommand extends commandBase {
    private readonly config: EventListenerConfigurationDto;

    constructor(config: EventListenerConfigurationDto) {
        super();
        this.config = config;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminLogs.adminEventListenerConfiguration;

        return this.post<void>(url, JSON.stringify(this.config), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("saveEventListenerConfigurationSucceeded")))
            .fail((response: JQueryXHR) =>
                this.reportError(
                    t("saveEventListenerConfigurationFailed"),
                    response.responseText,
                    response.statusText
                )
            );
    }
}

export = saveAdminLogsEventListenerConfigurationCommand;
