import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("maintenanceCommands");

type EventListenerConfigurationDto = Omit<Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration, "Persist">;

class getAdminLogsEventListenerConfigurationCommand extends commandBase {
    execute(): JQueryPromise<EventListenerConfigurationDto> {
        const url = endpoints.global.adminLogs.adminEventListenerConfiguration;

        return this.query<EventListenerConfigurationDto>(url, null).fail((response: JQueryXHR) =>
            this.reportError(
                t("getEventListenerConfigurationFailed"),
                response.responseText,
                response.statusText
            )
        );
    }
}

export = getAdminLogsEventListenerConfigurationCommand;
