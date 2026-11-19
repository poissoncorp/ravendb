import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getMigratorPathConfigurationCommand extends commandBase {

    execute(): JQueryPromise<MigratorPathConfiguration> {
        const url = endpoints.global.studioTasks.studioTasksAdminMigratorPath;
       
        return this.query<MigratorPathConfiguration>(url, null)
            .fail((response: JQueryXHR) =>
                this.reportError(t("migratorPathFailed"), response.responseText, response.statusText));
    }
}

export = getMigratorPathConfigurationCommand;
