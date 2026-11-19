import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getDatabasesForStudioCommand extends commandBase {

    execute(): JQueryPromise<StudioDatabasesResponse> {
        const url = endpoints.global.studioDatabases.studioTasksDatabases;

        return this.query<StudioDatabasesResponse>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getDatabasesFailed"), response.responseText, response.statusText));
    }
}

export = getDatabasesForStudioCommand;
