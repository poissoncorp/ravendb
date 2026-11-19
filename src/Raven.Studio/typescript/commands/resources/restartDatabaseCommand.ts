import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class restartDatabaseCommand extends commandBase {
    private databaseName: string;
    private location: databaseLocationSpecifier;

    constructor(databaseName: string, location: databaseLocationSpecifier) {
        super();
        this.databaseName = databaseName;
        this.location = location;
    }

    execute(): JQueryPromise<statusDto<disableDatabaseResult>> {
        const url = endpoints.databases.studioDatabaseTasks.adminStudioTasksRestart + this.urlEncodeArgs(this.location);

        return this.post(url, null, this.databaseName, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("restartFailed"), response.responseText, response.statusText));
    }
}

export = restartDatabaseCommand;  
