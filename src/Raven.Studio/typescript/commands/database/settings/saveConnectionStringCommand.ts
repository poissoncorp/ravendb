import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import connectionStringsTypes = require("components/pages/database/settings/connectionStrings/connectionStringsTypes");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveConnectionStringCommand extends commandBase {
    private readonly db: database | string;
    private readonly connectionString: connectionStringsTypes.ConnectionStringDto;

    constructor(db: database | string, connectionString: connectionStringsTypes.ConnectionStringDto) {
        super();
        this.db = db;
        this.connectionString = connectionString;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings;

        return this.put<void>(url, JSON.stringify(this.connectionString), this.db)
            .fail((response: JQueryXHR) =>
                this.reportError(t("saveConnectionStringFailed"), response.responseText, response.statusText)
            )
            .done(() => this.reportSuccess(t("saveConnectionStringSucceeded")));
    }
}

export = saveConnectionStringCommand;
