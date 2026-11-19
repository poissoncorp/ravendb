import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveUnusedDatabaseIDsCommand extends commandBase {

    constructor(private unusedDatabaseIDs: string[], private dbName: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.dbName,
            validate: true
        }

        const url = endpoints.global.adminDatabases.adminDatabasesUnusedIds + this.urlEncodeArgs(args);

        const payload = {
            DatabaseIds: this.unusedDatabaseIDs
        };

        return this.post(url, JSON.stringify(payload))
            .done(() => this.reportSuccess(t("saveUnusedDatabaseIdsSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("saveUnusedDatabaseIdsFailed"), response.responseText));
    }
}

export = saveUnusedDatabaseIDsCommand;
