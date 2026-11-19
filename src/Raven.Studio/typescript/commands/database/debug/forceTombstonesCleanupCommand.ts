import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class forceTombstonesCleanupCommand extends commandBase {

    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;

    constructor(db: database | string, location: databaseLocationSpecifier) {
        super();
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<number> {
        const url = endpoints.databases.adminTombstone.adminTombstonesCleanup + this.urlEncodeArgs(this.location)
        return this.post(url, null, this.db)
            .done((result: { Value: number }) => {
                if (result.Value === 0) {
                    this.reportSuccess(t("tombstonesCleanup.nothingToCleanup"));
                } else {
                    this.reportSuccess(
                        t("tombstonesCleanup.success", {
                            count: result.Value,
                            formattedCount: result.Value.toLocaleString(),
                        })
                    );
                }
            })
            .fail((response: JQueryXHR) => this.reportError(t("tombstonesCleanup.failed"), response.responseText, response.statusText));
    }
}

export = forceTombstonesCleanupCommand;
