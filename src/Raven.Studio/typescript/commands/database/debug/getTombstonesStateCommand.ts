import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getTombstonesStateCommand extends commandBase {

    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;

    constructor(db: database | string, location: databaseLocationSpecifier) {
        super();
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<TombstonesStateOnWire> {
        const url = endpoints.databases.adminTombstone.adminTombstonesState;
        const args = this.location;
        return this.query<TombstonesStateOnWire>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("loadTombstonesStateFailed"), response.responseText, response.statusText));
    }
}

export = getTombstonesStateCommand;
