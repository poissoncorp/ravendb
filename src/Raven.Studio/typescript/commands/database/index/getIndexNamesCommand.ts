import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexNamesCommand extends commandBase {

    private db: database | string;

    private readonly location: databaseLocationSpecifier = null;

    constructor(db: database | string, location: databaseLocationSpecifier = null) {
        super();
        this.location = location;
        this.db = db;
        // TODO location param should not be optional
    }

    execute(): JQueryPromise<string[]> {
        const args = {
            namesOnly: true,
            pageSize: 1024,
            ...this.location
        };
        
        const url = endpoints.databases.index.indexes;
        return this.query(url, args, this.db, (x: resultsDto<string>) => x.Results)
            .fail((response: JQueryXHR) => this.reportError(t("getNamesFailed"), response.responseText, response.statusText));
    }
} 

export = getIndexNamesCommand;
