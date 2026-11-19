import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexesErrorCountCommand extends commandBase {

    constructor(private db: database | string, private location: databaseLocationSpecifier) {
        super();
    }
    
    execute(): JQueryPromise<{Results: indexErrorsCount[]}> {
        const args = this.location;
        const url = endpoints.databases.studioIndex.studioIndexesErrorsCount;

        const locationText = genUtils.formatLocation(this.location);
            
        return this.query<{Results: indexErrorsCount[]}>(url, args, this.db)
            .fail((result: JQueryXHR) => this.reportError(t("getErrorsCountFailed", { location: locationText }),
                result.responseText, result.statusText));
    }
}

export = getIndexesErrorCountCommand;
