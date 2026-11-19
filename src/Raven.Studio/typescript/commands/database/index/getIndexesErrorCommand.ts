import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexesErrorCommand extends commandBase {

    constructor(private db: database | string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const url = endpoints.databases.index.indexesErrors + this.urlEncodeArgs(this.location);

        const locationText = genUtils.formatLocation(this.location);
        
        return this.query<Raven.Client.Documents.Indexes.IndexErrors[]>(url, null, this.db, x => x.Results)
            .fail((result: JQueryXHR) => this.reportError(t("getErrorsFailed", { location: locationText }), result.responseText, result.statusText));
    }
}

export = getIndexesErrorCommand;
