import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class forceIndexReplace extends commandBase {

    private readonly indexName: string;
    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;

    constructor(indexName: string, db: database | string, location: databaseLocationSpecifier) {
        super();
        this.indexName = indexName;
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            ...this.location
        }
        const url = endpoints.databases.index.indexesReplace + this.urlEncodeArgs(args);

        const locationText = genUtils.formatLocation(this.location);

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("replaceFailed", { indexName: this.indexName, location: locationText }), response.responseText, response.statusText));
    }
}

export = forceIndexReplace; 
