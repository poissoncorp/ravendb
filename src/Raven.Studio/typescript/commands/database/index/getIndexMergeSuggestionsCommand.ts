import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");
import IndexMergeResults = Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults;

const t = i18nModule.createTranslator("indexCommands");

class getIndexMergeSuggestionsCommand extends commandBase {

    private readonly db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<IndexMergeResults> {
        const url = endpoints.databases.index.indexesSuggestIndexMerge;
        
        return this.query<IndexMergeResults>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                if (!commandBase.isLicenseLimitException(response)) {
                    this.reportError(t("getMergeSuggestionsFailed"), response.responseText, response.statusText)
                }
            });
    }
}

export = getIndexMergeSuggestionsCommand;
