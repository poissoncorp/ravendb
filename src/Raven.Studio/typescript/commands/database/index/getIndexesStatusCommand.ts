import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexesStatusCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexingStatus> {
        const url = endpoints.databases.index.indexesStatus;
        return this.query<Raven.Client.Documents.Indexes.IndexingStatus>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getStatusFailed"), response.responseText, response.statusText);
            });
    }
}

export = getIndexesStatusCommand;
