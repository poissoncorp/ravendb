import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import collectionsStats = require("models/database/documents/collectionsStats");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getCollectionsStatsCommand extends commandBase {

    private ownerDb: database | string;

    constructor(ownerDb: database | string) {
        super();
        this.ownerDb = ownerDb;

        if (!this.ownerDb) {
            throw new Error("Must specify a database.");
        }
    }

    execute(): JQueryPromise<collectionsStats> {
        const finalResult = $.Deferred<collectionsStats>();
        this.query<Raven.Client.Documents.Operations.CollectionStatistics>(endpoints.databases.collections.collectionsStats, null, this.ownerDb)
            .done(results => {
                const stats = new collectionsStats(results);
                finalResult.resolve(stats);
            })
            .fail((response) => {
                this.reportError(t("getCollectionsStatsFailed"), response.responseText, response.statusText);
                finalResult.reject(response.responseText);
            });

        return finalResult;
    }
}

export = getCollectionsStatsCommand;
