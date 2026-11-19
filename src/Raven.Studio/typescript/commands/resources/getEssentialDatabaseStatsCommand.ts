import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getEssentialDatabaseStatsCommand extends commandBase {

    private db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.EssentialDatabaseStatistics> {
        const url = endpoints.databases.stats.statsEssential;
        return this.query<Raven.Client.Documents.Operations.EssentialDatabaseStatistics>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getEssentialDatabaseStatsFailed"), response.responseText, response.statusText));
    }
}

export = getEssentialDatabaseStatsCommand;
