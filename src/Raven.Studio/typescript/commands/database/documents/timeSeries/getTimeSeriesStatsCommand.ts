import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getTimeSeriesStatsCommand extends commandBase {
    
    constructor(private docId: string, private db: database | string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesStatistics> {
        const args = {
            docId: this.docId
        };
        
        const url = endpoints.databases.timeSeries.timeseriesStats;
        
        return this.query<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesStatistics>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((result: JQueryXHR) => this.reportError(t("timeSeries.getStatsFailed"), result.responseText, result.statusText));
    }
}

export = getTimeSeriesStatsCommand;
