import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getTimeSeriesConfigurationCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration> {

        const deferred = $.Deferred<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration>();
        const url = endpoints.databases.timeSeries.timeseriesConfig;
        this.query(url, null, this.db)
            .done((config: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) => deferred.resolve(config))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    deferred.resolve(null);
                } else {
                    deferred.reject(xhr);
                    this.reportError(t("timeSeries.getConfigurationFailed"), xhr.responseText, xhr.statusText);
                }
                
            });

        return deferred;
    }
}

export = getTimeSeriesConfigurationCommand;
