import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class saveTimeSeriesConfigurationCommand extends commandBase {
    constructor(private db: database | string, private configuration: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoint.databases.timeSeries.adminTimeseriesConfig;
        const args = ko.toJSON(this.configuration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("timeSeries.saveConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveTimeSeriesConfigurationCommand;
