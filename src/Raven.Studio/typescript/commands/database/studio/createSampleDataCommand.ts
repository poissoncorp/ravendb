import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("studioCommands");

class createSampleDataCommand extends commandBase {
    private db: database | string;

    constructor(db: database | string) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<any> {
        return this.post(endpoints.databases.sampleData.studioSampleData, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("sampleDataFailed"), response.responseText, response.statusText))
            .done(() => this.reportSuccess(t("sampleDataCreated")));
    }
}

export = createSampleDataCommand;
