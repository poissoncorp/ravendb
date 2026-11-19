import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getStorageReportCommand extends commandBase {

    constructor(private db: database | string, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<storageReportDto> {
        const url = endpoints.databases.storage.debugStorageReport;
        const args = this.location;
        return this.query<storageReportDto>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("loadStorageReportFailed"), response.responseText, response.statusText));
    }
}

export = getStorageReportCommand;
