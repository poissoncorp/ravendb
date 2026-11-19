import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

class getEnvironmentStorageReportCommand extends commandBase {

    constructor(private db: database | string, private location: databaseLocationSpecifier, private name: string, private type: string, private detailed = false) {
        super();
    }

    execute(): JQueryPromise<detailedStorageReportItemDto> {
        const args = {
            name: this.name,
            type: this.type,
            details: this.detailed,
            ...this.location
        };
        const url = endpoints.databases.storage.debugStorageEnvironmentReport + this.urlEncodeArgs(args);
        
        return this.query<detailedStorageReportItemDto>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("loadDetailedStorageReportFailed"), response.responseText, response.statusText));
    }
}

export = getEnvironmentStorageReportCommand;
