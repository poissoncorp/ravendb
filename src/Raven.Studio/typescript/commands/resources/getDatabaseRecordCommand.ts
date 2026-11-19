import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import document = require("models/database/documents/document");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class getDatabaseRecordCommand extends commandBase {

    private readonly databaseName: string;
    private readonly reportRefreshProgress: boolean;

    constructor(db: database | string, reportRefreshProgress = false) {
        super();
        this.databaseName = (typeof db === "string" ? db : db.name);
        this.reportRefreshProgress = reportRefreshProgress;
    }

    execute(): JQueryPromise<document> {
        const resultsSelector = (queryResult: queryResultDto<documentDto>) => new document(queryResult);
        const args = {
            name: this.databaseName
        };
        const url = endpoints.global.adminDatabases.adminDatabases + this.urlEncodeArgs(args);

        const getTask = this.query(url, null, null, resultsSelector);

        if (this.reportRefreshProgress) {
            getTask.done(() => this.reportSuccess(t("refreshDatabaseRecordSucceeded", { databaseName: this.databaseName })));
            getTask.fail((response: JQueryXHR) => this.reportError(t("refreshDatabaseRecordFailed"), response.responseText, response.statusText));
        }
        return getTask;
    }
}

export = getDatabaseRecordCommand;
