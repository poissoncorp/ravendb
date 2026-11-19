import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getBackupLocationCommand extends commandBase {

    private readonly inputPath: string;
    private readonly database: database | string;

    constructor( inputPath: string,  database: database | string) {
        super();
        this.inputPath = inputPath;
        this.database = database;
    }

    execute(): JQueryPromise<Raven.Server.Web.Studio.DataDirectoryResult> {
        const args = {
            path: this.inputPath,
            getNodesInfo: true
        };

        const url = endpoints.databases.ongoingTasks.adminBackupDataDirectory + this.urlEncodeArgs(args);

        return this.query<Raven.Server.Web.Studio.DataDirectoryResult>(url, null, this.database)
            .fail((response: JQueryXHR) =>
                this.reportError(t("backupLocationFailed"), response.responseText, response.statusText));
    }
}

export = getBackupLocationCommand;
