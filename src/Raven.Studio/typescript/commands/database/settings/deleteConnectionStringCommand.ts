import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class deleteConnectionStringCommand extends commandBase {
    private readonly db: database | string;
    private readonly type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType;
    private readonly connectionStringName: string;

    constructor(db: database | string, type: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType, connectionStringName: string) {
        super();
        this.db = db;
        this.type = type;
        this.connectionStringName = connectionStringName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            type: this.type,
            connectionString: this.connectionStringName
        };
        
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(t("deleteConnectionStringSucceeded", { name: this.connectionStringName })))
            .fail((response: JQueryXHR) => this.reportError(t("deleteConnectionStringFailed", { name: this.connectionStringName }), response.responseText, response.statusText));
    }
}

export = deleteConnectionStringCommand;
