import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class saveConflictSolverConfigurationCommand extends commandBase {
    private readonly databaseName: string;
    private readonly configuration: Raven.Client.ServerWide.ConflictSolver;

    constructor(db: database | string, configuration: Raven.Client.ServerWide.ConflictSolver) {
        super();
        this.databaseName = typeof db === "string" ? db : db.name;
        this.configuration = configuration;
    }

    execute(): JQueryPromise<updateConflictSolverConfigurationResponse> {
        const urlArgs = {
            name: this.databaseName
        };
        const url = endpoints.global.adminDatabases.adminReplicationConflictsSolver + this.urlEncodeArgs(urlArgs);
        const args = ko.toJSON(this.configuration);
        return this.post<updateConflictSolverConfigurationResponse>(url, args)
            .done(() => this.reportSuccess(t("conflictSolverConfigurationSaved")))
            .fail((response: JQueryXHR) => this.reportError(t("saveConflictSolverConfigurationFailed"), response.responseText, response.statusText));

    }
}

export = saveConflictSolverConfigurationCommand;
