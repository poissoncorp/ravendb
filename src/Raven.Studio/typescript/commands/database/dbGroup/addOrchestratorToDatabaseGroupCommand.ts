import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("dbGroupCommands");

class addOrchestratorToDatabaseGroupCommand extends commandBase {

    private databaseName: string;

    private nodeTagToAdd: string;

    constructor(databaseName: string, nodeTagToAdd: string) {
        super();
        this.nodeTagToAdd = nodeTagToAdd;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd,
        };
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesOrchestrator + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, null)
            .done(() => this.reportSuccess(t("addOrchestratorSucceeded", { nodeTag: this.nodeTagToAdd, databaseName: this.databaseName })))
            .fail((response: JQueryXHR) => this.reportError(t("addOrchestratorFailed"), response.responseText, response.statusText));
    }
}

export = addOrchestratorToDatabaseGroupCommand;
