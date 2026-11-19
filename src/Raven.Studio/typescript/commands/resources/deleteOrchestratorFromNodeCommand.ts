import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class deleteOrchestratorFromNodeCommand extends commandBase {

    private databaseName: string;

    private node: string;

    constructor(databaseName: string, node: string) {
        super();
        this.node = node;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.shardedAdminDatabase.adminDatabasesOrchestrator;
        const args = {
            name: [this.databaseName],
            node: this.node
        };

        return this.del<updateDatabaseConfigurationsResult>(url + this.urlEncodeArgs(args), null)
            .done(() => {
                this.reportSuccess(t("deleteOrchestratorSucceeded", { nodeTag: this.node }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("deleteOrchestratorFailed", { nodeTag: this.node }), response.responseText, response.statusText));
    }


} 

export = deleteOrchestratorFromNodeCommand;
