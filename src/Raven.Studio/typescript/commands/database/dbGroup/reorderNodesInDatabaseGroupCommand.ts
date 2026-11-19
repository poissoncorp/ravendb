import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("dbGroupCommands");

class reorderNodesInDatabaseGroupCommand extends commandBase {

    private readonly databaseName: string;
    private readonly nodesOrder: string[];
    private readonly fixedTopology: boolean;

    constructor(databaseName: string, nodesOrder: string[], fixedTopology: boolean) {
        super();
        this.fixedTopology = fixedTopology;
        this.nodesOrder = nodesOrder;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.databaseName,
        };
        const url = endpoints.global.adminDatabases.adminDatabasesReorder + this.urlEncodeArgs(args);

        const payload: Raven.Client.ServerWide.Operations.ReorderDatabaseMembersOperation.Parameters = {
            MembersOrder: this.nodesOrder,
            Fixed: this.fixedTopology
        };
        
        return this.post<void>(url, JSON.stringify(payload), null,  { dataType: undefined })
            .done(() => this.reportSuccess(t("reorderNodesSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("reorderNodesFailed"), response.responseText, response.statusText));
    }
}

export = reorderNodesInDatabaseGroupCommand;
