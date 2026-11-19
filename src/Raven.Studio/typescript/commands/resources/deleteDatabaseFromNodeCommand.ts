import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class deleteDatabaseFromNodeCommand extends commandBase {

    private databaseName: string;

    private nodes: Array<string>;

    private isHardDelete: boolean;

    constructor(databaseName: string, nodes: Array<string>, isHardDelete: boolean) {
        super();
        this.isHardDelete = isHardDelete;
        this.nodes = nodes;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        const args = {
            HardDelete: this.isHardDelete,
            DatabaseNames: [this.databaseName],
            FromNodes: this.nodes
        };

        return this.del<updateDatabaseConfigurationsResult>(url, JSON.stringify(args), null)
            .done(() => {
                this.reportSuccess(t("deleteFromNodesSucceeded", { count: this.nodes.length, nodes: this.nodes.join(", ") }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("deleteFromNodesFailed", { nodes: this.nodes.join(", ") }), response.responseText, response.statusText));
    }


} 

export = deleteDatabaseFromNodeCommand;
