import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("dbGroupCommands");

class addNodeToDatabaseGroupCommand extends commandBase {

    private databaseName: string;

    private nodeTagToAdd: string;

    private mentorNode: string = undefined;

    constructor(databaseName: string, nodeTagToAdd: string, mentorNode: string = undefined) {
        super();
        this.mentorNode = mentorNode;
        this.nodeTagToAdd = nodeTagToAdd;
        this.databaseName = databaseName;
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.DatabasePutResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd,
            mentor: this.mentorNode
        };
        const url = endpoints.global.adminDatabases.adminDatabasesNode + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Operations.DatabasePutResult>(url, null)
            .done(() => this.reportSuccess(t("addNodeSucceeded", { nodeTag: this.nodeTagToAdd })))
            .fail((response: JQueryXHR) => this.reportError(t("addNodeFailed"), response.responseText, response.statusText));
    }
}

export = addNodeToDatabaseGroupCommand;
