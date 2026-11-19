import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("dbGroupCommands");

class addNodeToShardCommand extends commandBase {

    constructor(private databaseName: string, private nodeTagToAdd: string, private mentorNode: string = undefined) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult> {
        const args = {
            name: this.databaseName,
            node: this.nodeTagToAdd,
            mentor: this.mentorNode
        };
        const url = endpoints.global.adminDatabases.adminDatabasesNode + this.urlEncodeArgs(args);

        return this.put<Raven.Client.ServerWide.Sharding.AddDatabaseShardResult>(url, null)
            .done(() => this.reportSuccess(t("addNodeToShardSucceeded", { nodeTag: this.nodeTagToAdd, databaseName: this.databaseName })))
            .fail((response: JQueryXHR) => this.reportError(t("addNodeToShardFailed"), response.responseText, response.statusText));
    }
}

export = addNodeToShardCommand;
