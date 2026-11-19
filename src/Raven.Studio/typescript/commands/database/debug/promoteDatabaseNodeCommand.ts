import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("debugCommands");

type PromoteImmediatelyResultDto = Pick<
    Raven.Client.ServerWide.Operations.DatabasePutResult,
    "Name" | "RaftCommandIndex"
>;

class promoteDatabaseNodeCommand extends commandBase {
    private readonly databaseName: string;
    private readonly nodeTag: string;

    constructor(databaseName: string, nodeTag: string) {
        super();
        this.databaseName = databaseName;
        this.nodeTag = nodeTag;
    }

    execute(): JQueryPromise<PromoteImmediatelyResultDto> {
        const args = {
            name: this.databaseName,
            node: this.nodeTag,
        };

        const url = endpoints.global.adminDatabases.adminDatabasesPromote + this.urlEncodeArgs(args);

        return this.post<PromoteImmediatelyResultDto>(url, null, null).fail((response: JQueryXHR) =>
            this.reportError(t("promoteNodeFailed", { nodeTag: this.nodeTag }), response.responseText, response.statusText)
        );
    }
}

export = promoteDatabaseNodeCommand;
