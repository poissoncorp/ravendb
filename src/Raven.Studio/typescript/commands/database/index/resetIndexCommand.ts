import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");
import i18nModule = require("common/i18n/i18n");

type IndexResetMode = Raven.Client.Documents.Indexes.IndexResetMode;

const t = i18nModule.createTranslator("indexCommands");

class resetIndexCommand extends commandBase {

    private readonly indexName: string;
    private readonly databaseName: string;
    private readonly mode: IndexResetMode;
    private readonly location: databaseLocationSpecifier;

    constructor(indexName: string, databaseName: string, mode: IndexResetMode, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.databaseName = databaseName;
        this.mode = mode;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<{ IndexId: number }> {
        const args = {
            name: this.indexName,
            mode: this.mode,
            ...this.location
        };
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);

        const locationText = genUtils.formatLocation(this.location);

        return this.reset<{ IndexId: number }>(url, null, this.databaseName)
            .fail((response: JQueryXHR) => this.reportError(t("resetFailed", { indexName: this.indexName, location: locationText }), response.responseText, response.statusText));
    }
}

export = resetIndexCommand;
