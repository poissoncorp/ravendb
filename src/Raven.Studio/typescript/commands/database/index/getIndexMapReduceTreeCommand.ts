import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("indexCommands");

class getIndexMapReduceTreeCommand extends commandBase {

    constructor(private db: database | string, private location: databaseLocationSpecifier, private indexName: string, private documentIds: Array<string>) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.Debugging.ReduceTree[]> {
        const url = endpoints.databases.index.indexesDebug;
        const args =
        {
            docId: this.documentIds,
            name: this.indexName,
            op: "map-reduce-tree",
            ...this.location
        };
        return this.query(url + this.urlEncodeArgs(args), null, this.db, x => x.Results)
            .fail((response: JQueryXHR) => this.reportError(t("loadMapReduceTreeFailed"), response.responseText, response.statusText))
            .done((trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) => {
                if (!trees.length) {
                    const documents = this.documentIds.map(x => "'" + x + "'").join(",");
                    this.reportWarning(t("noMapReduceResults", { documents }));
                }
            })
    }
} 

export = getIndexMapReduceTreeCommand;
