import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class getCustomAnalyzersCommand extends commandBase {

    private readonly db: database | string;
    private readonly getNamesOnly: boolean;

    constructor(db: database | string, getNamesOnly = false) {
        super();
        this.db = db;
        this.getNamesOnly = getNamesOnly;
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>> {
        let url = endpoints.databases.analyzers.analyzers;

        if (this.getNamesOnly) {
            const args = {
                namesOnly: this.getNamesOnly
            };
            
            url += this.urlEncodeArgs(args);
        }

        return this.query<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>>(url, null, this.db, x => x.Analyzers)
            .fail((response: JQueryXHR) => {
                this.reportError(t("getCustomAnalyzersFailed"), response.responseText, response.statusText);
            });
    }
}

export = getCustomAnalyzersCommand;
