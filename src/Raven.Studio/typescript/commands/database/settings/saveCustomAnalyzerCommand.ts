import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class saveCustomAnalyzerCommand extends commandBase {

    constructor(private db: database | string, private analyzerDto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminAnalyzers.adminAnalyzers;
        const payload = {
            Analyzers: [ this.analyzerDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError(t("saveCustomAnalyzerFailed"), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("saveCustomAnalyzerSucceeded", { name: this.analyzerDto.Name }));
            });
    }
}

export = saveCustomAnalyzerCommand; 

