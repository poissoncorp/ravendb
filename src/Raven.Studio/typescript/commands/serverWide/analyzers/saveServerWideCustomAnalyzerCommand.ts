import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class saveServerWideCustomAnalyzerCommand extends commandBase {

    constructor(private analyzerDto: Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminAnalyzers.adminAnalyzers;
        
        const payload = {
            Analyzers: [ this.analyzerDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("analyzers.saveSucceeded", { name: this.analyzerDto.Name })))
            .fail((response: JQueryXHR) => this.reportError(t("analyzers.saveFailed"), response.responseText, response.statusText));
    }
}

export = saveServerWideCustomAnalyzerCommand; 

