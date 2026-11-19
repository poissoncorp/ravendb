import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class getServerWideCustomAnalyzersCommand extends commandBase {

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>> {
        const args = {
            namesOnly: false
        };

        const url = endpoints.global.analyzers.analyzers + this.urlEncodeArgs(args);

        return this.query<Array<Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition>>(url, null, null, x => x.Analyzers)
            .fail((response: JQueryXHR) => {
                this.reportError(t("analyzers.getFailed"), response.responseText, response.statusText);
            });
    }
}

export = getServerWideCustomAnalyzersCommand;
