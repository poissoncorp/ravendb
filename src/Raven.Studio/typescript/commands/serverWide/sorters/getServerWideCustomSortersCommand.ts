import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class getServerWideCustomSortersCommand extends commandBase {

    execute(): JQueryPromise<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>> {
        const url = endpoints.global.sorters.sorters;
        
        return this.query<Array<Raven.Client.Documents.Queries.Sorting.SorterDefinition>>(url, null, null, x => x.Sorters)
            .fail((response: JQueryXHR) => {
                this.reportError(t("sorters.getFailed"), response.responseText, response.statusText); 
            });
    }
}

export = getServerWideCustomSortersCommand;
