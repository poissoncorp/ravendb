import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class saveServerWideCustomSorterCommand extends commandBase {
    
    constructor(private sorterDto: Raven.Client.Documents.Queries.Sorting.SorterDefinition) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminSorters.adminSorters;
        
        const payload = {
            Sorters: [ this.sorterDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("sorters.saveSucceeded", { name: this.sorterDto.Name })))
            .fail((response: JQueryXHR) => this.reportError(t("sorters.saveFailed"), response.responseText, response.statusText));
    }
}

export = saveServerWideCustomSorterCommand;
