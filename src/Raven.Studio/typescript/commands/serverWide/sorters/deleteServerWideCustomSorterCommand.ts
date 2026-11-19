import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

class deleteServerWideCustomSorterCommand extends commandBase {
    private readonly name: string;
    
    constructor(name: string) {
        super();
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.global.adminSorters.adminSorters + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null)
            .done(() => this.reportSuccess(t("sorters.deleteSucceeded", { name: this.name })))
            .fail((response: JQueryXHR) => this.reportError(t("sorters.deleteFailed", { name: this.name }), response.responseText, response.statusText));
    }
}

export = deleteServerWideCustomSorterCommand;
