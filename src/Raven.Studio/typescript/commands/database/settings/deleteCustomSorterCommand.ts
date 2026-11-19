import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

class deleteCustomSorterCommand extends commandBase {
    private readonly db: database | string;
    private readonly name: string;

    constructor(db: database | string, name: string) {
        super();
        this.db = db;
        this.name = name;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.name
        };
        
        const url = endpoints.databases.adminSorters.adminSorters + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null, this.db)
            .done(() => {
                this.reportSuccess(t("deleteCustomSorterSucceeded", { name: this.name }));
            })
            .fail((response: JQueryXHR) => {
                this.reportError(t("deleteCustomSorterFailed", { name: this.name }), response.responseText, response.statusText); 
            });
    }
}

export = deleteCustomSorterCommand; 

