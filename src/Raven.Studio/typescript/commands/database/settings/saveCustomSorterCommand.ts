import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsCommands");

type SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;

class saveCustomSorterCommand extends commandBase {
    private db: database | string;
    private sorterDto: SorterDefinition;

    constructor(db: database | string, sorterDto: SorterDefinition) {
        super();
        this.db = db;
        this.sorterDto = sorterDto;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.adminSorters.adminSorters;
        const payload = {
            Sorters: [ this.sorterDto ]
        };

        return this.put<void>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                this.reportError(t("saveCustomSorterFailed"), response.responseText, response.statusText); 
            })
            .done(() => {
                this.reportSuccess(t("saveCustomSorterSucceeded", { name: this.sorterDto.Name }));
            });
    }
}

export = saveCustomSorterCommand; 

