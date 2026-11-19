import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class deleteCollectionCommand extends commandBase {
    private displayCollectionName: string;

    constructor(private collectionName: string, private db: database | string, private excludedIds: Array<string>) {
        super();

        this.displayCollectionName = (collectionName === "*") ? "All Documents" : collectionName;
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = { name: this.collectionName };
        const url = endpoints.databases.studioCollections.studioCollectionsDocs + this.urlEncodeArgs(args);
        const payload = { ExcludeIds: this.excludedIds };

        return this.del<operationIdDto>(url, JSON.stringify(payload), this.db)            
            .fail((response: JQueryXHR) => {
                this.reportError(t("deleteCollectionFailed", { collectionName: this.collectionName }), response.responseText, response.statusText);
            });
    }
}

export = deleteCollectionCommand; 
