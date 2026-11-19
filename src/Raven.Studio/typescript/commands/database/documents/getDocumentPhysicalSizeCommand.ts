import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class getDocumentPhysicalSizeCommand extends commandBase {

    constructor(private id: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Commands.DocumentSizeDetails> {

        const args = {
            id: this.id
        };
        
        const url = endpoints.databases.document.docsSize + this.urlEncodeArgs(args);

        return this.query<Raven.Client.Documents.Commands.DocumentSizeDetails>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getDocumentPhysicalSizeFailed"), response.responseText, response.statusText));
    }
}
 
export = getDocumentPhysicalSizeCommand;
