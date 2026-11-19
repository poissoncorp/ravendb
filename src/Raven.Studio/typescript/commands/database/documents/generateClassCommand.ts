import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class generateClassCommand extends commandBase {
    
    constructor(private db: database | string, private docId: string, private lang: string) {
        super();
    }

    execute(): JQueryPromise<string> {
        const url = endpoints.databases.document.docsClass;
        const args = {
            id: this.docId,
            lang: this.lang
        };
        return this.query<string>(url, args, this.db, null, { dataType: "text" })
            .fail((response: JQueryXHR) => this.reportError(t("generateClassFailed"),
                response.responseText,
                response.statusText));
    }
}

export = generateClassCommand;
