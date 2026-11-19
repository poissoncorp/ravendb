import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("identitiesCommands");

class getIdentitiesCommand extends commandBase {

    constructor(private db: database | string) {
        super();
    }

    execute(): JQueryPromise<dictionary<number>> {
        const url = endpoints.databases.identityDebug.debugIdentities;
        
        return this.query<dictionary<number>>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("getFailed"), response.responseText, response.statusText));
    }
}

export = getIdentitiesCommand;
