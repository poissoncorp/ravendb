import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("identitiesCommands");

class seedIdentityCommand extends commandBase {

    constructor(private db: database | string, private prefix: string, private value: number) {
        super();
    }

    execute(): JQueryPromise<dictionary<number>> {
        const args = {
            name: this.prefix,
            value: this.value,
            force: true
        };
        
        const url = endpoints.databases.identity.identitySeed + this.urlEncodeArgs(args);

        return this.post(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(t("seedFailed"), response.responseText, response.statusText));
    }
}

export = seedIdentityCommand;
