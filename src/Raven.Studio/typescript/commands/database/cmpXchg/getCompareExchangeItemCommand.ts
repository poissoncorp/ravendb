import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("cmpXchgCommands");

class getCompareExchangeItemCommand extends commandBase {

    constructor(private database: database, private key: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeValue<any>> {
        const args = {
            key: this.key
        };

        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, x => x.Results[0])
            .fail((response: JQueryXHR) => this.reportError(t("getItemFailed", { key: this.key }), response.responseText, response.statusText));
    }
}

export = getCompareExchangeItemCommand;
