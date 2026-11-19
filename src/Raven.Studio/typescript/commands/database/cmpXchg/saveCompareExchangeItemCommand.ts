import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("cmpXchgCommands");

class saveCompareExchangeItemCommand extends commandBase {

    constructor(private database: database, private key: string, private index: number,
                private valueData: any, private metadata: any) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>> {
        const args = {
            key: this.key,
            index: this.index
        };
        
        const payload: any = {
            Object: this.valueData
        };
        
        if (this.metadata !== undefined) {
            payload["@metadata"] = this.metadata;
        }
        
        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        
        return this.put<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>>(url, JSON.stringify(payload), this.database)
            .fail((response: JQueryXHR) => this.reportError(t("saveFailed"), response.responseText, response.statusText));
    }
}

export = saveCompareExchangeItemCommand;
