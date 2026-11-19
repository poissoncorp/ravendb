import commandBase = require("commands/commandBase");
import database = require("models/resources/database")
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("documentsCommands");

class setCounterCommand extends commandBase {

    constructor(private counterName: string, private deltaValue: number, private documentId: string, private db: database | string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Counters.CountersDetail> {
        const payload: Omit<Raven.Client.Documents.Operations.Counters.CounterBatch, "FromEtl"> = {
            ReplyWithAllNodesValues: true,
            Documents:
                [{
                    DocumentId: this.documentId,
                    Operations:
                        [{
                            Type: 'Increment',
                            CounterName: this.counterName,
                            Delta: this.deltaValue
                        }]
                }]
        };

        const url = endpoints.databases.counters.counters;

        return this.post<Raven.Client.Documents.Operations.Counters.CountersDetail>(url, JSON.stringify(payload), this.db, {dataType: undefined})
            .done(() => {
                this.reportSuccess(t("counters.set", { counterName: this.counterName }));
            })
            .fail((response: JQueryXHR) => this.reportError(t("counters.setFailed"), response.responseText, response.statusText));
    }
}

export = setCounterCommand;
