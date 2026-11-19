import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class testQueueSinkCommand extends commandBase {
    constructor(private db: database | string, private payload: Raven.Server.Documents.QueueSink.Test.TestQueueSinkScript,
                private brokerType: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.QueueSink.Test.TestQueueSinkScriptResult> {
        const url = endpoints.databases.queueSink.adminQueueSinkTest;

        return this.post<Raven.Server.Documents.QueueSink.Test.TestQueueSinkScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("queueSinkTestFailed", { brokerType: this.brokerType }), response.responseText, response.statusText);
            });
    }
}

export = testQueueSinkCommand;
