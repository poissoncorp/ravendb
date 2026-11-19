import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import KafkaConnectionSettings = Raven.Client.Documents.Operations.ETL.Queue.KafkaConnectionSettings;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

type ConnectionOptionsDto = { [optionKey: string]: string };

export default class testServerWideKafkaServerConnectionCommand extends commandBase {
    constructor(
        private readonly bootstrapServers: string,
        private readonly useServerCertificate: boolean,
        private readonly connectionOptionsDto: ConnectionOptionsDto
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.queueEtlServerWideConnection.adminEtlQueueKafkaTestConnection;

        const payload: KafkaConnectionSettings = {
            BootstrapServers: this.bootstrapServers,
            ConnectionOptions: this.connectionOptionsDto,
            UseRavenCertificate: this.useServerCertificate,
        };

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), undefined, {
            dataType: undefined,
        })
            .fail((response: JQueryXHR) => this.reportError(t("connectionStrings.testFailed.kafka"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("connectionStrings.testFailed.kafka"), result.Error);
                }
            });
    }
}
