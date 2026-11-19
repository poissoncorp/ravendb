import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import KafkaConnectionSettings = Raven.Client.Documents.Operations.ETL.Queue.KafkaConnectionSettings;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterCommands");

type ConnectionOptionsDto = {[optionKey: string]: string};

class testKafkaServerConnectionCommand extends commandBase {
    private readonly db: database | string;
    private readonly bootstrapServers: string;
    private readonly useServerCertificate: boolean;
    private readonly connectionOptionsDto: ConnectionOptionsDto;

    constructor(db: database | string, bootstrapServers: string, useServerCertificate: boolean, connectionOptionsDto: ConnectionOptionsDto) {
        super();
        this.db = db;
        this.bootstrapServers = bootstrapServers;
        this.useServerCertificate = useServerCertificate;
        this.connectionOptionsDto = connectionOptionsDto;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {

        const url = endpoints.databases.queueEtlConnection.adminEtlQueueKafkaTestConnection

        const payload: KafkaConnectionSettings = {
            BootstrapServers: this.bootstrapServers,
            ConnectionOptions: this.connectionOptionsDto,
            UseRavenCertificate: this.useServerCertificate
        }

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("testFailed.kafka"), response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("testFailed.kafka"), result.Error);
                }
            });
    }
}

export = testKafkaServerConnectionCommand;
