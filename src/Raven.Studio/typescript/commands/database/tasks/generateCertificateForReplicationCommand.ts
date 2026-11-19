import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class generateCertificateForReplicationCommand extends commandBase {

    constructor(private db: database | string, private expirationInMonths: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate> {
        const args = {
            validMonths: this.expirationInMonths
        };
        
        const url = endpoints.databases.pullReplication.adminPullReplicationGenerateCertificate + this.urlEncodeArgs(args);

        return this.post<Raven.Server.Documents.Handlers.PullReplicationHandler.PullReplicationCertificate>(url, null, this.db, undefined, 20000)
            .fail((response: JQueryXHR) => {
                this.reportError(t("replicationCertificateFailed"), response.responseText, response.statusText);
            });
    }
}

export = generateCertificateForReplicationCommand;
