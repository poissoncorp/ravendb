import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class replicationProgressCommand extends commandBase {

    private readonly db: database | string;
    private readonly location: databaseLocationSpecifier;
    private readonly reportFailure: boolean;

    constructor(db: database | string, location: databaseLocationSpecifier, reportFailure = true) {
        super();
        this.reportFailure = reportFailure;
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<resultsDto<ReplicationTaskProgress>> {
        const url = endpoints.databases.replication.replicationProgress;
        const args = this.location;

        return this.query<resultsDto<ReplicationTaskProgress>>(url, args, this.db, null, { timeout: commandBase.perNodeRequestTimeoutMs })
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(t("replicationProgressFailed"), response.responseText);    
                }
            });
    }
}

export = replicationProgressCommand; 
