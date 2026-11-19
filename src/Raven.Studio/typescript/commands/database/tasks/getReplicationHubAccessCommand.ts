import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getReplicationHubAccessCommand extends commandBase {

    constructor(private db: database | string, private hubTaskName: string) {
          super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Replication.ReplicationHubAccessResult> {
        const args = {
            name: this.hubTaskName
        };
        
        const url = endpoints.databases.pullReplication.adminTasksPullReplicationHubAccess + this.urlEncodeArgs(args);
        
        return this.query<Raven.Client.Documents.Operations.Replication.ReplicationHubAccessResult>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("hubAccessFailed", { hubTaskName: this.hubTaskName }), response.responseText, response.statusText);
            });
    }
}

export = getReplicationHubAccessCommand; 
