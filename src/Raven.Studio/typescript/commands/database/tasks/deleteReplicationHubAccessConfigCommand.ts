import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class deleteReplicationHubAccessConfigCommand extends commandBase {
    
    constructor(private db: database | string, private hubTaskName: string, private thumbprint: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = { 
            name: this.hubTaskName,
            thumbprint: this.thumbprint
        };

        const url = endpoints.databases.pullReplication.adminTasksPullReplicationHubAccess + this.urlEncodeArgs(args);
        
        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(t("hubAccessDeleted", { hubTaskName: this.hubTaskName })))
            .fail((response: JQueryXHR) => this.reportError(t("hubAccessDeleteFailed", { hubTaskName: this.hubTaskName }), response.responseText));
    }
}

export = deleteReplicationHubAccessConfigCommand;
