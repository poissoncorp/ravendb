import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class deleteDatabaseCommand extends commandBase {

    private databases: string[];

    private isHardDelete: boolean;

    constructor(databases: string[], isHardDelete: boolean) {
        super();
        this.isHardDelete = isHardDelete;
        this.databases = databases;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {
        const url = endpoints.global.adminDatabases.adminDatabases;
        
        const payload: Raven.Client.ServerWide.Operations.DeleteDatabasesOperation.Parameters = {
            HardDelete: this.isHardDelete,
            DatabaseNames: this.databases,
            FromNodes: undefined
        };

        return this.del<updateDatabaseConfigurationsResult>(url, JSON.stringify(payload), null, null, 9000 * this.databases.length)
            .fail((response: JQueryXHR) => this.reportError(t("deleteFailed"), response.responseText, response.statusText));
    }


} 

export = deleteDatabaseCommand;
