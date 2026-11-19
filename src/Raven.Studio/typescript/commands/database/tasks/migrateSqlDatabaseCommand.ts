import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class migrateSqlDatabaseCommand extends commandBase {
    
    constructor(private db: database | string, private dto: Raven.Server.SqlMigration.Model.MigrationRequest) {
          super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.databases.sqlMigration.adminSqlMigrationImport;
        return this.post<operationIdDto>(url, JSON.stringify(this.dto), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("sqlMigrationFailed"), response.responseText, response.statusText);
            });
    }
}

export = migrateSqlDatabaseCommand; 
