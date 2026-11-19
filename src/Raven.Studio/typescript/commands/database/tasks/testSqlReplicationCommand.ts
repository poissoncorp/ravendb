import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class testSqlReplicationCommand<TRelationalConnectionString extends Raven.Client.Documents.Operations.ConnectionStrings.ConnectionString, TRelationalEtlConfiguration extends Raven.Client.Documents.Operations.ETL.EtlConfiguration<TRelationalConnectionString>> extends commandBase {
    constructor(private db: database | string, private payload: Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.TestRelationalDatabaseEtlScript<TRelationalConnectionString, TRelationalEtlConfiguration>) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.RelationalDatabaseEtlTestScriptResult> {
        const url = endpoints.databases.sqlEtl.adminEtlSqlTest;

        return this.post<Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test.RelationalDatabaseEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(t("sqlEtlTestFailed"), response.responseText, response.statusText);
            });
    }
}

export = testSqlReplicationCommand; 

