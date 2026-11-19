import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class testOlapEtlCommand extends commandBase {
    constructor(private db: database | string, private payload: Raven.Server.Documents.ETL.Providers.OLAP.Test.TestOlapEtlScript) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.OLAP.Test.OlapEtlTestScriptResult> {
        const url = endpoints.databases.olapEtl.adminEtlOlapTest;

        return this.post<Raven.Server.Documents.ETL.Providers.OLAP.Test.OlapEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("olapEtlTestFailed"), response.responseText, response.statusText);
            });
    }
}

export = testOlapEtlCommand; 

