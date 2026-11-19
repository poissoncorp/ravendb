import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class testElasticSearchEtlCommand extends commandBase {
    constructor(private db: database | string, private payload: Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.TestElasticSearchEtlScript) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.ElasticSearchEtlTestScriptResult> {
        const url = endpoints.databases.elasticSearchEtl.adminEtlElasticsearchTest;

        return this.post<Raven.Server.Documents.ETL.Providers.ElasticSearch.Test.ElasticSearchEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(t("elasticSearchEtlTestFailed"), response.responseText, response.statusText);
            });
    }
}

export = testElasticSearchEtlCommand;
