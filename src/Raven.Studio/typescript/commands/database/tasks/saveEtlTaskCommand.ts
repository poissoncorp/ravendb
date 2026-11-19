import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class saveEtlTaskCommand<T extends Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration | 
                                   Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration |
                                   Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration |
                                   Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration |
                                   Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration |
                                   Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration |
                                   Raven.Client.Documents.Operations.AI.GenAiConfiguration |
                                   Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration > extends commandBase {
    
    private constructor(private db: database | string, private payload: T, private scriptsToReset?: string[], private changeVector?: string) {
        super();
    }  

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {

        const taskLabelOptions = this.getTaskLabelOptions();

        return this.updateEtl()
            .fail((response: JQueryXHR) => {
                this.reportError(t("etlSaveFailed", taskLabelOptions), response.responseText, response.statusText);
            })
            .done(() => {
                this.reportSuccess(t("etlSaved", taskLabelOptions)); 
            });
    }

    private getTaskLabelOptions(): { etlType: string; context?: "embeddingsGeneration" } {
        if (this.payload.EtlType === "EmbeddingsGeneration") {
            return { etlType: this.payload.EtlType, context: "embeddingsGeneration" };
        }

        return { etlType: this.payload.EtlType.toUpperCase() };
    }

    private updateEtl(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        
        const args = {
            id : this.payload.TaskId || undefined,
            reset: this.scriptsToReset || undefined,
            changeVector: this.changeVector || undefined
        };
        
        const url = endpoints.databases.ongoingTasks.adminEtl + this.urlEncodeArgs(args);
        
        return this.put(url, JSON.stringify(this.payload), this.db);
    }

    static forRavenEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.RavenEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forSqlEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.SQL.SqlEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forSnowflakeEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forOlapEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.OLAP.OlapEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forElasticSearchEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchEtlConfiguration>(db, payload, scriptsToReset);
    }

    static forEmbeddingsGeneration(db: database | string, payload: Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.AI.EmbeddingsGenerationConfiguration>(db, payload, scriptsToReset);
    }

    static forGenAi(db: database | string, payload: Raven.Client.Documents.Operations.AI.GenAiConfiguration, scriptsToReset: string[], changeVector?: string) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.AI.GenAiConfiguration>(db, payload, scriptsToReset, changeVector);
    }

    static forQueueEtl(db: database | string, payload: Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration, scriptsToReset?: string[]) {
        return new saveEtlTaskCommand<Raven.Client.Documents.Operations.ETL.Queue.QueueEtlConfiguration>(db, payload, scriptsToReset);
    }
}

export = saveEtlTaskCommand;
