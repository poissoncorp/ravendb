import commandBase = require("commands/commandBase");
import database =  require("models/resources/database");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

class getEmbeddingGenerationTasksCommand  extends commandBase {
    constructor(private db: database | string, private location: databaseLocationSpecifier, private reportFailure = true) {
        super();
    }

    execute(): JQueryPromise<Record<string, Record<string, string[]>>> {
        const task = this.getEmbeddingsGenerationTaskNames();

        if (this.reportFailure) {
            task.fail((response: JQueryXHR) => {
                this.reportError(t("embeddingsGenerationTasksFailed"), response.responseText, response.statusText);
            });
        }

        return task;
    }

    private getEmbeddingsGenerationTaskNames(): JQueryPromise<Record<string, Record<string, string[]>>> {
        const args = this.location;

        const url = endpoints.databases.studioQueryingAssistant.studioTasksEmbeddings;

        return this.query(url, args, this.db);
    }
}

export = getEmbeddingGenerationTasksCommand;
