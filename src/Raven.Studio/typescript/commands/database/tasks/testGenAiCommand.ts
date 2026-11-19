import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksCommands");

type TestGenAiScript = Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
type GenAiTestScriptResult = Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult;

class testGenAiCommand extends commandBase {
    constructor(private db: string, private payload: TestGenAiScript) {
        super();
    }  

    execute(): JQueryPromise<GenAiTestScriptResult> {
        const url = endpoints.databases.genAi.adminAiGenAiTest;

        return this.post<GenAiTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {                         
                this.reportError(t("genAiTestFailed"), response.responseText, response.statusText);
            });
    }
}

export = testGenAiCommand; 

