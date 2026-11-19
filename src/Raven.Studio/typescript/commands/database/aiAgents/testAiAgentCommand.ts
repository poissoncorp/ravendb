import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("aiAgentsCommands");

class testAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestRequest,
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestResult> {
        const url = endpoints.databases.aiAgent.aiAgentTest;

        return this.post(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError(t("testFailed"), response.responseText, response.statusText)
        );
    }
}

export = testAiAgentCommand;
