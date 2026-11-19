import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("aiAgentsCommands");

class saveAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private dto: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration
    ) {
        super();
    }

    execute() {
        const url = endpoints.databases.aiAgent.adminAiAgent;

        return this.put(url, JSON.stringify(this.dto), this.db).fail((response: JQueryXHR) =>
            this.reportError(t("saveFailed"), response.responseText, response.statusText)
        );
    }
}

export = saveAiAgentCommand;
