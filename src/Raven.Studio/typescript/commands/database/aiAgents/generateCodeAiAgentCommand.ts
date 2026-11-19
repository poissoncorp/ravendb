import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("aiAgentsCommands");

export type AiAgentGenerateCodeLanguage = "c#" | "python" | "javascript";

export interface AiAgentGenerateCodeResultDto {
    GeneratedCode: string;
}

export default class generateCodeAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId: string,
        private language: AiAgentGenerateCodeLanguage
    ) {
        super();
    }

    execute() {
        const args = {
            agentId: this.agentId,
            language: this.language,
        };

        const url = endpoints.databases.aiAgent.adminAiAgentGenerateCode;

        return this.query<AiAgentGenerateCodeResultDto>(url, args, this.db).fail((response: JQueryXHR) =>
            this.reportError(t("generateCodeFailed"), response.responseText, response.statusText)
        );
    }
}
