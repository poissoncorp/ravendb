import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("aiAgentsCommands");

class deleteAiAgentCommand extends commandBase {
    constructor(
        private db: string,
        private agentId: string
    ) {
        super();
    }

    execute() {
        const args = {
            agentId: this.agentId,
        };

        const url = endpoints.databases.aiAgent.adminAiAgent + this.urlEncodeArgs(args);

        return this.del(url, null, this.db)
            .done(() => {
                this.reportSuccess(t("deleteSucceeded", { agentId: this.agentId }));
            })
            .fail((response: JQueryXHR) =>
                this.reportError(t("deleteFailed"), response.responseText, response.statusText)
            );
    }
}

export = deleteAiAgentCommand;
