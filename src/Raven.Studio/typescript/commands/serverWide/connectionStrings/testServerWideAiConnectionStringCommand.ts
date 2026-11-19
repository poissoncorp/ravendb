import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("serverWideCommands");

export default class testServerWideAiConnectionStringCommand extends commandBase {
    constructor(
        private type: Raven.Client.Documents.Operations.AI.AiConnectorType,
        private modelType: Raven.Client.Documents.Operations.AI.AiModelType,
        private settings: Partial<AiConnectionStringsSettings>
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.AI.AiConnectionTestResult> {
        const args = {
            type: this.type,
            modelType: this.modelType,
        };

        const url =
            endpoints.global.aiIntegrationServerWideConnection.adminAiTestConnection + this.urlEncodeArgs(args);

        return this.post(url, JSON.stringify(this.settings))
            .fail((response: JQueryXHR) =>
                this.reportError(t("connectionStrings.testFailed.ai"), response.responseText, response.statusText)
            )
            .done((result: Raven.Server.Documents.AI.AiConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(t("connectionStrings.testFailed.ai"), result.Error);
                }
            });
    }
}
