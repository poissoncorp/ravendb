import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("aiAssistantCommands");

export interface GiveConsentAiAssistantResultDto {
    Status: Extract<AiAssistantResponseStatus, "Success" | "InvalidCredentials">;
}

export default class giveConsentAiAssistantCommand extends commandBase {
    constructor() {
        super();
    }

    execute(): JQueryPromise<GiveConsentAiAssistantResultDto> {
        const url = endpoints.global.aiAssistant.assistantGiveConsent;

        const deferred = $.Deferred<GiveConsentAiAssistantResultDto>();

        this.post(url, null)
            .done((result) => deferred.resolve(result))
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 401 && xhr.responseJSON?.Status) {
                    deferred.resolve({
                        Status: xhr.responseJSON.Status,
                    });
                } else {
                    this.reportError(t("giveConsentFailed"), xhr.responseText, xhr.statusText);
                    deferred.reject(xhr);
                }
            });

        return deferred;
    }
}
