import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("resourcesCommands");

class sendFeedbackCommand extends commandBase {
    private readonly dto: Raven.Server.Documents.Studio.FeedbackForm;

    constructor(dto: Raven.Server.Documents.Studio.FeedbackForm) {
        super();
        this.dto = dto;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.studioFeedback.studioFeedback;
        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("sendFeedbackSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("sendFeedbackFailed"), response.responseText, response.statusText));
    }
}

export = sendFeedbackCommand;
