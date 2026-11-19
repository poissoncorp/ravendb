import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class finishSetupCommand extends commandBase {

    execute(): JQueryPromise<void> {
        const url = endpoints.global.setup.setupFinish;

        return this.post(url, null, null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("finishSetupFailed"), response.responseText, response.statusText));
    }
}

export = finishSetupCommand;
