import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class loadAgreementCommand extends commandBase {

    constructor(private email: string) {
        super();
    }

    execute(): JQueryPromise<string> {
        const args = {
            email: this.email
        };
        
        const url = endpoints.global.setup.setupLetsencryptAgreement + this.urlEncodeArgs(args);

        return this.query(url, null, null, x => x.Uri)
            .fail((response: JQueryXHR) => this.reportError(t("loadAgreementFailed"), response.responseText, response.statusText));
    }
}

export = loadAgreementCommand;
