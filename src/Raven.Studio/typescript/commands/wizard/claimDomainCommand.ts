import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class claimDomainCommand extends commandBase {

    constructor(private domain: string, private license: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            action: "claim"
        };
        const url = endpoints.global.setup.setupDnsNCert + this.urlEncodeArgs(args); 
        const payload: Raven.Server.Commercial.ClaimDomainInfo = { 
            Domain: this.domain,
            License: this.license
        };

        return this.post(url, JSON.stringify(payload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(t("claimDomainFailed"), response.responseText, response.statusText));
    }
}

export = claimDomainCommand;
