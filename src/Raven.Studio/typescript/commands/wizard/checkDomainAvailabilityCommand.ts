import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class checkDomainAvailabilityCommand extends commandBase {

    constructor(private domainName: string, private license: Raven.Server.Commercial.License) {
        super();
    }

    execute(): JQueryPromise<domainAvailabilityResult> {
        const args = {
            action: "domain-availability"
        };
        const url = endpoints.global.setup.setupDnsNCert + this.urlEncodeArgs(args); 
        const payload = {
            Domain: this.domainName,
            License: this.license
        };

        return this.post(url, JSON.stringify(payload), null)
            .fail((response: JQueryXHR) => {
                if (response.status === 400) {
                    // ignore it will be handled in validator
                } else {
                    this.reportError(t("checkDomainAvailabilityFailed"), response.responseText, response.statusText)}
                });
    }
}

export = checkDomainAvailabilityCommand;
