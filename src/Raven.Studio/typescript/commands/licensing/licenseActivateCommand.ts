import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class licenseActivateCommand extends commandBase {

    private licensePayload: Raven.Server.Commercial.License;

    constructor(licensePayload: Raven.Server.Commercial.License) {
        super();
        this.licensePayload = licensePayload;
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.adminLicenseActivate;

        return this.post(url, JSON.stringify(this.licensePayload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                const message = response.status === 405 ?
                    t("activationDisabled") : 
                    t("activateFailed");

                this.reportError(message, response.responseText, response.statusText);
            });
    }
}

export = licenseActivateCommand; 
