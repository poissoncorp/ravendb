import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import moment = require("moment");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class licenseSendVerificationCodeCommand extends commandBase {
    constructor(private licensePayload: SendFreeLicenseVerificationRequest) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.license.licenseFreeSendVerificationCode;
        
        return this.post(url, JSON.stringify(this.licensePayload), null, { dataType: undefined })
            .fail((response: JQueryXHR) => {
                if (response.status === 429) {
                    this.reportError(t("tooManyRequests", { count: moment.duration(response.getResponseHeader("retry-after"), "seconds").asMinutes() }), response.responseText, response.statusText);
                    return;
                }
                
                this.reportError(t("generateLicenseFailed"), response.responseText, response.statusText);
            });
    }
}

export = licenseSendVerificationCodeCommand