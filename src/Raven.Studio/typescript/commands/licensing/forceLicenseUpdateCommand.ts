import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import LicenseLeaseResult = Raven.Server.Commercial.LicenseLeaseResult;
import messagePublisher = require("common/messagePublisher");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("licensingCommands");

class forceLicenseUpdateCommand extends commandBase {
    execute(): JQueryPromise<LicenseLeaseResult> {
        const url = endpoints.global.license.adminLicenseForceUpdate;

        return this.post<LicenseLeaseResult>(url, null, null)
            .done((result: LicenseLeaseResult) => {
                if (result.Status === "Updated") {
                    this.reportSuccess(t("licenseUpdated"));
                }
            })
            .fail((response: JQueryXHR) => {
                const message =
                    response.status === 405
                        ? t("activationDisabled")
                        : t("activateFailed");

                this.reportError(message, response.responseText, response.statusText);
            });
    }

    static handleNotModifiedStatus(isExpired: boolean) {
        messagePublisher.reportWarning(t("licenseFetched", { context: isExpired ? "expired" : "same" }));
    }
}

export = forceLicenseUpdateCommand;
