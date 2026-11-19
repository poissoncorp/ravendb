import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class registerSsoUserEntryCommand extends commandBase {

    constructor(private dto: certificatesTypes.RegisterSsoUserEntryDto) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificatesSsoUser;

        return this.put<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("registerSsoUserSucceeded", { name: this.dto.Name })))
            .fail((response: JQueryXHR) => this.reportError(t("registerSsoUserFailed"), response.responseText, response.statusText));
    }
}

export = registerSsoUserEntryCommand;
