import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class updateCertificateCommand extends commandBase {

    constructor(private dto: certificatesTypes.UpdateCertificateDto, private isDeleteTwoFactorConfiguration: boolean) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.global.adminCertificates.adminCertificatesEdit + this.urlEncodeArgs({
            deleteTwoFactorConfiguration: this.isDeleteTwoFactorConfiguration ? true : undefined
        });
        
        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("updateSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("updateFailed"), response.responseText, response.statusText));
    }
}

export = updateCertificateCommand;
