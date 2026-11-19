import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class replaceClusterCertificateCommand extends commandBase {

    constructor(private dto: certificatesTypes.ReplaceServerCertificateDto, private isReplaceImmediately: boolean) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const args = {
            replaceImmediately: this.isReplaceImmediately
        };

        const url = endpoints.global.adminCertificates.adminCertificatesReplaceClusterCert + this.urlEncodeArgs(args);
        
        return this.post<void>(url, JSON.stringify(this.dto), null, { dataType: undefined })
            .done(() => this.reportSuccess(t("replaceClusterCertificateSucceeded")))
            .fail((response: JQueryXHR) => this.reportError(t("replaceClusterCertificateFailed"), response.responseText, response.statusText));
    }
}

export = replaceClusterCertificateCommand;
