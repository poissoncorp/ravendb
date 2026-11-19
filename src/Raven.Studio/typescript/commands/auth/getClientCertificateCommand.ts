import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class getClientCertificateCommand extends commandBase {

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition & { HasTwoFactor: boolean; TwoFactorExpirationDate: string; }> {
        const url = endpoints.global.adminCertificates.certificatesWhoami;
        
        return this.query<Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition & { HasTwoFactor: boolean; TwoFactorExpirationDate: string; }>(url, null)
            .fail((response: JQueryXHR) => this.reportError(t("getClientCertificateFailed"), response.responseText,  response.statusText));
    }
}

export = getClientCertificateCommand;
