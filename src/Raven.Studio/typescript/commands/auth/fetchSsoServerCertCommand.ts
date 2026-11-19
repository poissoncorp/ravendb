import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import certificatesTypes = require("components/pages/resources/manageServer/certificates/utils/certificatesTypes");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class fetchSsoServerCertCommand extends commandBase {

    constructor(private url: string) {
        super();
    }

    execute(): JQueryPromise<certificatesTypes.FetchSsoServerCertResult> {
        const url = endpoints.global.adminCertificates.adminCertificatesSsoServerFetch;

        return this.query<certificatesTypes.FetchSsoServerCertResult>(url, { url: this.url })
            .fail((response: JQueryXHR) => this.reportError(t("fetchSsoServerCertFailed"), response.responseText, response.statusText));
    }
}

export = fetchSsoServerCertCommand;
