import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("authCommands");

class getClusterDomainsCommand extends commandBase {

    execute(): JQueryPromise<Array<string>> {
        const url = endpoints.global.adminCertificates.adminCertificatesClusterDomains;
        
        return this.query<Array<string>>(url, null, null,  x => x.ClusterDomains)
            .fail((response: JQueryXHR) => this.reportError(t("getClusterDomainsFailed"), response.responseText,  response.statusText));
    }
}

export = getClusterDomainsCommand;
