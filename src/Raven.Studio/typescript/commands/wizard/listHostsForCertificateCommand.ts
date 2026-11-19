import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardCommands");

class listHostsForCertificateCommand extends commandBase {

    constructor(private certificate: string, private password: string) {
        super();
    }

    execute(): JQueryPromise<Array<string>> {
        const url = endpoints.global.setup.setupHosts; 
        const payload = {
            Certificate: this.certificate,
            Password: this.password
        };

        const task = $.Deferred<Array<string>>();
        
        this.post(url, JSON.stringify(payload), null)
            .done(result => {
                const cns = result.AlternativeNames?.length ? [...result.AlternativeNames] : [result.CN];
                task.resolve(cns);
            })
            .fail((response: JQueryXHR) => {
                if (response.status !== 400) {
                    this.reportError(t("listHostsForCertificateFailed"), response.responseText, response.statusText);    
                }
                task.reject(response);
            });
        
        return task;
    }
}

export = listHostsForCertificateCommand;
