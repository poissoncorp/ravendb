/// <reference path="../../../typings/tsd.d.ts"/>
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("wizardModels");

class certificateInfo {
    certificateFileName = ko.observable<string>();
    fileProtected = ko.observable<boolean>(false);
    
    certificate = ko.observable<string>();
    certificatePassword = ko.observable<string>();
    certificateCNs = ko.observableArray<string>([]);

    wildcardCertificate: KnockoutComputed<boolean>;
    expirationDateFormatted: KnockoutComputed<string>;
    
    validationGroup: KnockoutValidationGroup;

    constructor() {
        this.initValidation();
        this.initObservables();
    }
    
    private initObservables() {
        this.wildcardCertificate = ko.pureComputed(() => {
            const cns = this.certificateCNs();
            return cns.some(x => x.startsWith("*"));
            
            // Currently, for the Setup Wizard flow, the server supports only the following 2 cases:
            // 1. pfx file with Single Wildcard value (*.someDomain)
            // 2. pfx file with Single -or- Multiple values of Non-Wildcard values (localhost, localhost2...)
            // For all other cases, the server should throw an exception (Also, the two types should not be mixed)
        });
    }

    private initValidation() {
        this.certificate.extend({
            required: true
        });

        this.certificateCNs.extend({
            validation: [{
                validator: (val: Array<string>) => val.length > 0,
                message: () => t("certificate.cnRequired")
            }]
        });

        this.validationGroup = ko.validatedObservable({
            certificate: this.certificate,
            certificatePassword: this.certificatePassword,
            certificateCNs: this.certificateCNs
        });
    }
}

export = certificateInfo;
