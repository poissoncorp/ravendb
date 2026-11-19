/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import accessManager = require("common/shell/accessManager");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("settingsModels");

class discoveryUrl {
    discoveryUrlName = ko.observable<string>();
    validationGroup: KnockoutValidationGroup;

    static isSecureServer = accessManager.default.secureServer();
    
    hasTestError = ko.observable<boolean>(false);
    
    dirtyFlag: () => DirtyFlag;
    
    constructor(urlName: string) {

        this.discoveryUrlName(urlName);
        this.initValidation();
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.discoveryUrlName,
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }
    
    initValidation() {
        this.discoveryUrlName.extend({
            validUrl: true
        });
        
        if (!discoveryUrl.isSecureServer) {
            this.discoveryUrlName.extend({
                validation: [
                    {
                        validator: (val: string) => !val || val.startsWith("http://"),
                        message: t("discoveryUrl.httpsNotSupported")
                    }]
            });
        }

        this.validationGroup = ko.validatedObservable({
            discoveryUrlName: this.discoveryUrlName
        });
    }
}

export = discoveryUrl;
