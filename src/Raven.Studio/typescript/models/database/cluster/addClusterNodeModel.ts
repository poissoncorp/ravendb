/// <reference path="../../../../typings/tsd.d.ts"/>
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("clusterModels");

class addClusterNodeModel {
    serverUrl = ko.observable<string>();
    nodeTag = ko.observable<string>();
    addAsWatcher = ko.observable<boolean>(false);
    maxUtilizedCores = ko.observable<number>(undefined);
    useAvailableCores = ko.observable<boolean>(true);

    validationGroup: KnockoutValidationGroup = ko.validatedObservable({
        serverUrl: this.serverUrl,
        nodeTag: this.nodeTag,
        maxUtilizedCores: this.maxUtilizedCores
    });

    constructor() {
        this.useAvailableCores.subscribe(newValue => {
            this.maxUtilizedCores.clearError();
            if (!newValue) {
                return;
            }
        });

        this.initValidation();
    }

    private initValidation() {
        this.serverUrl.extend({
            required: true,
            validUrl: true
        });

        this.nodeTag.extend({
            pattern: {
                message: t("addNode.nodeTagPattern"),
                params: '^[A-Z]+$'
            },
            minLength: 1,
            maxLength: 4,
        });

        this.maxUtilizedCores.extend({
            required: {
                onlyIf: () => !this.useAvailableCores()
            },
            min: 1
        });
    }
}

export = addClusterNodeModel;
