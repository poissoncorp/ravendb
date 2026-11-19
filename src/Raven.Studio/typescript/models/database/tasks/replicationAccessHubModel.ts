/// <reference path="../../../../typings/tsd.d.ts"/>
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import prefixPathModel = require("models/database/tasks/prefixPathModel");
import replicationAccessBaseModel = require("models/database/tasks/replicationAccessBaseModel");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

class replicationAccessHubModel extends replicationAccessBaseModel {
  
    isNewAccessItem = ko.observable<boolean>();
    
    usingExistingCertificate = ko.observable<boolean>(false); 
    
    accessConfigurationWasExported = ko.observable<boolean>(false);
    certificateWasDownloaded = ko.observable<boolean>(false);
    
    certificateInfoWasSavedForSinkTask: KnockoutComputed<boolean>;

    filteringReplicationText: KnockoutComputed<string>;
    
    disableExport: KnockoutComputed<boolean>;
    disableDownload: KnockoutComputed<boolean>;

    validationGroupForSaveWithFiltering: KnockoutValidationGroup;
    validationGroupForSaveNoFiltering: KnockoutValidationGroup;
    validationGroupForExportWithFiltering: KnockoutValidationGroup;
    validationGroupForExportNoFiltering: KnockoutValidationGroup;

    constructor(accessName: string, certificate: replicationCertificateModel,
                hubToSink: prefixPathModel[], sinkToHub: prefixPathModel[],
                filteringRequired: boolean, isNewItem = true) {
        
        super(accessName, certificate, hubToSink, sinkToHub, filteringRequired);
        
        this.isNewAccessItem(isNewItem);

        this.initObservables();
        this.initValidation();
    }
    
    initObservables() {
        super.initObservables();

        this.certificateInfoWasSavedForSinkTask = ko.pureComputed(() => {
            return this.accessConfigurationWasExported() || this.certificateWasDownloaded();
        })
        
        this.disableExport = ko.pureComputed(() => {
            return !this.certificate() || !this.replicationAccessName() || (!this.hubToSinkPrefixes().length && this.filteringPathsRequired()); 
        })

        this.disableDownload = ko.pureComputed(() => {
            return !this.certificate() || this.usingExistingCertificate();
        })
        
        this.filteringReplicationText = ko.pureComputed(() => {

            const h2s = this.hubToSinkPrefixes().length;
            const s2h = this.sinkToHubPrefixes().length

            if (h2s && s2h) {
                return t("replicationAccess.bothDirectionsPaths", {
                    hubToSink: t("replicationAccess.hubToSinkPaths", { count: h2s }),
                    sinkToHub: t("replicationAccess.sinkToHubPaths", { count: s2h }),
                });
            }
            if (h2s) {
                return t("replicationAccess.hubToSinkPaths", { count: h2s });
            }
            if (s2h) {
                return t("replicationAccess.sinkToHubPaths", { count: s2h });
            }

            return "";
        })
    }
    
    initValidation() {
        super.initValidation();
        
        this.certificateInfoWasSavedForSinkTask.extend({
            validation: [
                {
                    validator: () => !this.isNewAccessItem() ||
                                     this.usingExistingCertificate() ||
                                     !this.certificate() ||
                                     this.certificateInfoWasSavedForSinkTask(),
                    message: t("replicationAccess.exportBeforeSaving")
                }
            ]
        });

        this.validationGroupForSaveWithFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            certificateInfoWasSavedForSinkTask: this.certificateInfoWasSavedForSinkTask,
            hubToSinkPrefixes: this.hubToSinkPrefixes,
            sinkToHubPrefixes: this.sinkToHubPrefixes
        });

        this.validationGroupForSaveNoFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            certificateInfoWasSavedForSinkTask: this.certificateInfoWasSavedForSinkTask,
        });

        this.validationGroupForExportWithFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate,
            hubToSinkPrefixes: this.hubToSinkPrefixes,
            sinkToHubPrefixes: this.sinkToHubPrefixes
        });

        this.validationGroupForExportNoFiltering = ko.validatedObservable({
            replicationAccessName: this.replicationAccessName,
            certificate: this.certificate
        });
    }
    
    getValidationGroupForExport(withFiltering: boolean) {
        if (withFiltering) {
            return this.validationGroupForExportWithFiltering;
        }
        
        return this.validationGroupForExportNoFiltering;
    }

    getValidationGroupForSave(withFiltering: boolean) {
        if (withFiltering) {
            return this.validationGroupForSaveWithFiltering;
        }

        return this.validationGroupForSaveNoFiltering;
    }

    static empty(filteringRequired: boolean): replicationAccessHubModel {
        return new replicationAccessHubModel("", null, [], [], filteringRequired);
    }
    
    static clone(itemToClone: replicationAccessHubModel): replicationAccessHubModel {
        return new replicationAccessHubModel(
            itemToClone.replicationAccessName(),
            itemToClone.certificate(),
            itemToClone.hubToSinkPrefixes(),
            itemToClone.sinkToHubPrefixes(),
            itemToClone.filteringPathsRequired(),
            itemToClone.isNewAccessItem()
        );
    }
}

export = replicationAccessHubModel;
