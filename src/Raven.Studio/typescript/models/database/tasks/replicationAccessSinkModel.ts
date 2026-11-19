/// <reference path="../../../../typings/tsd.d.ts"/>
import replicationCertificateModel = require("models/database/tasks/replicationCertificateModel");
import prefixPathModel = require("models/database/tasks/prefixPathModel");
import replicationAccessBaseModel = require("models/database/tasks/replicationAccessBaseModel");
import messagePublisher = require("common/messagePublisher");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");
const tCommon = i18nModule.createTranslator("studioCommon");

class replicationAccessSinkModel extends replicationAccessBaseModel {

    selectedFileName = ko.observable<string>();
    selectedFileCertificate = ko.observable<string>();
    selectedFilePassphrase = ko.observable<string>();
    
    certificateExtracted = ko.observable<boolean>(false);
    certificateSourceText: KnockoutComputed<string>;
    
    serverCertificateSelected = ko.observable<boolean>(false);
    serverCertificate = ko.observable<replicationCertificateModel>();
    
    validationGroup: KnockoutValidationGroup;
    
    constructor(accessName: string, 
                certificate: replicationCertificateModel,
                serverCertificate: replicationCertificateModel,
                hubToSink: prefixPathModel[],
                sinkToHub: prefixPathModel[]) {
        super(accessName, certificate, hubToSink, sinkToHub, false);
        
        this.serverCertificate(serverCertificate);
        
        this.certificateSourceText = ko.pureComputed(() => {
            return t("replicationAccess.certificateSource", { context: this.serverCertificateSelected() ? "server" : "own" });
        });
        
        this.initValidation();
    }
    
    initValidation() {
        super.initValidation();

        this.certificateExtracted.extend({
            validation: [
                {
                    validator: () => !this.selectedFileName() || this.certificateExtracted(),
                    message: t("replicationAccess.certificateNotExtracted")
                },
                {
                    validator: () => this.certificate(),
                    message: t("replicationAccess.privateKeyRequired")
                }
            ]
        });
        
        this.validationGroup = ko.validatedObservable({
            certificate: this.certificate,
            certificateExtracted: this.certificateExtracted,
            replicationAccessName: this.replicationAccessName
        });
    }

    onCertificateSelected(certAsBase64: string) {
        this.selectedFileCertificate(certAsBase64);
        this.tryReadCertificate(false);
    }

    tryReadCertificate(reportErrors = true): boolean {
        const certificate = this.selectedFileCertificate();
        const password = this.selectedFilePassphrase() || undefined;

        if (certificate) {
            try {
                this.certificate(replicationCertificateModel.fromPkcs12(certificate, password));
                this.selectedFileCertificate(null);
                this.selectedFilePassphrase("");
                this.certificateExtracted(true);
                return true;
            } catch ($e) {
                if (reportErrors) {
                    const errorMessage = $e instanceof Error && $e.message === tCommon("certificate.noPrivateKey")
                        ? $e.message
                        : t("replicationAccess.extractCertificateFailed");
                    messagePublisher.reportError(errorMessage, $e);
                }
                this.certificateExtracted(false);
                return false;
            }
        }
    }

    useServerCertificate(useServerCertificate: boolean) {
        if (!useServerCertificate) {
            this.serverCertificateSelected(false);
            this.certificate(null);
            return;
        }

        this.serverCertificateSelected(true);
        this.certificate(this.serverCertificate());
    }

    static clone(itemToClone: replicationAccessSinkModel): replicationAccessSinkModel {
        return new replicationAccessSinkModel(
            itemToClone.replicationAccessName(),
            itemToClone.certificate(),
            itemToClone.serverCertificate(),
            itemToClone.hubToSinkPrefixes(),
            itemToClone.sinkToHubPrefixes()
        );
    }
}

export = replicationAccessSinkModel;
