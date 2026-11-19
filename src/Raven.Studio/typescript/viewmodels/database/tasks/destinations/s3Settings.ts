import amazonSettings = require("models/database/tasks/periodicBackup/amazonSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import common = require("components/utils/common");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("s3Settings");

type S3StorageClass = Raven.Client.Documents.Operations.Backups.S3StorageClass;

type TargetOperation = "PeriodicBackup" | "ManualBackup" | "OLAP";

class s3Settings extends amazonSettings {
    
    view = require("views/database/tasks/destinations/s3Settings.html");
    
    bucketName = ko.observable<string>();
    useCustomS3Host = ko.observable<boolean>();
    customServerUrl = ko.observable<string>();
    forcePathStyle = ko.observable<boolean>();
    disableChecksumValidation = ko.observable<boolean>();
    accessKeyPropertyName: KnockoutComputed<string>;
    secretKeyPropertyName: KnockoutComputed<string>;
    isSecretHidden = ko.observable<boolean>(true);
    isSessionTokenHidden = ko.observable<boolean>(true);

    targetOperation: TargetOperation;

    storageClassOptions = common.storageClassOptions;
    storageClass = ko.observable<S3StorageClass>("Standard");
    storageClassLabel: KnockoutComputed<string>;

    constructor(dto: Raven.Client.Documents.Operations.Backups.S3Settings, allowedRegions: Array<string>, targetOperation: TargetOperation) {
        super(dto, "S3", allowedRegions);

        this.bucketName(dto.BucketName);
        this.customServerUrl(dto.CustomServerUrl);
        this.forcePathStyle(dto.ForcePathStyle);
        this.disableChecksumValidation(dto.DisableChecksumValidation);
        this.useCustomS3Host(!!dto.CustomServerUrl);
        this.targetOperation = targetOperation;
        this.storageClass(dto.StorageClass ?? "Standard");
        
        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.bucketName,
            this.awsAccessKey,
            this.awsSecretKey,
            this.awsSessionToken,
            this.awsRegionName,
            this.remoteFolderName,
            this.selectedAwsRegion,
            this.customServerUrl,
            this.forcePathStyle,
            this.disableChecksumValidation,
            this.useCustomS3Host,
            this.storageClass,
            
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);

        this.useCustomS3Host.subscribe(() => {
            if (this.testConnectionResult()) {
                this.testConnectionResult(null);
            }
        });

        this.accessKeyPropertyName = ko.pureComputed(() => s3Settings.getAccessKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));
        this.secretKeyPropertyName = ko.pureComputed(() => s3Settings.getSecretKeyPropertyName(this.useCustomS3Host(), this.customServerUrl()));
        this.storageClassLabel = ko.computed(() => this.storageClassOptions.find(option => option.value === this.storageClass())?.label);
    }

    static getAccessKeyPropertyName(useCustomS3Host: boolean, customServerUrl: string) {
        return s3Settings.isBackBlaze(useCustomS3Host, customServerUrl) ? t("applicationKeyId") : t("accessKey");
    }

    static getSecretKeyPropertyName(useCustomS3Host: boolean, customServerUrl: string) {
        return s3Settings.isBackBlaze(useCustomS3Host, customServerUrl) ? t("applicationKey") : t("secretKey");
    }

    private static isBackBlaze(useCustomS3Host: boolean, customServerUrl: string) {
        return useCustomS3Host && customServerUrl && customServerUrl.toLowerCase().endsWith(".backblazeb2.com");
    }

    private getPopoverTargetOperation(): string {
        if (this.targetOperation === "ManualBackup" || this.targetOperation === "PeriodicBackup") {
            return t("bucketInfo.targetOperationBackup");
        }

        return this.targetOperation;
    }
    
    compositionComplete(view: Element, container: HTMLElement) {
        popoverUtils.longWithHover($(".bucket-info", container),
            {
                content: tasksCommonContent.textForPopover(t("bucketInfo.storageName"), this.getPopoverTargetOperation())
            });
    }
    
    initValidation() {
        super.initAmazonValidation();
        
        /* Bucket name must :
            - be at least 3 and no more than 63 characters long.
            - be a series of one or more labels. 
                Adjacent labels are separated by a single period (.). 
                Bucket names can contain lowercase letters, numbers, and hyphens. 
                Each label must start and end with a lowercase letter or a number.
            - not be formatted as an IP address (e.g., 192.168.5.4).
        */

        const ipRegExp = /^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/;
        const letterOrNumberRegex = /^[a-z0-9]+$/;
        const regExp = /^[a-z0-9.-]+$/;
        this.bucketName.extend({
            validation: [
                {
                    validator: (bucketName: string) => bucketName && bucketName.length >= 3 && bucketName.length <= 63,
                    message: t("validation.bucketNameLength")
                },
                {
                    validator: (bucketName: string) => bucketName && regExp.test(bucketName),
                    message: t("validation.allowedCharacters")
                },
                {
                    validator: (bucketName: string) => bucketName && letterOrNumberRegex.test(bucketName[0]),
                    message: t("validation.bucketNameStart")
                },
                {
                    validator: (bucketName: string) => bucketName && letterOrNumberRegex.test(bucketName[bucketName.length - 1]),
                    message: t("validation.bucketNameEnd")
                },
                {
                    validator: (bucketName: string) => bucketName && !bucketName.includes(".."),
                    message: t("validation.consecutivePeriods")
                },
                {
                    validator: (bucketName: string) => bucketName && !bucketName.includes(".-") && !bucketName.includes("-."),
                    message: t("validation.dashesNextToPeriods")
                },
                {
                    validator: (bucketName: string) => !ipRegExp.test(bucketName),
                    message: t("validation.ipAddress")
                }
            ]
        });
        
        this.customServerUrl.extend({
            required: {
                onlyIf: () => this.useCustomS3Host()
            },
            validUrl: {
                onlyIf: () => this.useCustomS3Host()
            }
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            awsAccessKey: this.awsAccessKey,
            awsSecretKey: this.awsSecretKey,
            awsRegionName: this.awsRegionName,
            bucketName: this.bucketName,
            customServerUrl: this.customServerUrl
        });
    }
    
    isRegionRequired() {
        const isRegionRequired = this.useCustomS3Host ? !this.hasConfigurationScript() && !this.useCustomS3Host() :
                                                        !this.hasConfigurationScript();
        return super.isRegionRequired() && isRegionRequired;
    }

    toDto(): Raven.Client.Documents.Operations.Backups.S3Settings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.S3Settings;
        
        dto.BucketName = this.bucketName();
        dto.CustomServerUrl = !this.hasConfigurationScript() && this.useCustomS3Host() ? this.customServerUrl() : undefined;
        dto.ForcePathStyle = !this.hasConfigurationScript() && this.useCustomS3Host() ? this.forcePathStyle() : false;
        dto.DisableChecksumValidation = !this.hasConfigurationScript() && this.useCustomS3Host() ? this.disableChecksumValidation() : false;
        dto.StorageClass = this.storageClass();
        
        return genUtils.trimProperties(dto, ["CustomServerUrl", "RemoteFolderName", "AwsRegionName", "AwsAccessKey", "AwsSessionToken"]);
    }

    static empty(allowedRegions: Array<string>, targetOperation: TargetOperation): s3Settings {
        return new s3Settings({
            Disabled: true,
            AwsAccessKey: null,
            AwsRegionName: null,
            AwsSecretKey: null,
            AwsSessionToken: null,
            BucketName: null,
            RemoteFolderName: null,
            GetBackupConfigurationScript: null,
            ForcePathStyle: false,
            DisableChecksumValidation: false,
            CustomServerUrl: null,
            StorageClass: "Standard",
        }, allowedRegions, targetOperation);
    }
    
    toggleIsSecretHidden() {
        this.isSecretHidden(!this.isSecretHidden());
    }

    toggleIsSessionTokenHidden() {
        this.isSessionTokenHidden(!this.isSessionTokenHidden());
    }
}

export = s3Settings;
