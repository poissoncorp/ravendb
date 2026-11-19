import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("tasksModels");

abstract class amazonSettings extends backupSettings {
    awsAccessKey = ko.observable<string>();
    awsSecretKey = ko.observable<string>();
    awsSessionToken = ko.observable<string>();
    awsRegionName = ko.observable<string>();
    remoteFolderName = ko.observable<string>();

    selectedAwsRegion = ko.observable<string>();
    
    allowedRegions: Array<string>;

    static get availableAwsRegionEndpointsStatic() {
        return [
            { label: t("backup.awsRegions.afSouth1"), value: "af-south-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apEast1"), value: "ap-east-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apSouth2"), value: "ap-south-2", hasS3: true, hasGlacier: false },
            { label: t("backup.awsRegions.apSoutheast3"), value: "ap-southeast-3", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apSoutheast4"), value: "ap-southeast-4", hasS3: true, hasGlacier: false },
            { label: t("backup.awsRegions.apSouth1"), value: "ap-south-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apNortheast3"), value: "ap-northeast-3", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apNortheast2"), value: "ap-northeast-2", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apSoutheast1"), value: "ap-southeast-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apSoutheast2"), value: "ap-southeast-2", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.apNortheast1"), value: "ap-northeast-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.caCentral1"), value: "ca-central-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.cnNorth1"), value: "cn-north-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.cnNorthwest1"), value: "cn-northwest-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euCentral1"), value: "eu-central-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euWest1"), value: "eu-west-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euWest2"), value: "eu-west-2", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euSouth1"), value: "eu-south-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euWest3"), value: "eu-west-3", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euNorth1"), value: "eu-north-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.euSouth2"), value: "eu-south-2", hasS3: true, hasGlacier: false },
            { label: t("backup.awsRegions.euCentral2"), value: "eu-central-2", hasS3: true, hasGlacier: false },
            { label: t("backup.awsRegions.ilCentral1"), value: "il-central-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.meSouth1"), value: "me-south-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.meCentral1"), value: "me-central-1", hasS3: true, hasGlacier: false },
            { label: t("backup.awsRegions.saEast1"), value: "sa-east-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usEast1"), value: "us-east-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usEast2"), value: "us-east-2", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usWest1"), value: "us-west-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usWest2"), value: "us-west-2", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usGovEast1"), value: "us-gov-east-1", hasS3: true, hasGlacier: true },
            { label: t("backup.awsRegions.usGovWest1"), value: "us-gov-west-1", hasS3: true, hasGlacier: true }
        ];
    }

    availableAwsRegionEndpoints = amazonSettings.availableAwsRegionEndpointsStatic;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.AmazonSettings, 
                connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType,
                allowedRegions: Array<string>) {
        super(dto, connectionType);

        this.allowedRegions = allowedRegions;
        
        this.awsAccessKey(dto.AwsAccessKey);
        this.awsSecretKey(dto.AwsSecretKey);
        this.awsSessionToken(dto.AwsSessionToken || "");
        this.awsRegionName(dto.AwsRegionName);
        this.remoteFolderName(dto.RemoteFolderName || "");

        const lowerCaseRegionName = !dto.AwsRegionName ? "" : dto.AwsRegionName.toLowerCase();
        const region = this.availableAwsRegionEndpoints.find(x => x.value === lowerCaseRegionName);
        this.selectedAwsRegion(region ? amazonSettings.getDisplayRegionName(region) : dto.AwsRegionName);

        this.selectedAwsRegion.subscribe(newSelectedAwsRegion => {
            if (!newSelectedAwsRegion) {
                this.awsRegionName(null);
                return;
            }

            const newSelectedAwsRegionLowerCase = newSelectedAwsRegion.toLowerCase();
            const foundRegion = this.availableAwsRegionEndpoints.find(x =>
                amazonSettings.getDisplayRegionName(x).toLowerCase() === newSelectedAwsRegionLowerCase);
            
            if (foundRegion) {
                // if we managed to find long name - set short name under the hood
                this.awsRegionName(foundRegion.value);
            } else {
                this.awsRegionName(newSelectedAwsRegion.trim());
            }
        });
        
        this.dirtyFlag = new ko.DirtyFlag([
            this.awsAccessKey,
            this.awsSecretKey,
            this.awsRegionName,
            this.selectedAwsRegion,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    initAmazonValidation() {
        
        this.awsRegionName.extend({
            required: {
                onlyIf: () => this.isRegionRequired()
            },
            validation: [
                {
                    validator: (awsRegionName: string) => !this.isRegionRequired() ||
                                                          (awsRegionName && (this.isRegionFound(awsRegionName) || !this.allowedRegions)),
                                                          // Allow custom regions only if administrator didn't restrict regions (if this.allowedRegions is null)
                    message: t("backup.invalidRegion")
                },
                {
                    validator: (awsRegionName: string) => !this.isRegionRequired() || 
                                                          (awsRegionName && awsRegionName.includes("-") && !awsRegionName.startsWith("-") && !awsRegionName.endsWith("-")),
                    message: t("backup.regionFormat")
                }
            ]
        });

        this.awsAccessKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });

        this.awsSecretKey.extend({
            required: {
                onlyIf: () => this.enabled()
            }
        });
    }

    isRegionRequired() {
        return this.enabled();
    }

    private isRegionFound(region: string) {
        return this.availableAwsRegionEndpoints.find(x => x.value.toLowerCase() === region.toLowerCase());
    }
    
    createAwsRegionAutoCompleter(hasS3: boolean) {
        return ko.pureComputed(() => {
            let key = this.selectedAwsRegion();
            const options = this.availableAwsRegionEndpoints
                .filter(x => hasS3 ? x.hasS3 : x.hasGlacier)
                .filter(x => this.allowedRegions ? _.includes(this.allowedRegions, x.label) : true);

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => amazonSettings.getDisplayRegionName(x).toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }

    useAwsRegion(awsRegionEndpoint: { label: string, value: string }) {
        this.selectedAwsRegion(amazonSettings.getDisplayRegionName(awsRegionEndpoint));
    }

    static getDisplayRegionName(awsRegionEndpoint: { label: string, value: string }): string {
        return awsRegionEndpoint.label + " - " + awsRegionEndpoint.value;
    }

    toDto(): Raven.Client.Documents.Operations.Backups.AmazonSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.AmazonSettings;
        dto.AwsAccessKey = this.awsAccessKey();
        dto.AwsSecretKey = this.awsSecretKey();
        dto.AwsSessionToken = this.awsSessionToken() || null;
        dto.AwsRegionName = this.awsRegionName();
        dto.RemoteFolderName = this.remoteFolderName() || null;
        return dto;
    }
}

export = amazonSettings;
