import backupSettings = require("models/database/tasks/periodicBackup/backupSettings");
import jsonUtil = require("common/jsonUtil");
import genUtils = require("common/generalUtils");
import popoverUtils = require("common/popoverUtils");
import tasksCommonContent = require("models/database/tasks/tasksCommonContent");
import i18nModule = require("common/i18n/i18n");

const t = i18nModule.createTranslator("googleCloudSettings");

const GOOGLE_CREDENTIALS_EXAMPLE = `{
    "type": "service_account",
    "project_id": "test-raven-237012",
    "private_key_id": "12345678123412341234123456789101",
    "private_key": "-----BEGIN PRIVATE KEY-----\\abCse=\\n-----END PRIVATE KEY-----\\n",
    "client_email": "raven@test-raven-237012-237012.iam.gserviceaccount.com",
    "client_id": "111390682349634407434",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/viewonly%40test-raven-237012.iam.gserviceaccount.com"
  }
        `;

class googleCloudSettings extends backupSettings {

    view = require("views/database/tasks/destinations/googleCloudSettings.html");
    
    bucket = ko.observable<string>();
    remoteFolderName = ko.observable<string>();
    googleCredentialsJson = ko.observable<string>();
    credentialsVisible = ko.observable<boolean>(true);
    credentialsExample = GOOGLE_CREDENTIALS_EXAMPLE;

    targetOperation: string;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.GoogleCloudSettings, targetOperation: string) {
        super(dto, "GoogleCloud");

        this.bucket(dto.BucketName);
        this.remoteFolderName(dto.RemoteFolderName || "");
        this.googleCredentialsJson(dto.GoogleCredentialsJson);
        this.credentialsVisible(!dto.GoogleCredentialsJson);

        this.targetOperation = targetOperation;
        
        this.initValidation();
        
        _.bindAll(this, "toggleCredentials");

        this.dirtyFlag = new ko.DirtyFlag([
            this.enabled,
            this.bucket,
            this.remoteFolderName,
            this.googleCredentialsJson,
            this.configurationScriptDirtyFlag().isDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    toggleCredentials() {
        this.credentialsVisible.toggle();
    }

    compositionComplete(view: Element, container: HTMLElement) {
        popoverUtils.longWithHover($(".bucket-gcs-info", container),
            {
                content: tasksCommonContent.textForPopoverGCS(t("bucketInfo.storageName"), this.targetOperation)
            });
    }
    
    initValidation() {
        const allowedCharactersRegExp = /^[a-z0-9._-]+$/;
        const allowedBeginningCharactersRegExp = /^[a-z0-9]+$/;
        const firstDashRuleRegExp = /\.-/;
        const secondDashRuleRegExp = /\.\./;
        const thirdDashRuleRegExp = /-\./;
        const fourthDashesRegExp = /_\./;
        const fifthDashesRegExp = /\._/;
        this.bucket.extend({
            validation: [
                {
                    validator: (bucket: string) => bucket && bucket.length >= 3 && bucket.length <= 222,
                    message: t("validation.bucketNameLength")
                },
                {
                    validator: (bucket: string) => allowedCharactersRegExp.test(bucket),
                    message: t("validation.allowedCharacters")
                },
                {
                    validator: (bucket: string) => bucket && allowedBeginningCharactersRegExp.test(bucket[0]) && allowedBeginningCharactersRegExp.test(bucket[bucket.length - 1]),
                    message: t("validation.bucketNameStartEnd")
                },
                {
                    validator: (bucket: string) => !firstDashRuleRegExp.test(bucket) && !secondDashRuleRegExp.test(bucket) &&
                        !thirdDashRuleRegExp.test(bucket) && !fourthDashesRegExp.test(bucket) &&
                        !fifthDashesRegExp.test(bucket),
                    message: t("validation.adjacentSpecialCharacters")
                }
            ]
        });

        this.googleCredentialsJson.extend({
            required: {
                onlyIf: () => this.enabled()
            },
            validation: [
                {
                    validator: (json: string) => json && json.includes("\"type\""),
                    message: t("validation.missingType")
                },
                {
                    validator: (json: string) => json && json.includes("\"private_key\""),
                    message: t("validation.missingPrivateKey")
                },
                {
                    validator: (json: string) => json && json.includes("\"client_email\""),
                    message: t("validation.missingClientEmail")
                }
            ]
        });

        this.localConfigValidationGroup = ko.validatedObservable({
            bucket: this.bucket,
            accountKey: this.googleCredentialsJson
        });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.GoogleCloudSettings {
        const dto = super.toDto() as Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
        dto.BucketName = this.bucket();
        dto.RemoteFolderName = this.remoteFolderName() || null;
        dto.GoogleCredentialsJson = this.googleCredentialsJson();

        return genUtils.trimProperties(dto, ["RemoteFolderName"]);
    }

    static empty(targetOperation: string): googleCloudSettings {
        return new googleCloudSettings({
            Disabled: true,
            RemoteFolderName: null,
            GoogleCredentialsJson: null,
            BucketName: null,
            GetBackupConfigurationScript: null
        }, targetOperation);
    }
}

export = googleCloudSettings;
