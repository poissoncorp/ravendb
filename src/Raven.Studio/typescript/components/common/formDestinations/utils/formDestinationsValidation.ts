import { yupObjectSchema } from "components/utils/yupUtils";
import {
    AmazonDestination,
    AzureAuthType,
    AzureDestination,
    BackupConfigurationScript,
    FormDestinationDataBase,
    FormDestinations,
    FtpDestination,
    GlacierDestination,
    GoogleCloudDestination,
    LocalDestination,
    S3Destination,
} from "./formDestinationsTypes";
import * as yup from "yup";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("formDestinations");

const configSchema = yupObjectSchema<BackupConfigurationScript>({
    isOverrideConfig: yup.boolean(),
    arguments: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    exec: yup
        .string()
        .nullable()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
    timeoutInMs: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isOverrideConfig", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

const destinationBaseSchema = yupObjectSchema<FormDestinationDataBase>({
    isEnabled: yup.boolean(),
    config: configSchema,
});

type WithoutBase<T extends FormDestinationDataBase> = Omit<T, "isEnabled" | "isOverrideConfig" | "config">;
type WithoutAmazonAndBase<T extends S3Destination | GlacierDestination> = Omit<
    WithoutBase<T>,
    "awsAccessKey" | "awsRegionName" | "awsSecretKey" | "remoteFolderName" | "awsSessionToken"
>;

const yupRequiredStringForEnabled = yup
    .string()
    .nullable()
    .when(["isEnabled", "config"], {
        is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
        then: (schema) => schema.required(),
    });

const localSchema = yupObjectSchema<WithoutBase<LocalDestination>>({
    folderPath: yupRequiredStringForEnabled,
}).concat(destinationBaseSchema);

const amazonSchema = yupObjectSchema<AmazonDestination>({
    remoteFolderName: yup.string().nullable(),
    awsRegionName: yup
        .string()
        .nullable()
        .when(["isEnabled", "isUseCustomHost", "config"], {
            is: (isEnabled: boolean, isUseCustomHost: boolean, config: BackupConfigurationScript) =>
                isEnabled && !isUseCustomHost && !config.isOverrideConfig,
            then: (schema) =>
                schema.required().test(
                    "include-dash",
                    () => t("validation.awsRegionDash"),
                    (value) => value.includes("-") && !value.startsWith("-") && !value.endsWith("-")
                ),
        }),
    awsAccessKey: yupRequiredStringForEnabled,
    awsSecretKey: yupRequiredStringForEnabled,
    awsSessionToken: yup.string().nullable(),
});

export const s3Schema = yupObjectSchema<WithoutAmazonAndBase<S3Destination>>({
    bucketName: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(63)
                    .matches(/^[a-z\d]/, () => t("validation.bucketNameStart"))
                    .matches(/[a-z\d]$/, () => t("validation.bucketNameEnd"))
                    .matches(/^[a-z0-9.-]+$/, () => t("validation.bucketNameAllowedCharacters"))
                    .test(
                        "dashes-next-to-period",
                        () => t("validation.bucketNameDashesNextToPeriods"),
                        (value) => !value.includes(".-") && !value.includes("-.")
                    )
                    .test(
                        "consecutive-periods",
                        () => t("validation.bucketNameConsecutivePeriods"),
                        (value) => !value.includes("..")
                    )
                    .test(
                        "ip",
                        () => t("validation.bucketNameIpAddress"),
                        (value) => !/^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/.test(value)
                    ),
        }),
    forcePathStyle: yup.boolean(),
    disableChecksumValidation: yup.boolean(),
    isUseCustomHost: yup.boolean(),
    customServerUrl: yup
        .string()
        .nullable()
        .when(["isUseCustomHost", "config"], {
            is: (isUseCustomHost: boolean, config: BackupConfigurationScript) =>
                isUseCustomHost && !config.isOverrideConfig,
            then: (schema) => schema.required().basicUrl(),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

function yupRequiredStringForAzureAuthType(authType: AzureAuthType) {
    return yup
        .string()
        .nullable()
        .when(["isEnabled", "config", "authType"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript, selectedAuthType: AzureAuthType) =>
                isEnabled && !config.isOverrideConfig && selectedAuthType === authType,
            then: (schema) => schema.required(),
        });
}

export const azureSchema = yupObjectSchema<WithoutBase<AzureDestination>>({
    storageContainer: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(63)
                    .matches(/^[a-z0-9-]+$/, () => t("validation.containerAllowedCharacters"))
                    .matches(/^[a-z0-9]/, () => t("validation.containerStart"))
                    .matches(/[a-z0-9]$/, () => t("validation.containerEnd"))
                    .test(
                        "consecutive-dashes",
                        () => t("validation.containerConsecutiveDashes"),
                        (value) => !/--/.test(value)
                    ),
        }),
    remoteFolderName: yup.string().nullable(),
    accountName: yupRequiredStringForEnabled,
    authType: yup.string<AzureAuthType>(),
    accountKey: yupRequiredStringForAzureAuthType("accountKey"),
    sasToken: yupRequiredStringForAzureAuthType("sasToken"),
}).concat(destinationBaseSchema);

const googleCloudSchema = yupObjectSchema<WithoutBase<GoogleCloudDestination>>({
    bucketName: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema
                    .required()
                    .min(3)
                    .max(222, () => t("validation.googleBucketLength"))
                    .matches(/^[a-z0-9._-]+$/, () => t("validation.googleBucketAllowedCharacters"))
                    .matches(/^[a-z0-9]/, () => t("validation.googleBucketStart"))
                    .matches(/[a-z0-9]$/, () => t("validation.googleBucketEnd"))
                    .test(
                        "dash-period-underscore",
                        () => t("validation.googleBucketAdjacentSeparators"),
                        (value) => {
                            return (
                                !/\.-/.test(value) &&
                                !/\.\./.test(value) &&
                                !/-\./.test(value) &&
                                !/_\./.test(value) &&
                                !/\._/.test(value)
                            );
                        }
                    ),
        }),

    remoteFolderName: yup.string().nullable(),
    googleCredentialsJson: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema
                    .required()
                    .matches(/"type"/, () => t("validation.googleCredentialsMissingField", { field: "type" }))
                    .matches(/"private_key"/, () =>
                        t("validation.googleCredentialsMissingField", { field: "private_key" })
                    )
                    .matches(/"client_email"/, () =>
                        t("validation.googleCredentialsMissingField", { field: "client_email" })
                    ),
        }),
}).concat(destinationBaseSchema);

const glacierSchema = yupObjectSchema<WithoutAmazonAndBase<GlacierDestination>>({
    vaultName: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema
                    .required()
                    .min(1)
                    .max(255)
                    .matches(/^[A-Za-z0-9_.-]+$/, () => t("validation.vaultNameAllowedCharacters")),
        }),
})
    .concat(destinationBaseSchema)
    .concat(amazonSchema);

const ftpSchema = yupObjectSchema<WithoutBase<FtpDestination>>({
    url: yup
        .string()
        .nullable()
        .when(["isEnabled", "config"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript) => isEnabled && !config.isOverrideConfig,
            then: (schema) =>
                schema.required().test(
                    "ftp-url",
                    () => t("validation.ftpUrlProtocol"),
                    (value) => {
                        const lowerCaseValue = value.toLowerCase();
                        return lowerCaseValue.startsWith("ftp://") || lowerCaseValue.startsWith("ftps://");
                    }
                ),
        }),
    userName: yupRequiredStringForEnabled,
    password: yupRequiredStringForEnabled,
    certificateAsBase64: yup
        .string()
        .nullable()
        .when(["isEnabled", "config", "url"], {
            is: (isEnabled: boolean, config: BackupConfigurationScript, url: string) =>
                isEnabled && !config.isOverrideConfig && url && url.toLowerCase().startsWith("ftps://"),
            then: (schema) => schema.required(),
        }),
}).concat(destinationBaseSchema);

export const destinationsSchema = yup.object({
    destinations: yupObjectSchema<FormDestinations["destinations"]>({
        local: localSchema,
        s3: s3Schema,
        azure: azureSchema,
        googleCloud: googleCloudSchema,
        glacier: glacierSchema,
        ftp: ftpSchema,
    }).test(
        "at-least-one-destination",
        () => t("validation.atLeastOneDestination"),
        (value) => {
            return (
                value.local.isEnabled ||
                value.s3.isEnabled ||
                value.azure.isEnabled ||
                value.googleCloud.isEnabled ||
                value.glacier.isEnabled ||
                value.ftp.isEnabled
            );
        }
    ),
});
