import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { azureSchema, s3Schema } from "components/common/formDestinations/utils/formDestinationsValidation";
import { remoteAttachmentsConstants } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsConstants";
import { storageClassOptions } from "components/utils/common";
import type { StudioTranslate } from "common/i18n/resources";

interface RemoteAttachmentsDestinationContext {
    destinations: { identifier: string }[];
    currentIdentifier?: string;
}

function getDestinationBaseSchema(t: StudioTranslate<"remoteAttachments">) {
    return yup.object({
        identifier: yup
            .string()
            .test("unique-identifier", t("validation.identifierUnique"), getIsUniqueIdentifier)
            .required(),
        disabled: yup.boolean(),
    });
}

function getIsUniqueIdentifier(value: string, ctx: yup.TestContext<RemoteAttachmentsDestinationContext>) {
    const { context } = ctx.options;

    if (context.destinations != null) {
        const matches = context.destinations.filter((dest) => dest.identifier.toLowerCase() === value.toLowerCase());

        if (!context.currentIdentifier) {
            return matches.length === 0;
        }

        if (value === context.currentIdentifier) {
            return matches.length <= 1;
        }

        return matches.length === 0;
    }

    return true;
}

const s3StorageClassSchema = yup.object({
    storageClass: yup
        .string()
        .oneOf(storageClassOptions.map((x) => x.value))
        .default("Standard"),
});

function getDestinationSchema(t: StudioTranslate<"remoteAttachments">) {
    return yup
        .object({
            provider: yup.string().oneOf(remoteAttachmentsConstants.destinationProviderList).required(),
            s3: s3Schema
                .concat(s3StorageClassSchema)
                .nullable()
                .when("provider", {
                    is: "s3",
                    then: (schema) => schema.required(),
                    otherwise: (schema) => schema.nullable(),
                }),
            azure: azureSchema.nullable().when("provider", {
                is: "azure",
                then: (schema) => schema.required(),
                otherwise: (schema) => schema.nullable(),
            }),
        })
        .concat(getDestinationBaseSchema(t));
}

const schema = yup.object({
    isRemoteAttachmentsEnabled: yup.boolean(),
    isCheckFrequencyInSecEnabled: yup.boolean(),
    checkFrequencyInSec: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isCheckFrequencyInSecEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isMaxItemsToProcessEnabled: yup.boolean(),
    maxItemsToProcess: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isMaxItemsToProcessEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
    isConcurrentUploadsEnabled: yup.boolean(),
    concurrentUploads: yup
        .number()
        .nullable()
        .positive()
        .integer()
        .when("isConcurrentUploadsEnabled", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

export function getRemoteAttachmentsDestinationYupResolver(t: StudioTranslate<"remoteAttachments">) {
    return yupResolver(getDestinationSchema(t));
}
export type RemoteAttachmentsDestinationFormData = yup.InferType<ReturnType<typeof getDestinationSchema>>;

export const remoteAttachmentsYupResolver = yupResolver(schema);
export type RemoteAttachmentsFormData = yup.InferType<typeof schema>;
