import * as yup from "yup";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("databases");

export const dataDirectoryStepSchema = yup.object({
    isDefault: yup.boolean(),
    directory: yup.string().when("isDefault", {
        is: false,
        then: (schema) =>
            schema
                .required()
                .trim()
                .strict()
                .max(248)
                .test(
                    "forbidden-characters",
                    () => t("createDatabase.validation.pathForbiddenCharacters"),
                    (value) => {
                        return /^[^*?"<>|]*$/.test(value);
                    }
                )
                .test(
                    "forbidden-characters",
                    () => t("createDatabase.validation.nameForbidden"),
                    (value) => {
                        return !/^(nul|prn|con|lpt[0-9]|com[0-9])(\.|$)/i.test(value);
                    }
                )
                .test(
                    "forbidden-prefix",
                    () => t("createDatabase.validation.pathIllegalPrefix"),
                    (value) => {
                        if (!value) {
                            return true;
                        }

                        const valueLower = value.toLowerCase();
                        return (
                            !valueLower.startsWith("~") &&
                            !valueLower.startsWith("$home") &&
                            !valueLower.startsWith("appdrive:")
                        );
                    }
                ),
    }),
});

export const encryptionStepSchema = yup.object({
    key: yup.string().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.base64().required(),
    }),
    isKeySaved: yup.boolean().when("$isEncrypted", {
        is: true,
        then: (schema) => schema.oneOf([true], () => t("createDatabase.validation.encryptionKeyNotSaved")),
    }),
});

export type Encryption = yup.InferType<typeof encryptionStepSchema>;

export const databaseNameSchema = yup
    .string()
    .trim()
    .strict()
    .required()
    .test(
        "db-exists",
        () => t("createDatabase.validation.databaseExists"),
        (value, ctx) => {
            return !ctx.options.context.usedDatabaseNames.some(
                (name: string) => name.toLowerCase() === value.toLowerCase()
            );
        }
    );
