import * as yup from "yup";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("connectionStrings");

export interface ConnectionStringsNameContext {
    isForNewConnection: boolean;
    usedNames: string[];
}

export const serverWideConnectionStringPrefix = "Server Wide Connection String";

const separator = ", ";

export function getServerWideShortName(fullName: string): string {
    return fullName.slice(serverWideConnectionStringPrefix.length + separator.length);
}

export const connectionStringsUtils = {
    nameSchema: yup
        .string()
        .nullable()
        .required()
        .test(
            "no-server-wide-prefix",
            () => t("form.nameNoServerWidePrefix", { prefix: serverWideConnectionStringPrefix }),
            (value) => {
                if (!value) {
                    return true;
                }

                return !value.trim().toLowerCase().startsWith(serverWideConnectionStringPrefix.toLowerCase());
            }
        )
        .test(
            "is-name-unique",
            () => t("form.nameNotUnique"),
            (value, ctx) => {
                const { isForNewConnection, usedNames } = ctx.options.context as ConnectionStringsNameContext;

                if (isForNewConnection) {
                    return !usedNames.includes(value);
                }

                return true;
            }
        ),
};
