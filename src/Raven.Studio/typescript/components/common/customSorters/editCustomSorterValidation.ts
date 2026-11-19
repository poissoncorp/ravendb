import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("customSortersCommon");

function buildSchema(takenNames: string[]) {
    return yup.object({
        name: yup
            .string()
            .required()
            .notOneOf(takenNames, () => t("validation.nameTaken")),
        code: yup
            .string()
            .required()
            .test(
                "class-name",
                () => t("validation.classNameMismatch"),
                (code, ctx) => {
                    const regex = new RegExp("class " + ctx.parent.name + "[{\\W]");
                    return regex.test(code);
                }
            ),
    });
}

export function createCustomSorterYupResolver(takenNames: string[] = []) {
    return yupResolver(buildSchema(takenNames));
}

export type CustomSorterFormData = yup.InferType<ReturnType<typeof buildSchema>>;
