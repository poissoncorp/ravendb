import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { createTranslator } from "common/i18n/i18n";

const t = createTranslator("customAnalyzersCommon");

const schema = yup.object({
    name: yup.string().required(),
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

export const customAnalyzerYupResolver = yupResolver(schema);
export type CustomAnalyzerFormData = yup.InferType<typeof schema>;
