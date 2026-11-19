import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import type { StudioTranslate } from "common/i18n/resources";

function getSchema(t: StudioTranslate<"gatherDebugInfo">) {
    return yup
        .object({
            includeServer: yup.boolean().required(),
            includeDatabases: yup.boolean().required(),
            includeLogs: yup.boolean().required(),
            isSelectAllDatabases: yup.boolean().required(),
            selectedDatabases: yup
                .array()
                .of(yup.string().required())
                .when(["includeDatabases", "isSelectAllDatabases"], {
                    is: (includeDatabases: boolean, isSelectAllDatabases: boolean) => {
                        return includeDatabases && !isSelectAllDatabases;
                    },
                    then: (schema) => schema.min(1, t("databases.required")),
                }),
        })
        .required();
}

export const getGatherDebugInfoYupResolver = (t: StudioTranslate<"gatherDebugInfo">) => yupResolver(getSchema(t));
export type GatherDebugInfoFormData = yup.InferType<ReturnType<typeof getSchema>>;
