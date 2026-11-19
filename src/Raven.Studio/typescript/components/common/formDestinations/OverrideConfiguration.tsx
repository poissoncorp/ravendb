import React from "react";
import InputGroup from "react-bootstrap/InputGroup";
import { FormInput, FormLabel } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { BackupConfigurationScript, FormDestinations } from "./utils/formDestinationsTypes";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type FieldBase = `destinations.${keyof FormDestinations["destinations"]}`;

interface OverrideConfigurationProps {
    fieldBase: FieldBase;
}

const OverrideConfiguration = ({ fieldBase }: OverrideConfigurationProps) => {
    const t = useStudioTranslation("formDestinations");
    const { control } = useFormContext<FormDestinations>();

    return (
        <div className="vstack gap-3 mt-2">
            <div className="mb-2">
                <FormLabel>{t("overrideConfig.execLabel")}</FormLabel>
                <FormInput
                    name={getName(fieldBase, "exec")}
                    control={control}
                    placeholder={t("overrideConfig.execPlaceholder")}
                    type="text"
                    autoComplete="off"
                />
            </div>
            <div className="mb-2">
                <FormLabel>{t("overrideConfig.argumentsLabel")}</FormLabel>
                <FormInput
                    type="text"
                    name={getName(fieldBase, "arguments")}
                    control={control}
                    placeholder={t("overrideConfig.argumentsPlaceholder")}
                    autoComplete="off"
                />
            </div>
            <div>
                <FormLabel>{t("overrideConfig.timeoutLabel")}</FormLabel>
                <InputGroup>
                    <FormInput
                        name={getName(fieldBase, "timeoutInMs")}
                        control={control}
                        placeholder={t("overrideConfig.timeoutPlaceholder")}
                        type="number"
                        autoComplete="off"
                    />
                    <InputGroup.Text>{t("overrideConfig.timeoutUnit")}</InputGroup.Text>
                </InputGroup>
            </div>
        </div>
    );
};

function getName(
    fieldBase: `destinations.${keyof FormDestinations["destinations"]}`,
    fieldName: keyof BackupConfigurationScript
): `destinations.${keyof FormDestinations["destinations"]}.config.${keyof BackupConfigurationScript}` {
    return `${fieldBase}.config.${fieldName}`;
}

export default OverrideConfiguration;
