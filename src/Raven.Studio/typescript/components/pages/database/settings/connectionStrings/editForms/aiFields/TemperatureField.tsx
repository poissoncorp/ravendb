import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { FormCheckbox, FormInput, FormLabel } from "components/common/Form";
import {
    ConnectionFormData,
    AiConnection,
} from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { useEffect } from "react";
import InputGroup from "react-bootstrap/InputGroup";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type FormData = ConnectionFormData<AiConnection>;

interface TemperatureFieldProps {
    baseName: Extract<FormData["connectorType"], "ollamaSettings" | "azureOpenAiSettings" | "openAiSettings">;
}

export default function TemperatureField({ baseName }: TemperatureFieldProps) {
    const t = useStudioTranslation("connectionStrings");
    const { control, setValue, watch } = useFormContext<FormData>();

    const formValues = useWatch({ control });

    const fieldNameIsSetTemperature = `${baseName}.isSetTemperature` satisfies FieldPath<FormData>;
    const fieldNameTemperature = `${baseName}.temperature` satisfies FieldPath<FormData>;

    // Reset temperature when isSetTemperature is disabled
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (name === fieldNameIsSetTemperature && !values[baseName].isSetTemperature) {
                setValue(fieldNameTemperature, null, { shouldValidate: true });
            }
        });

        return () => unsubscribe();
    }, [setValue, watch]);

    if (formValues.modelType !== "Chat") {
        return null;
    }

    return (
        <div className="mb-2">
            <FormLabel>
                {t("ai.temperature.label")}
                <PopoverWithHoverWrapper
                    message={
                        <>
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="ai.temperature.info"
                                components={{ code: <code /> }}
                            />
                            <ul className="mb-0">
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.temperature.higher"
                                        components={{ code: <code /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.temperature.lower"
                                        components={{ code: <code /> }}
                                    />
                                </li>
                                <li className="mt-1">
                                    <StudioTrans
                                        ns="connectionStrings"
                                        i18nKey="ai.temperature.ollamaDefault"
                                        components={{ br: <br />, code: <code /> }}
                                    />
                                </li>
                            </ul>
                        </>
                    }
                >
                    <Icon icon="info" color="info" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormLabel>
            <InputGroup>
                <div className="toggle-field-checkbox">
                    <FormCheckbox control={control} name={fieldNameIsSetTemperature} color="primary" />
                </div>
                <FormInput
                    type="number"
                    control={control}
                    name={fieldNameTemperature}
                    placeholder={t("ai.temperature.placeholder")}
                    disabled={!formValues[baseName].isSetTemperature}
                />
            </InputGroup>
        </div>
    );
}
