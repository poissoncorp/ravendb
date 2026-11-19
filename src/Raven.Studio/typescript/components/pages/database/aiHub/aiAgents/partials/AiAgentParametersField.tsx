import classNames from "classnames";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Control } from "react-hook-form";
import { TestAiAgentFormData } from "../edit/utils/editAiAgentValidation";
import { ChatAiAgentFormData } from "../chat/utils/chatAiAgentValidation";
import { SelectOption } from "components/common/select/Select";
import InputGroup from "react-bootstrap/InputGroup";
import { aiAgentParametersUtils } from "../utils/aiAgentParametersUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface Parameter {
    name?: string;
    value?: string;
    type?: Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType;
}

type FormData = TestAiAgentFormData | ChatAiAgentFormData;

interface AiAgentParametersFieldProps {
    control: Control<FormData>;
    value: Parameter[];
    wrapperClassName?: string;
    headerClassName?: string;
    panelClassName?: string;
}

export default function AiAgentParametersField({
    control,
    value,
    wrapperClassName,
    headerClassName,
    panelClassName,
}: AiAgentParametersFieldProps) {
    const t = useStudioTranslation("aiAgents");
    if (value.length === 0) {
        return null;
    }

    return (
        <div className={classNames("w-100 vstack flex-grow-1 overflow-auto", wrapperClassName)}>
            <h3 className={classNames(headerClassName)}>
                <Icon icon="metrics" color="primary" />
                {t("parameters.heading")}
                <PopoverWithHoverWrapper message={t("parameters.headingInfo")}>
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </h3>
            <div className="overflow-auto d-grid gap-2 mb-2">
                {value.map((parameter, idx) => (
                    <ParameterItem
                        key={idx}
                        control={control}
                        parameter={parameter}
                        idx={idx}
                        panelClassName={panelClassName}
                    />
                ))}
            </div>
        </div>
    );
}

interface ParameterItemProps {
    control: Control<FormData>;
    parameter: Parameter;
    idx: number;
    panelClassName?: string;
}

function ParameterItem({ control, parameter, idx, panelClassName }: ParameterItemProps) {
    const t = useStudioTranslation("aiAgents");
    if (parameter.type === "Null") {
        return null;
    }

    return (
        <div className={classNames("p-2 rounded-2 d-grid gap-1 border border-secondary", panelClassName)}>
            <div className="d-flex justify-content-between align-items-end">
                <span className="text-truncate font-monospace" title={parameter.name} style={{ maxWidth: "300px" }}>
                    {parameter.name}
                </span>
                <FormSwitch control={control} name={`parameters.${idx}.isSendToModel`} color="primary">
                    {t("parameters.sendToModel")}
                </FormSwitch>
            </div>
            <ParameterItemInput control={control} parameter={parameter} idx={idx} />
        </div>
    );
}

interface ParameterItemInputProps {
    control: Control<FormData>;
    parameter: Parameter;
    idx: number;
}

function ParameterItemInput({ control, parameter, idx }: ParameterItemInputProps) {
    const t = useStudioTranslation("aiAgents");
    const typeInfo = aiAgentParametersUtils.getParameterTypeInfo(parameter.type);

    if (parameter.type === "Boolean") {
        return (
            <InputGroup>
                <FormSelect
                    control={control}
                    name={`parameters.${idx}.value`}
                    options={
                        [
                            { value: true, label: t("parameters.booleanTrue") },
                            { value: false, label: t("parameters.booleanFalse") },
                        ] satisfies SelectOption<boolean>[]
                    }
                    isClearable={false}
                    isSearchable={false}
                />
                <InputGroup.Text>{typeInfo.label}</InputGroup.Text>
            </InputGroup>
        );
    }

    return (
        <FormInput
            type={parameter.type === "Number" ? "number" : "text"}
            control={control}
            name={`parameters.${idx}.value`}
            placeholder={t("parameters.valuePlaceholder", {
                context: typeInfo.exampleValue ? "withExample" : "withoutExample",
                name: parameter.name,
                exampleValue: typeInfo.exampleValue,
            })}
            addon={typeInfo.label}
        />
    );
}
