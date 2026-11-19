import { EmptySet } from "components/common/EmptySet";
import {
    FormErrorIcon,
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSwitch,
    OptionalLabel,
} from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useFormContext } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import Collapse from "react-bootstrap/Collapse";
import CollapseButton from "components/common/CollapseButton";
import { SelectOption } from "components/common/select/Select";
import useEditAiAgentParametersSection from "../hooks/useEditAiAgentParametersSection";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

export default function EditAiAgentParametersSection() {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();
    const parametersEditor = useEditAiAgentParametersSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    {t("parameters.heading")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="parameters.headingInfo"
                                components={{ br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["parameters"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("parameters.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-secondary border border-secondary small">
                                    <Icon icon="settings" margin="m-0" className="text-body" />
                                </div>
                                <div>{t("parameters.listTitle")}</div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={parametersEditor.handleAdd}>
                                <Icon icon="plus" />
                                {t("parameters.add")}
                            </Button>
                        </div>
                        <div className="vstack">
                            {parametersEditor.fieldArray.fields.length === 0 ? (
                                <div className="well p-2 rounded-2 border border-secondary mt-2 d-flex justify-content-center align-items-center">
                                    <EmptySet compact className="text-muted">
                                        {t("parameters.empty")}
                                    </EmptySet>
                                </div>
                            ) : (
                                <div className="parameters-cards vstack">
                                    {parametersEditor.fieldArray.fields.map((field, index) => (
                                        <ParameterItem
                                            key={field.id}
                                            index={index}
                                            parameterItem={parametersEditor.parameters[index]}
                                            remove={() => parametersEditor.handleRemove(index)}
                                            save={() => parametersEditor.handleSave(index)}
                                            edit={() => parametersEditor.handleEdit(index)}
                                        />
                                    ))}
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}

interface ParameterItemProps {
    index: number;
    parameterItem: EditAiAgentFormData["parameters"][number];
    remove: () => void;
    save: () => Promise<void>;
    edit: () => void;
}

function ParameterItem({ index, parameterItem, remove, save, edit }: ParameterItemProps) {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();

    if (!parameterItem) {
        return null;
    }

    if (!parameterItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 d-flex justify-content-between align-items-center gap-2">
                <div className="parameter-summary min-width-0 flex-grow-1">
                    <div className="d-flex align-items-center gap-1 min-width-0">
                        <h4 className="m-0 text-truncate" title={parameterItem.name}>
                            {parameterItem.name}
                        </h4>
                        <span className="small text-secondary">|</span>
                        <span className="fs-4 flex-shrink-0 text-monospace text-muted">
                            {getParameterTypeLabel(parameterItem.type, t)}
                        </span>
                    </div>
                    <small className="d-block text-truncate" title={parameterItem.description}>
                        {parameterItem.description}
                    </small>
                </div>
                <div className="hstack gap-2 flex-shrink-0">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={edit}>
                        <Icon icon="chevron-down" margin="m-0" />
                    </Button>
                </div>
            </div>
        );
    }

    return (
        <div className="well p-2 rounded-2 border border-secondary mt-2">
            <div className="hstack justify-content-between">
                <h4 className="m-0">{t("parameters.configure")}</h4>
                <div className="hstack gap-2">
                    <Button variant="danger" onClick={remove}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                    <Button variant="secondary" onClick={save}>
                        <Icon icon="chevron-up" margin="m-0" />
                    </Button>
                </div>
            </div>
            <hr className="mt-2 mb-3" />
            <div className="grid gap-2 mb-3">
                <FormGroup marginClass="m-0" className="g-col-6">
                    <FormLabel>{t("parameters.nameLabel")}</FormLabel>
                    <FormInput
                        type="text"
                        control={control}
                        name={`parameters.${index}.name`}
                        placeholder={t("parameters.namePlaceholder")}
                    />
                </FormGroup>
                <FormGroup marginClass="m-0" className="g-col-6">
                    <FormLabel>{t("parameters.typeLabel")}</FormLabel>
                    <FormSelect
                        control={control}
                        name={`parameters.${index}.type`}
                        options={getTypeOptions(t)}
                        placeholder={t("parameters.typePlaceholder")}
                        isSearchable={false}
                        isClearable={false}
                    />
                </FormGroup>
            </div>
            <FormGroup>
                <FormLabel>
                    {t("shared.description")} <OptionalLabel />
                </FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    rows={3}
                    control={control}
                    name={`parameters.${index}.description`}
                    placeholder={t("parameters.descriptionPlaceholder")}
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    {t("parameters.policyLabel")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editAiAgent"
                                i18nKey="parameters.policyInfo"
                                components={{ b: <b />, br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info-new" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormSelect
                    control={control}
                    name={`parameters.${index}.policy`}
                    options={getPolicyOptions(t)}
                    placeholder={t("parameters.policyPlaceholder")}
                    isSearchable={false}
                    isClearable={false}
                />
            </FormGroup>
            <FormSwitch control={control} name={`parameters.${index}.isSendToModel`}>
                {t("parameters.sendToModel")}
                <PopoverWithHoverWrapper
                    message={
                        <StudioTrans
                            ns="editAiAgent"
                            i18nKey="parameters.sendToModelInfo"
                            components={{ br: <br /> }}
                        />
                    }
                >
                    <Icon icon="info-new" margin="ms-1" />
                </PopoverWithHoverWrapper>
            </FormSwitch>
        </div>
    );
}

function getParameterTypeLabel(
    type: Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType,
    t: StudioTranslate<"editAiAgent">
) {
    return getTypeOptions(t).find((option) => option.value === type)?.label ?? type;
}

function getTypeOptions(
    t: StudioTranslate<"editAiAgent">
): SelectOption<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterValueType>[] {
    return [
        { value: "String", label: t("parameters.types.string") },
        { value: "Number", label: t("parameters.types.number") },
        { value: "Boolean", label: t("parameters.types.boolean") },
        { value: "ArrayOfString", label: t("parameters.types.arrayOfString") },
        { value: "ArrayOfNumber", label: t("parameters.types.arrayOfNumber") },
        { value: "ArrayOfBoolean", label: t("parameters.types.arrayOfBoolean") },
        { value: "Default", label: t("parameters.types.any") },
        { value: "Null", label: t("parameters.types.null") },
    ];
}

function getPolicyOptions(
    t: StudioTranslate<"editAiAgent">
): SelectOption<Raven.Client.Documents.Operations.AI.Agents.AiAgentParameterPolicy>[] {
    return [
        { value: "Default", label: t("parameters.policies.default") },
        { value: "ForbidModelGeneration", label: t("parameters.policies.forbidModelGeneration") },
    ];
}
