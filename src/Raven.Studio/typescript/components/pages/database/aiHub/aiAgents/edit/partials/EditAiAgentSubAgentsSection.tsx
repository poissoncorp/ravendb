import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import useEditAiAgentSubAgentsSection from "components/pages/database/aiHub/aiAgents/edit/hooks/useEditAiAgentSubAgentsSection";
import { useAppSelector } from "components/store";
import { editAiAgentSelectors } from "components/pages/database/aiHub/aiAgents/edit/store/editAiAgentSlice";
import { SelectOption } from "components/common/select/Select";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditAiAgentSubAgentsSection() {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();
    const { fieldArray, handleAdd, handleSave, handleEdit, handleRemove } = useEditAiAgentSubAgentsSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    {t("subAgents.heading")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans ns="editAiAgent" i18nKey="subAgents.headingInfo" components={{ br: <br /> }} />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["subAgents"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("subAgents.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary small">
                                    <Icon icon="sub-agents" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    {t("subAgents.listTitle")}
                                    <PopoverWithHoverWrapper message={t("subAgents.listInfo")}>
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={handleAdd}>
                                <Icon icon="plus" />
                                {t("subAgents.add")}
                            </Button>
                        </div>
                        <div className="vstack">
                            {fieldArray.fields.map((field, index) => (
                                <SubAgentItem
                                    key={field.id}
                                    index={index}
                                    remove={() => handleRemove(index)}
                                    save={() => handleSave(index)}
                                    edit={() => handleEdit(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}

interface SubAgentItemProps {
    index: number;
    remove: () => void;
    save: () => void;
    edit: () => void;
}

function SubAgentItem({ index, remove, save, edit }: SubAgentItemProps) {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();

    const currentIdentifier = useWatch({
        control,
        name: "identifier",
    });

    const subAgents = useWatch({
        control,
        name: "subAgents",
    });

    const allIdentifiersOptions: SelectOption[] = useAppSelector(editAiAgentSelectors.allIdentifiers)
        .filter((id) => id !== currentIdentifier)
        .map((id) => ({
            label: id,
            value: id,
        }));

    const subAgentItem = subAgents[index];

    const isAgentNotFound =
        subAgentItem.identifier && !allIdentifiersOptions.some((option) => option.value === subAgentItem.identifier);

    if (!subAgentItem.isEditing) {
        return (
            <div className="well p-2 rounded-2 border border-secondary mt-2 hstack justify-content-between align-items-center gap-3">
                <div className="tool-info">
                    <h4 className="m-0">{subAgentItem.identifier}</h4>
                    <small className="tool-description">{subAgentItem.description}</small>
                </div>
                <div className="hstack gap-2">
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
                <h4 className="m-0">{t("subAgents.configure")}</h4>
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
            <FormGroup>
                <FormLabel>{t("subAgents.identifierLabel")}</FormLabel>
                <FormSelectAutocomplete
                    control={control}
                    name={`subAgents.${index}.identifier`}
                    options={allIdentifiersOptions}
                />
                {isAgentNotFound && <div className="text-warning mt-1 fw-bold">{t("subAgents.notFound")}</div>}
            </FormGroup>
            <FormGroup>
                <FormLabel>{t("shared.description")}</FormLabel>
                <FormInput
                    type="textarea"
                    as="textarea"
                    control={control}
                    name={`subAgents.${index}.description`}
                    rows={4}
                    placeholder={t("subAgents.descriptionPlaceholder")}
                />
            </FormGroup>
        </div>
    );
}
