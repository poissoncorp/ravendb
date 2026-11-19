import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import useEditAiAgentToolsSection from "../hooks/useEditAiAgentToolsSection";
import EditAiAgentQueryToolItem from "./EditAiAgentQueryToolItem";
import EditAiAgentActionToolItem from "./EditAiAgentActionToolItem";
import useBoolean from "components/hooks/useBoolean";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon } from "components/common/Form";
import { useFormContext } from "react-hook-form";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditAiAgentToolsSection() {
    const t = useStudioTranslation("editAiAgent");
    const { control } = useFormContext<EditAiAgentFormData>();
    const toolsEditor = useEditAiAgentToolsSection();

    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);

    return (
        <>
            <div className="hstack mt-3">
                <h3 className="m-0">
                    {t("tools.heading")}
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans ns="editAiAgent" i18nKey="tools.headingInfo" components={{ br: <br /> }} />
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["queries", "actions"]} onError={() => setIsPanelOpen(true)} />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("tools.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="query" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    {t("tools.queryToolsTitle")}
                                    <PopoverWithHoverWrapper
                                        message={
                                            <StudioTrans
                                                ns="editAiAgent"
                                                i18nKey="tools.queryToolsInfo"
                                                components={{ br: <br /> }}
                                            />
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddQuery}>
                                <Icon icon="plus" />
                                {t("tools.addQueryTool")}
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.queriesFieldArray.fields.map((field, index) => (
                                <EditAiAgentQueryToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveQuery(index)}
                                    save={() => toolsEditor.handleSaveQuery(index)}
                                    edit={() => toolsEditor.handleEditQuery(index)}
                                />
                            ))}
                        </div>
                    </div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary mt-2">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="force" color="primary" margin="m-0" />
                                </div>
                                <div>
                                    {t("tools.actionToolsTitle")}
                                    <PopoverWithHoverWrapper
                                        message={
                                            <StudioTrans
                                                ns="editAiAgent"
                                                i18nKey="tools.actionToolsInfo"
                                                components={{ br: <br /> }}
                                            />
                                        }
                                    >
                                        <Icon icon="info-new" />
                                    </PopoverWithHoverWrapper>
                                </div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddAction}>
                                <Icon icon="plus" />
                                {t("tools.addActionTool")}
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.actionsFieldArray.fields.map((field, index) => (
                                <EditAiAgentActionToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveAction(index)}
                                    save={() => toolsEditor.handleSaveAction(index)}
                                    edit={() => toolsEditor.handleEditAction(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
        </>
    );
}
