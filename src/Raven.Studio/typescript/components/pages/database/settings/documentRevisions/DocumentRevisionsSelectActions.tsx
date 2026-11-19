import React from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { documentRevisionsActions } from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function DocumentRevisionsSelectActions() {
    const t = useStudioTranslation("documentRevisions");
    const commonT = useStudioTranslation("common");
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();

    const allConfigsNames = useAppSelector(documentRevisionsSelectors.allConfigsNames);
    const selectedConfigsNames = useAppSelector(documentRevisionsSelectors.selectedConfigNames);

    if (allConfigsNames.length === 0) {
        return null;
    }

    const selectionState = genUtils.getSelectionState(allConfigsNames, selectedConfigsNames);

    const toggleAll = () => {
        reportEvent("revisions", "toggle-select-all");
        dispatch(documentRevisionsActions.allSelectedConfigNamesToggled());
    };

    return (
        <div className="position-relative">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleAll}
                color="primary"
                title={t("selectActions.selectAllTitle")}
                size="lg"
            >
                <span className="small-label">{t("selectActions.selectAll")}</span>
            </Checkbox>

            <SelectionActions active={selectionState !== "Empty"}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <StudioTrans
                            ns="documentRevisions"
                            i18nKey="selectActions.selectedCount"
                            options={{ count: selectedConfigsNames.length }}
                            components={{ strong: <strong className="text-emphasis me-1" /> }}
                        />
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <Dropdown>
                            <Dropdown.Toggle
                                as={CustomDropdownToggle}
                                variant="secondary"
                                isCaretHidden
                                title={t("selectActions.setStateTitle")}
                                className="rounded-pill"
                            >
                                <Icon icon="play" /> {t("selectActions.setState")}
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    title={t("selectActions.enable")}
                                    onClick={() => dispatch(documentRevisionsActions.selectedConfigsEnabled())}
                                >
                                    <Icon icon="play" color="success" />
                                    <span>{t("selectActions.enable")}</span>
                                </Dropdown.Item>
                                <Dropdown.Item
                                    title={t("selectActions.disable")}
                                    onClick={() => dispatch(documentRevisionsActions.selectedConfigsDisabled())}
                                >
                                    <Icon icon="stop" color="danger" />
                                    <span>{t("selectActions.disable")}</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <Button
                            variant="danger"
                            onClick={() => dispatch(documentRevisionsActions.selectedConfigsDeleted())}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" /> {commonT("delete")}
                        </Button>
                    </ButtonGroup>
                    <Button
                        onClick={() => dispatch(documentRevisionsActions.allSelectedConfigNamesToggled())}
                        variant="link"
                    >
                        {commonT("cancel")}
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
