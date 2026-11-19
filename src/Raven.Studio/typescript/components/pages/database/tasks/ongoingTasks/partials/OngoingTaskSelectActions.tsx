import React from "react";
import genUtils from "common/generalUtils";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import Spinner from "react-bootstrap/Spinner";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { OngoingTaskOperationConfirmType } from "../../shared/OngoingTaskOperationConfirm";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface OngoingTaskSelectActionsProps {
    allTasks: number[];
    selectedTasks: number[];
    setSelectedTasks: (ids: number[]) => void;
    onTaskOperation: (x: OngoingTaskOperationConfirmType) => void;
    isTogglingState: boolean;
    isDeleting: boolean;
}

export default function OngoingTaskSelectActions(props: OngoingTaskSelectActionsProps) {
    const { allTasks, selectedTasks, setSelectedTasks, onTaskOperation, isTogglingState, isDeleting } = props;
    const t = useStudioTranslation("ongoingTasks");
    const tCommon = useStudioTranslation("common");

    if (allTasks.length === 0) {
        return null;
    }

    const anythingSelected = selectedTasks.length > 0;
    const selectionState = genUtils.getSelectionState(allTasks, selectedTasks);

    const toggleSelectAll = () => {
        if (selectionState === "Empty") {
            setSelectedTasks([...selectedTasks, ...allTasks]);
        } else {
            setSelectedTasks(selectedTasks.filter((x) => !allTasks.includes(x)));
        }
    };

    return (
        <div className="position-relative mt-3 ms-3">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleSelectAll}
                color="primary"
                title={t("selectActions.selectAllTitle")}
                size="lg"
            >
                <span className="small-label">{t("selectActions.selectAll")}</span>
            </Checkbox>

            <SelectionActions active={anythingSelected}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <StudioTrans
                            ns="ongoingTasks"
                            i18nKey="selectActions.selected"
                            options={{ count: selectedTasks.length }}
                            components={{ strong: <strong className="text-emphasis me-1" /> }}
                        />
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                disabled={!anythingSelected || isTogglingState}
                                title={t("selectActions.setStateTitle")}
                                className="rounded-pill"
                            >
                                {isTogglingState ? <Spinner size="sm" /> : <Icon icon="play" />}{" "}
                                {t("selectActions.setState")}
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    title={t("selectActions.enable")}
                                    onClick={() => onTaskOperation("enable")}
                                >
                                    <Icon icon="play" color="success" />
                                    <span>{t("selectActions.enable")}</span>
                                </Dropdown.Item>
                                <Dropdown.Item
                                    title={t("selectActions.disable")}
                                    onClick={() => onTaskOperation("disable")}
                                >
                                    <Icon icon="stop" color="danger" />
                                    <span>{t("selectActions.disable")}</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <ButtonWithSpinner
                            variant="danger"
                            onClick={() => onTaskOperation("delete")}
                            className="rounded-pill flex-grow-0"
                            isSpinning={isDeleting}
                            icon="trash"
                        >
                            {tCommon("delete")}
                        </ButtonWithSpinner>
                    </ButtonGroup>
                    <Button onClick={() => setSelectedTasks([])} variant="link">
                        {tCommon("cancel")}
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
