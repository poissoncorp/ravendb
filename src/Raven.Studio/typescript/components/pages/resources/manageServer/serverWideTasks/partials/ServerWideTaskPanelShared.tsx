import React from "react";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { RichPanelDetailItem, RichPanelName, RichPanelSelect } from "components/common/RichPanel";
import { OngoingTaskOperationConfirmType } from "components/pages/database/tasks/shared/OngoingTaskOperationConfirm";
import { ServerWideTaskSharedInfo } from "../serverWideTaskModels";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface BaseServerWideTaskPanelProps<T extends ServerWideTaskSharedInfo> {
    task: T;
    isSelected: (taskId: number) => boolean;
    toggleSelection: (checked: boolean, task: ServerWideTaskSharedInfo) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isDeleting: (taskId: number) => boolean;
    isTogglingState: (taskId: number) => boolean;
}

export function ServerWideTaskSelect(props: {
    task: ServerWideTaskSharedInfo;
    isSelected: (taskId: number) => boolean;
    toggleSelection: (checked: boolean, task: ServerWideTaskSharedInfo) => void;
}) {
    const { task, isSelected, toggleSelection } = props;

    return (
        <RichPanelSelect>
            <Checkbox
                selected={isSelected(task.taskId)}
                toggleSelection={(e) => toggleSelection(e.currentTarget.checked, task)}
            />
        </RichPanelSelect>
    );
}

export function ServerWideTaskName(props: { task: ServerWideTaskSharedInfo; editUrl: string }) {
    const { task, editUrl } = props;
    const t = useStudioTranslation("serverWideTasks");

    return (
        <RichPanelName>
            <a href={editUrl} title={t("task.nameTitle", { name: task.taskName })}>
                {task.taskName}
            </a>
        </RichPanelName>
    );
}

export function ServerWideTaskResponsibleNode(props: { task: ServerWideTaskSharedInfo }) {
    const { task } = props;
    const t = useStudioTranslation("serverWideTasks");

    if (!task.responsibleNodeTag) {
        return null;
    }

    return (
        <div title={t("task.responsibleNodeTitle")}>
            <Icon icon="cluster-node" />
            {task.responsibleNodeTag}
        </div>
    );
}

export function ServerWideTaskStatus(props: {
    task: ServerWideTaskSharedInfo;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isTogglingState: boolean;
}) {
    const { task, onTaskOperation, isTogglingState } = props;
    const t = useStudioTranslation("serverWideTasks");

    return (
        <Dropdown>
            <Dropdown.Toggle
                disabled={isTogglingState}
                title={t("task.stateTitle")}
                variant={task.taskState === "Disabled" ? "warning" : "secondary"}
            >
                {isTogglingState && <Spinner size="sm" />} {t("task.state", { context: task.taskState })}
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={() => onTaskOperation("enable", [task])}>
                    <Icon icon="play" color="success" /> {t("task.enable")}
                </Dropdown.Item>
                <Dropdown.Item onClick={() => onTaskOperation("disable", [task])}>
                    <Icon icon="stop" color="danger" /> {t("task.disable")}
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
}

export function ServerWideTaskActions(props: {
    task: ServerWideTaskSharedInfo;
    editUrl: string;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, tasks: ServerWideTaskSharedInfo[]) => void;
    isDeleting: boolean;
    detailsVisible: boolean;
    toggleDetails: () => void;
}) {
    const { task, editUrl, onTaskOperation, isDeleting, detailsVisible, toggleDetails } = props;
    const t = useStudioTranslation("serverWideTasks");

    return (
        <ButtonGroup>
            <Button variant="secondary" onClick={toggleDetails} title={t("task.detailsTitle")}>
                <Icon icon={detailsVisible ? "fold" : "unfold"} margin="m-0" />
            </Button>
            <Button variant="secondary" href={editUrl} title={t("task.editTitle")}>
                <Icon icon="edit" margin="m-0" />
            </Button>
            <ButtonWithSpinner
                variant="danger"
                isSpinning={isDeleting}
                onClick={() => onTaskOperation("delete", [task])}
                title={t("task.deleteTitle")}
                spinnerMargin="m-0"
                icon={{ icon: "trash", margin: "m-0" }}
            />
        </ButtonGroup>
    );
}

export function ServerWideTaskExcludedDatabases(props: { task: ServerWideTaskSharedInfo }) {
    const { task } = props;
    const t = useStudioTranslation("serverWideTasks");

    if (task.excludedDatabases.length === 0) {
        return null;
    }

    return (
        <RichPanelDetailItem label={t("task.excludedDatabases")}>
            {task.excludedDatabases.join(", ")}
        </RichPanelDetailItem>
    );
}
