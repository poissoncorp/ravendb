import React, { useCallback, useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import { useAppDispatch, useAppSelector } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import { useEventsCollector } from "hooks/useEventsCollector";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import { Icon } from "components/common/Icon";
import {
    changeDatabasesLockMode,
    confirmToggleDatabases,
    toggleDatabases,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import useConfirm from "components/common/ConfirmDialog";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Dropdown from "react-bootstrap/Dropdown";
import genUtils = require("common/generalUtils");
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DatabasesSelectActionsProps {
    selectedDatabases: DatabaseSharedInfo[];
    databaseNames: string[];
    setSelectedDatabaseNames: (x: string[]) => void;
}

export function DatabasesSelectActions({
    selectedDatabases,
    databaseNames,
    setSelectedDatabaseNames,
}: DatabasesSelectActionsProps) {
    const t = useStudioTranslation("databases");
    const tCommon = useStudioTranslation("common");
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const { reportEvent } = useEventsCollector();

    const [lockChanges, setLockChanges] = useState(false);
    const [toggleChanges, setToggleChanges] = useState(false);
    const [deleteChanges, setDeleteChanges] = useState(false);

    const dispatch = useAppDispatch();
    const confirm = useConfirm();

    const canDeleteSelection = selectedDatabases.some((x) => x.lockMode === "Unlock");
    const anythingSelected = selectedDatabases.length > 0;
    const selectedDatabaseNames = selectedDatabases.map((x) => x.name);

    const selectionState = genUtils.getSelectionState(
        databaseNames,
        selectedDatabases.map((x) => x.name)
    );

    const toggleSelectAll = useCallback(() => {
        if (selectionState === "Empty") {
            setSelectedDatabaseNames([...selectedDatabaseNames, ...databaseNames]);
        } else {
            setSelectedDatabaseNames(selectedDatabaseNames.filter((x) => !databaseNames.includes(x)));
        }
    }, [databaseNames, selectedDatabaseNames, selectionState, setSelectedDatabaseNames]);

    if (!isOperatorOrAbove) {
        // no access
        return null;
    }

    const onChangeLockMode = async (lockMode: DatabaseLockMode) => {
        const dbs = selectedDatabases;

        reportEvent("databases", "set-lock-mode", lockMode);

        const isConfirmed = await confirm({
            title: t("changeLockModeConfirm"),
        });

        if (isConfirmed) {
            setLockChanges(true);
            try {
                await dispatch(changeDatabasesLockMode(dbs, lockMode));
            } finally {
                setLockChanges(false);
            }
        }
    };

    const onToggleDatabases = async (enable: boolean) => {
        const dbs = selectedDatabases;
        const result = await dispatch(confirmToggleDatabases(dbs, enable));

        if (result) {
            setToggleChanges(true);
            try {
                await dispatch(toggleDatabases(dbs, enable));
            } finally {
                setToggleChanges(false);
            }
        }
    };

    const onDelete = async () => {
        const result = await dispatch(databaseActions.confirmDeleteDatabases(selectedDatabases));

        if (result.can) {
            setDeleteChanges(true);
            try {
                await dispatch(databaseActions.deleteDatabases(result.databases, result.keepFiles));
            } finally {
                setDeleteChanges(false);
            }
        }
    };

    return (
        <div className="position-relative px-4 pt-3">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleSelectAll}
                color="primary"
                title={t("selection.selectAllTitle")}
                size="lg"
                className="ms-5"
            >
                <span className="small-label">{t("selection.selectAll")}</span>
            </Checkbox>

            <SelectionActions active={anythingSelected && !toggleChanges}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <StudioTrans
                            ns="databases"
                            i18nKey="selection.selectedCount"
                            options={{ count: selectedDatabases.length }}
                            components={{ strong: <strong className="text-emphasis me-1" /> }}
                        />
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        {isOperatorOrAbove && (
                            <Dropdown>
                                <Dropdown.Toggle
                                    variant="secondary"
                                    disabled={!anythingSelected || toggleChanges}
                                    title={t("selection.setStateTitle")}
                                    className="rounded-pill"
                                >
                                    {toggleChanges ? <Spinner size="sm" /> : <Icon icon="play" />}{" "}
                                    {t("selection.setState")}
                                </Dropdown.Toggle>
                                <Dropdown.Menu>
                                    <Dropdown.Item title={t("actions.enable")} onClick={() => onToggleDatabases(true)}>
                                        <Icon icon="unlock" />
                                        <span>{t("actions.enable")}</span>
                                    </Dropdown.Item>
                                    <Dropdown.Item
                                        title={t("actions.disable")}
                                        onClick={() => onToggleDatabases(false)}
                                    >
                                        <Icon icon="lock" />
                                        <span>{t("actions.disable")}</span>
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        )}

                        {isOperatorOrAbove && (
                            <Dropdown>
                                <Dropdown.Toggle
                                    variant="secondary"
                                    title={t("selection.setLockModeTitle")}
                                    disabled={!anythingSelected || lockChanges}
                                    className="rounded-pill"
                                >
                                    {lockChanges ? <Spinner size="sm" /> : <Icon icon="lock" />}{" "}
                                    {t("selection.setLockMode")}
                                </Dropdown.Toggle>
                                <Dropdown.Menu>
                                    <Dropdown.Item
                                        onClick={() => onChangeLockMode("Unlock")}
                                        title={t("selection.allowDeleteTitle")}
                                    >
                                        <Icon icon="trash" addon="check" />
                                        <span>{t("selection.allowDelete")}</span>
                                    </Dropdown.Item>
                                    <Dropdown.Item
                                        onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                        title={t("selection.preventDeleteTitle")}
                                    >
                                        <Icon icon="trash" addon="cancel" />
                                        <span>{t("selection.preventDelete")}</span>
                                    </Dropdown.Item>
                                    <Dropdown.Item
                                        onClick={() => onChangeLockMode("PreventDeletesError")}
                                        title={t("selection.preventDeleteWithErrorTitle")}
                                    >
                                        <Icon icon="trash" addon="exclamation" />
                                        <span>{t("selection.preventDeleteWithError")}</span>
                                    </Dropdown.Item>
                                </Dropdown.Menu>
                            </Dropdown>
                        )}
                        {isOperatorOrAbove && (
                            <ButtonWithSpinner
                                variant="danger"
                                onClick={onDelete}
                                disabled={!canDeleteSelection || deleteChanges}
                                className="rounded-pill flex-grow-0"
                                icon="trash"
                                isSpinning={deleteChanges}
                            >
                                {tCommon("delete")}
                            </ButtonWithSpinner>
                        )}
                    </ButtonGroup>
                    <Button onClick={() => setSelectedDatabaseNames([])} variant="link">
                        {tCommon("cancel")}
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
