import React, { useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ResetIndexesButton from "components/pages/database/indexes/list/partials/ResetIndexesButton";
import { IndexSharedInfo } from "components/models/indexes";
import { ExportIndexes } from "components/pages/database/indexes/list/migration/export/ExportIndexes";
import useBoolean from "components/hooks/useBoolean";
import Dropdown from "react-bootstrap/Dropdown";
import genUtils = require("common/generalUtils");
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface IndexSelectActionProps {
    allIndexes: IndexSharedInfo[];
    selectedIndexes: string[];
    replacements: IndexSharedInfo[];
    deleteSelectedIndexes: () => Promise<void>;
    startSelectedIndexes: () => Promise<void>;
    disableSelectedIndexes: () => Promise<void>;
    pauseSelectedIndexes: () => Promise<void>;
    resetSelectedIndexes: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    setLockModeSelectedIndexes: (lockMode: IndexLockMode) => Promise<void>;
    toggleSelectAll: () => void;
    onCancel: () => void;
}

export default function IndexSelectAction(props: IndexSelectActionProps) {
    const {
        allIndexes,
        selectedIndexes,
        deleteSelectedIndexes,
        startSelectedIndexes,
        disableSelectedIndexes,
        pauseSelectedIndexes,
        resetSelectedIndexes,
        setLockModeSelectedIndexes,
        toggleSelectAll,
        onCancel,
    } = props;
    const t = useStudioTranslation("indexesList");
    const tCommon = useStudioTranslation("common");

    const [globalLockChanges] = useState(false);
    // TODO: IDK I just wanted it to compile

    const { value: isExportIndexModalOpen, toggle: toggleIsExportIndexModalOpen } = useBoolean(false);

    const indexNames = allIndexes.map((x) => x.name);
    const selectionState = genUtils.getSelectionState(indexNames, selectedIndexes);

    return (
        <div className="position-relative">
            <Checkbox
                toggleSelection={toggleSelectAll}
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                title={t("selection.selectAllTitle")}
                color="primary"
                size="lg"
                className="ms-3"
            >
                <span className="small-label">{t("selection.selectAll")}</span>
            </Checkbox>

            <SelectionActions active={selectedIndexes.length > 0}>
                <div className="d-flex flex-wrap align-items-center justify-content-center gap-2">
                    <div className="lead text-nowrap">
                        <StudioTrans
                            ns="indexesList"
                            i18nKey="selection.selected"
                            components={{ strong: <strong className="text-emphasis me-1" /> }}
                            options={{ count: selectedIndexes.length }}
                        />
                    </div>
                    <div className="hstack gap-2 flex-wrap justify-content-center">
                        <Button
                            variant="primary"
                            disabled={selectedIndexes.length === 0}
                            onClick={toggleIsExportIndexModalOpen}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="index-export" />
                            <span>{t("selection.export", { count: selectedIndexes.length })}</span>
                        </Button>
                        {isExportIndexModalOpen && (
                            <ExportIndexes
                                toggle={toggleIsExportIndexModalOpen}
                                indexes={allIndexes}
                                selectedNames={selectedIndexes}
                            />
                        )}

                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                title={t("selection.setStateTitle")}
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="play" />}
                                {t("selection.setState")}
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={startSelectedIndexes} title={t("actions.startIndexing")}>
                                    <Icon icon="play" /> <span>{t("actions.startIndexing")}</span>
                                </Dropdown.Item>
                                <Dropdown.Item onClick={disableSelectedIndexes} title={t("actions.disableIndexing")}>
                                    <Icon icon="stop" color="danger" /> <span>{t("actions.disableIndexing")}</span>
                                </Dropdown.Item>
                                <Dropdown.Item onClick={pauseSelectedIndexes} title={t("actions.pauseIndexing")}>
                                    <Icon icon="pause" color="warning" /> <span>{t("actions.pauseIndexing")}</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                title={t("selection.setLockModeTitle")}
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="lock" />}
                                {t("selection.setLockMode")}
                            </Dropdown.Toggle>

                            <Dropdown.Menu>
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("Unlock")}
                                    title={t("selection.unlockTitle")}
                                >
                                    <Icon icon="unlock" /> <span>{t("actions.unlock")}</span>
                                </Dropdown.Item>
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("LockedIgnore")}
                                    title={t("selection.lockTitle")}
                                >
                                    <Icon icon="lock" /> <span>{t("actions.lock")}</span>
                                </Dropdown.Item>
                                <Dropdown.Divider />
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("LockedError")}
                                    title={t("selection.lockErrorTitle")}
                                >
                                    <Icon icon="lock-error" /> <span>{t("actions.lockError")}</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>
                        <ResetIndexesButton isRounded resetIndex={resetSelectedIndexes} />
                        <Button
                            variant="danger"
                            disabled={selectedIndexes.length === 0}
                            onClick={deleteSelectedIndexes}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" />
                            <span>{tCommon("delete")}</span>
                        </Button>
                    </div>
                    <Button onClick={onCancel} variant="link">
                        {tCommon("cancel")}
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
