import CollapseButton from "components/common/CollapseButton";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import Collapse from "react-bootstrap/Collapse";
import { UseFieldArrayReturn, useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskTablesExplorer from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTablesExplorer";
import EditCdcSinkTaskTableEditor from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/sections/tables/EditCdcSinkTaskTableEditor";
import useResizableWidth from "components/hooks/useResizableWidth";
import ColumnResize from "components/common/ColumnResize";
import classNames from "classnames";
import { FormErrorIcon } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { useAppSelector } from "components/store";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { useEditCdcSinkTaskTableActions } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskTableActions";
import {
    CdcSinkSourceTable,
    getRelatedSourceTablesToAdd,
    getSourceTableOptionLabel,
    mapRelatedSqlTablesToFormData,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";
import { useCallback, useMemo, useRef } from "react";
import { ColumnDef, getCoreRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { useResizeObserver } from "hooks/useResizeObserver";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface EditCdcSinkTaskTablesSectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskTablesSection({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { value: isPanelOpen, toggle: togglePanel, setTrue: openPanel } = useBoolean(true);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    return (
        <div className="mt-3 vstack pb-3" style={{ minHeight: tablesFieldArray.fields.length > 0 ? "100%" : "300px" }}>
            <div className="hstack align-items-center">
                <h3 className="m-0">{t("tables.title")}</h3>
                <FormErrorIcon control={control} paths={["tables"]} onError={openPanel} />
                <CollapseButton isExpanded={isPanelOpen} toggle={togglePanel} />
            </div>
            <div className="mb-1">{t("tables.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <TablesPanel tablesFieldArray={tablesFieldArray} />
            </Collapse>
        </div>
    );
}

function TablesPanel({ tablesFieldArray }: EditCdcSinkTaskTablesSectionProps) {
    const resizable = useResizableWidth({
        initialWidth: 300,
        minWidth: 190,
        maxWidth: 500,
        placement: "right",
    });

    return (
        <div className="mt-3 vstack flex-grow-1 min-height-0">
            <MissingRelatedTablesAlert />
            <div className="hstack align-items-stretch panel-bg-2 rounded-2 border border-secondary flex-grow-1 min-height-0">
                <div
                    className={classNames("rounded-2 h-100 p-2 position-relative", {
                        "is-dragging": resizable.isDragging,
                    })}
                    style={{ width: resizable.width }}
                >
                    <EditCdcSinkTaskTablesExplorer tablesFieldArray={tablesFieldArray} />
                    <ColumnResize handleMouseDown={resizable.handleMouseDown} placement="right" />
                </div>
                <div className="border-start border-secondary panel-bg-1 rounded-end-2 flex-grow-1 overflow-hidden min-height-0">
                    <EditCdcSinkTaskTableEditor />
                </div>
            </div>
        </div>
    );
}

function MissingRelatedTablesAlert() {
    const t = useStudioTranslation("editCdcSinkTask");
    const sourceSchema = useAppSelector(editCdcSinkTaskSelectors.sourceSchema);
    const tableActions = useEditCdcSinkTaskTableActions();
    const {
        value: isAddTablesModalOpen,
        setTrue: openAddTablesModal,
        setFalse: closeAddTablesModal,
    } = useBoolean(false);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const tables = useWatch({ control, name: "tables" });

    const relatedSourceTables = useMemo(
        () => getRelatedSourceTablesToAdd(sourceSchema, tables ?? []),
        [sourceSchema, tables]
    );

    if (relatedSourceTables.length === 0) {
        return null;
    }

    const handleAddTables = (tablesToAdd: CdcSinkSourceTable[]) => {
        if (tablesToAdd.length === 0) {
            return;
        }

        tableActions.addRootTables(mapRelatedSqlTablesToFormData(sourceSchema, tables ?? [], tablesToAdd));
    };

    return (
        <>
            <RichAlert variant="warning" className="mb-3">
                <div className="d-flex align-items-center gap-2">
                    <div className="flex-grow-1 min-width-0">
                        {t("tables.missingRelatedTables", { count: relatedSourceTables.length })}
                    </div>
                    <Button
                        variant="warning"
                        size="sm"
                        className="text-nowrap align-self-center"
                        onClick={openAddTablesModal}
                    >
                        <Icon icon="plus" />
                        {t("tables.addMissingRootTables", { count: relatedSourceTables.length })}
                    </Button>
                </div>
            </RichAlert>
            {isAddTablesModalOpen && (
                <AddRelatedTablesModal
                    sourceTables={relatedSourceTables}
                    onAdd={handleAddTables}
                    onClose={closeAddTablesModal}
                />
            )}
        </>
    );
}

interface AddRelatedTablesModalProps {
    sourceTables: CdcSinkSourceTable[];
    onAdd: (sourceTables: CdcSinkSourceTable[]) => void;
    onClose: () => void;
}

function AddRelatedTablesModal({ sourceTables, onAdd, onClose }: AddRelatedTablesModalProps) {
    const t = useStudioTranslation("editCdcSinkTask");
    const commonT = useStudioTranslation("common");
    const tableWrapperRef = useRef<HTMLDivElement>(null);
    const { width } = useResizeObserver({ ref: tableWrapperRef });
    const columns = useAddRelatedTablesColumns(width ?? 0);
    const table = useReactTable({
        data: sourceTables,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getRowId: getSourceTableOptionLabel,
        initialState: {
            rowSelection: Object.fromEntries(
                sourceTables.map((sourceTable) => [getSourceTableOptionLabel(sourceTable), true])
            ),
            sorting: [
                {
                    id: "TableName",
                    desc: false,
                },
            ],
        },
    });

    const selectedSourceTables = table.getSelectedRowModel().rows.map((row) => row.original);

    const handleAdd = () => {
        onAdd(selectedSourceTables);
        onClose();
    };

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary" size="lg">
            <Modal.Header onCloseClick={onClose} className="pb-0">
                <h3 className="m-0">{t("tables.addRootTablesTitle")}</h3>
            </Modal.Header>
            <Modal.Body className="vstack gap-3">
                <div ref={tableWrapperRef}>
                    <VirtualTable
                        table={table}
                        heightInPx={virtualTableUtils.getHeightInPx(sourceTables.length, 300)}
                    />
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={onClose}>
                    {commonT("cancel")}
                </Button>
                <Button
                    variant="primary"
                    className="rounded-pill"
                    onClick={handleAdd}
                    disabled={selectedSourceTables.length === 0}
                >
                    <Icon icon="plus" />
                    {t("tables.addSelectedRootTables", { count: selectedSourceTables.length })}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function useAddRelatedTablesColumns(widthPx: number) {
    const t = useStudioTranslation("editCdcSinkTask");
    const dynamicWidth = widthPx ? widthPx - columnCheckbox.size : 0;
    const bodyWidth = virtualTableUtils.getTableBodyWidth(dynamicWidth);
    const getSize = useCallback(virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    return useMemo<ColumnDef<CdcSinkSourceTable>[]>(
        () => [
            columnCheckbox as ColumnDef<CdcSinkSourceTable>,
            {
                id: "TableName",
                header: t("columns.tableName"),
                accessorFn: getSourceTableOptionLabel,
                cell: CellValueWrapper,
                size: getSize(100),
            },
        ],
        [getSize, t]
    );
}
