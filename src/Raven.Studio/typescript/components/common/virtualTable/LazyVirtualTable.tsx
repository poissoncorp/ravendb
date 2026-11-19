import "./VirtualTable.scss";
import VirtualTableBodyWrapper from "./partials/VirtualTableBodyWrapper";
import LazyVirtualTableRow from "./partials/LazyVirtualTableRow";
import LazyVirtualTablePaginationBar from "./partials/LazyVirtualTablePaginationBar";
import LazyVirtualTableDomLimitBanner from "./partials/LazyVirtualTableDomLimitBanner";
import { LazyRows } from "./hooks/useLazyRows";
import { useLazyTableViewport } from "./hooks/useLazyTableViewport";
import { useSelectionPreview } from "./hooks/useSelectionPreview";
import { ReactNode } from "react";
import { Table as TanstackTable } from "@tanstack/react-table";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface LazyVirtualTableProps<T> {
    table: TanstackTable<T>;
    lazyRows: LazyRows<T>;
    isPaginated?: boolean;
    onIsPaginatedChange?: (isPaginated: boolean) => void;
    heightInPx?: number;
    emptyMessage?: ReactNode;
    itemsName?: string;
    bottomOverlay?: ReactNode;
}

export default function LazyVirtualTable<T>({
    table,
    lazyRows,
    isPaginated = false,
    onIsPaginatedChange,
    heightInPx,
    emptyMessage,
    itemsName: itemsNameProp,
    bottomOverlay,
}: LazyVirtualTableProps<T>) {
    const t = useStudioTranslation("virtualTable");
    const itemsName = itemsNameProp ?? t("table.defaultItemsName");
    const viewport = useLazyTableViewport({
        lazyRows,
        isPaginated,
        setIsPaginated: onIsPaginatedChange,
        fixedHeightInPx: heightInPx,
    });
    const selectionPreview = useSelectionPreview(table.options.meta?.lazySelection);

    const { columnSizing, columnVisibility, columnOrder, columnPinning } = table.getState();
    const cellsRenderDependencies = [table.options.columns, columnSizing, columnVisibility, columnOrder, columnPinning];

    return (
        <div className="lazy-virtual-table">
            <div ref={viewport.areaRef} className="lazy-virtual-table-area">
                <VirtualTableBodyWrapper
                    table={table}
                    tableContainerRef={viewport.containerRef}
                    onScroll={viewport.onScroll}
                    isLoading={viewport.isLoading}
                    isEmpty={viewport.isEmpty}
                    emptyMessage={emptyMessage}
                    heightInPx={viewport.heightInPx}
                    overlay={
                        (viewport.error || viewport.isDomLimitBannerVisible || bottomOverlay) && (
                            <div className="floating-bars-container">
                                {viewport.error && (
                                    <div className="floating-bar text-nowrap" data-testid="fetch-error-banner">
                                        <span className="text-danger">{t("table.loadError", { itemsName })}</span>
                                        <Button variant="link" className="p-0" onClick={lazyRows.retry}>
                                            {t("table.retry")}
                                        </Button>
                                    </div>
                                )}
                                {viewport.isDomLimitBannerVisible && (
                                    <LazyVirtualTableDomLimitBanner
                                        itemsName={itemsName}
                                        canPaginate={lazyRows.fetchMode === "skipTake" && onIsPaginatedChange != null}
                                        onTurnOnPagination={viewport.turnOnPagination}
                                    />
                                )}
                                {bottomOverlay}
                            </div>
                        )
                    }
                >
                    <tbody
                        style={{ height: viewport.bodyHeightInPx }}
                        onMouseMove={selectionPreview.onMouseMove}
                        onMouseLeave={selectionPreview.onMouseLeave}
                    >
                        {table.getRowModel().rows.map((row) => {
                            const rowIndex = lazyRows.rows[row.index].index;
                            const positionInPage = rowIndex - viewport.firstRowIndex;

                            return (
                                <LazyVirtualTableRow
                                    key={positionInPage}
                                    row={row}
                                    rowIndex={rowIndex}
                                    positionInPage={positionInPage}
                                    isSelected={row.getIsSelected()}
                                    isSelectionPreview={selectionPreview.isPreviewed(rowIndex)}
                                    renderDependencies={cellsRenderDependencies}
                                />
                            );
                        })}
                    </tbody>
                </VirtualTableBodyWrapper>
            </div>
            {viewport.pagination && <LazyVirtualTablePaginationBar pagination={viewport.pagination} />}
        </div>
    );
}
