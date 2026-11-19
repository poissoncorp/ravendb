import {
    useReactTable,
    getCoreRowModel,
    getSortedRowModel,
    ColumnDef,
    getFilteredRowModel,
} from "@tanstack/react-table";
import SizeGetter from "components/common/SizeGetter";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import TableDisplaySettings from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { useState } from "react";
import Badge from "react-bootstrap/Badge";
import document from "models/database/documents/document";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type TestResultTab = "results" | "diagnostics";

interface DiagnosticsItem {
    Message: string;
}

interface DatabaseCustomSorterTestResultProps {
    result: pagedResultExtended<document>;
}

interface DatabaseCustomSorterTestResultWithSizeProps extends DatabaseCustomSorterTestResultProps {
    availableWidth: number;
}

export default function DatabaseCustomSorterTestResult(props: DatabaseCustomSorterTestResultProps) {
    return (
        <SizeGetter
            render={({ width }) => <DatabaseCustomSorterTestResultWithSize {...props} availableWidth={width} />}
        />
    );
}

function DatabaseCustomSorterTestResultWithSize({
    result,
    availableWidth,
}: DatabaseCustomSorterTestResultWithSizeProps) {
    const t = useStudioTranslation("customSorters");
    const [currentTab, setCurrentTab] = useState<TestResultTab>("results");

    const tableBodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);

    const { columnDefs: resultsColumnDefs, initialColumnVisibility } = useDocumentColumnsProvider({
        documents: result?.items || [],
        availableWidth: tableBodyWidth,
        hasPreview: true,
    });

    const [columnVisibility, setColumnVisibility] = useState<Record<string, boolean>>(initialColumnVisibility);

    const resultsTable = useReactTable({
        columns: resultsColumnDefs,
        data: result?.items || [],
        columnResizeMode: "onChange",
        state: {
            columnVisibility,
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        onColumnVisibilityChange: setColumnVisibility,
        getFilteredRowModel: getFilteredRowModel(),
    });

    const diagnosticsTable = useReactTable({
        columns: getDiagnosticsColumns(t, tableBodyWidth),
        data: result?.additionalResultInfo?.Diagnostics || [],
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <div>
            <h3>{t("test.resultsHeading")}</h3>
            {currentTab === "results" && (
                <>
                    <span className="text-muted small">{t("test.resultsLimit")}</span>
                    <div className="d-flex justify-content-end mb-2">
                        <TableDisplaySettings table={resultsTable} />
                    </div>
                    <VirtualTable table={resultsTable} heightInPx={400} />
                </>
            )}
            {currentTab === "diagnostics" && <VirtualTable table={diagnosticsTable} heightInPx={400} />}

            <div className="d-flex mt-2 gap-2">
                <Button
                    variant="secondary"
                    size="sm"
                    className="rounded-pill"
                    onClick={() => setCurrentTab("results")}
                    active={currentTab === "results"}
                >
                    {t("test.resultsTab")}
                    <Badge bg="primary" className="ms-1">
                        {result.items.length}
                    </Badge>
                </Button>
                <Button
                    size="sm"
                    className="rounded-pill"
                    variant="secondary"
                    onClick={() => setCurrentTab("diagnostics")}
                    active={currentTab === "diagnostics"}
                >
                    {t("test.diagnosticsTab")}
                    <Badge bg="primary" className="ms-1">
                        {result.additionalResultInfo?.Diagnostics?.length}
                    </Badge>
                </Button>
            </div>
        </div>
    );
}

const getDiagnosticsColumns = (t: StudioTranslate<"customSorters">, tableWidth = 0): ColumnDef<DiagnosticsItem>[] => {
    return [
        {
            header: t("test.messageColumn"),
            accessorFn: (x) => x,
            cell: CellWithCopyWrapper,
            size: tableWidth,
        },
    ];
};
