import React, { useMemo } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
import { formatNumber, formatPercentage } from "./analyzerUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface ResourceRow {
    node: string;
    processCpu?: number;
    machineCpu?: number;
    cores?: number;
    workingSet?: string;
    availableMemory?: string;
    dirtyMemory?: string;
    isHighDirty: boolean;
    gcGeneration?: number;
    gcPause?: number;
}

interface ResourceUsageProps {
    summary: DebugPackageAnalysisSummary;
}

interface ResourceUsageWithSizeProps extends ResourceUsageProps {
    width: number;
}

function useResourceUsageColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const resourceColumns: ColumnDef<ResourceRow>[] = useMemo(
        () => [
            {
                header: t("resourceUsage.columns.node"),
                accessorKey: "node",
                cell: ({ getValue }) => <NodeTagPill tag={getValue<string>()} />,
                size: getSize(7),
                enableSorting: true,
            },
            {
                header: t("resourceUsage.columns.processCpu"),
                accessorKey: "processCpu",
                cell: ({ getValue }) => formatPercentage(getValue<number>()),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: t("resourceUsage.columns.machineCpu"),
                accessorKey: "machineCpu",
                cell: ({ getValue }) => formatPercentage(getValue<number>()),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: t("resourceUsage.columns.cores"),
                accessorKey: "cores",
                cell: ({ getValue }) => formatNumber(getValue<number>()),
                size: getSize(8),
                enableSorting: true,
            },
            {
                header: t("resourceUsage.columns.workingSet"),
                accessorKey: "workingSet",
                cell: ({ getValue }) => getValue<string>() ?? "-",
                size: getSize(13),
            },
            {
                header: t("resourceUsage.columns.availableMemory"),
                accessorKey: "availableMemory",
                cell: ({ getValue }) => getValue<string>() ?? "-",
                size: getSize(16),
            },
            {
                header: t("resourceUsage.columns.dirtyMemory"),
                accessorKey: "dirtyMemory",
                cell: ResourceDirtyMemoryCell,
                size: getSize(13),
            },
            {
                header: t("resourceUsage.columns.lastGc"),
                id: "lastGc",
                accessorFn: (row) => row.gcGeneration ?? -1,
                cell: ({ row }) =>
                    row.original.gcGeneration != null
                        ? t("resourceUsage.gcGeneration", { generation: row.original.gcGeneration })
                        : "-",
                size: getSize(9),
                enableSorting: true,
            },
            {
                header: t("resourceUsage.columns.gcPause"),
                accessorKey: "gcPause",
                cell: ({ getValue }) => formatPercentage(getValue<number>()),
                size: getSize(10),
                enableSorting: true,
            },
        ],
        [getSize, t]
    );

    return { resourceColumns };
}

export default function ResourceUsage({ summary }: ResourceUsageProps) {
    return <SizeGetter render={({ width }) => <ResourceUsageWithSize summary={summary} width={width} />} />;
}

function ResourceUsageWithSize({ summary, width }: ResourceUsageWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const rows = useMemo(() => collectResourceRows(summary), [summary]);
    // a row is created per node, but a node may carry no CPU/memory/GC data at all - only treat the
    // section as populated when at least one node actually reported a resource metric
    const hasResourceData = useMemo(() => rows.some(hasAnyResourceMetric), [rows]);

    const { resourceColumns } = useResourceUsageColumns(width);

    const table = useReactTable({
        data: rows,
        columns: resourceColumns,
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: {
            sorting: [{ id: "node", desc: false }],
        },
        getRowId: (row) => row.node,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(rows.length, 300);

    return (
        <div className="resource-usage">
            <div className="panel-bg-1 rounded">
                <div className="p-4">
                    <h3 className="mb-3">{t("resourceUsage.title")}</h3>
                    {!hasResourceData ? (
                        <EmptySet compact className="justify-content-center">
                            {t("resourceUsage.empty")}
                        </EmptySet>
                    ) : (
                        <VirtualTable table={table} heightInPx={heightInPx} />
                    )}
                </div>
            </div>
        </div>
    );
}

function ResourceDirtyMemoryCell({ row }: { row: { original: ResourceRow } }) {
    return <span className={row.original.isHighDirty ? "text-warning" : ""}>{row.original.dirtyMemory ?? "-"}</span>;
}

function hasAnyResourceMetric(row: ResourceRow): boolean {
    return (
        row.processCpu != null ||
        row.machineCpu != null ||
        row.cores != null ||
        row.workingSet != null ||
        row.availableMemory != null ||
        row.dirtyMemory != null ||
        row.gcGeneration != null ||
        row.gcPause != null
    );
}

function collectResourceRows(summary: DebugPackageAnalysisSummary): ResourceRow[] {
    const rows: ResourceRow[] = [];

    Object.entries(summary.SummaryPerNode ?? {}).forEach(([nodeTag, node]) => {
        const cpu = node.CpuUsageInfo;
        const memory = node.MemoryUsageInfo;
        const gc = node.GcInfo;
        rows.push({
            node: nodeTag,
            processCpu: cpu?.CurrentCpuUsage,
            machineCpu: cpu?.CurrentMachineCpuUsage,
            cores: cpu?.NumberOfCores,
            workingSet: memory?.WorkingSet,
            availableMemory: memory?.AvailableMemory,
            dirtyMemory: memory?.DirtyMemory,
            isHighDirty: memory?.IsHighDirty ?? false,
            gcGeneration: gc?.Generation,
            gcPause: gc?.PauseTimePercentage,
        });
    });

    return rows.sort((a, b) => a.node.localeCompare(b.node));
}
