import React, { useMemo, useState } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { RichAlert } from "components/common/RichAlert";
import { Icon } from "components/common/Icon";
import StatTile from "./StatTile";
import genUtils from "common/generalUtils";
import { formatNumber, formatPercentage } from "./analyzerUtils";
import Button from "react-bootstrap/Button";
import appUrl from "common/appUrl";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import classNames from "classnames";
import SizeGetter from "components/common/SizeGetter";
import SegmentedControl from "components/common/SegmentedControl";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type CpuUsageAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.CpuUsageAnalysisInfo;
type MemoryAnalysisInfo =
    Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory.MemoryAnalysisInfo;
type GcMemoryInfo = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GcMemoryInfo;
type GenerationInfoSize = Raven.Server.Dashboard.Cluster.Notifications.GcInfoPayload.GenerationInfoSize;
type TcpConnections = Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.TcpConnections;
type PingResult = Raven.Server.Documents.Handlers.Debugging.NodeDebugHandler.PingResult;
type ThreadInfo = Raven.Server.Dashboard.ThreadInfo;

type MetricTab = "cpu" | "memory" | "gc" | "network" | "threads";

interface PerformanceMetricsProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

interface PerformanceMetricsWithSizeProps extends PerformanceMetricsProps {
    width: number;
}

export default function PerformanceMetrics({ summary, nodeTag }: PerformanceMetricsProps) {
    return (
        <SizeGetter
            render={({ width }) => <PerformanceMetricsWithSize summary={summary} nodeTag={nodeTag} width={width} />}
        />
    );
}

function PerformanceMetricsWithSize({ summary, nodeTag, width }: PerformanceMetricsWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const node = summary.SummaryPerNode?.[nodeTag];
    const [tab, setTab] = useState<MetricTab>("cpu");

    const metricTabs = useMemo<{ label: string; value: MetricTab; icon: IconName }[]>(
        () => [
            { label: t("performanceMetrics.tabs.cpu"), value: "cpu", icon: "processor" },
            { label: t("performanceMetrics.tabs.memory"), value: "memory", icon: "memory" },
            { label: t("performanceMetrics.tabs.gc"), value: "gc", icon: "gc" },
            { label: t("performanceMetrics.tabs.network"), value: "network", icon: "global" },
            { label: t("performanceMetrics.tabs.threads"), value: "threads", icon: "thread-stack-trace" },
        ],
        [t]
    );

    return (
        <div className="performance-metrics">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">{t("performanceMetrics.title")}</h3>
                    <SegmentedControl<MetricTab> items={metricTabs} selected={tab} onSelect={setTab} fullWidth />
                    {tab === "cpu" && <CpuTab cpu={node?.CpuUsageInfo} width={width} />}
                    {tab === "memory" && <MemoryTab memory={node?.MemoryUsageInfo} width={width} />}
                    {tab === "gc" && <GcTab gc={node?.GcInfo} width={width} />}
                    {tab === "network" && <NetworkTab packageId={summary.PackageId} nodeTag={nodeTag} width={width} />}
                    {tab === "threads" && (
                        <>
                            <div className="hstack">
                                <Button
                                    variant="secondary"
                                    size="sm"
                                    className="ms-auto"
                                    href={appUrl.forCaptureStackTraces(summary.PackageId, nodeTag)}
                                    target="_blank"
                                    rel="noreferrer"
                                >
                                    <Icon icon="stack-traces" /> {t("performanceMetrics.openStackTraces")}
                                </Button>
                            </div>
                            <ThreadsTab packageId={summary.PackageId} nodeTag={nodeTag} width={width} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

// --- CPU tab ---

function CpuTab({ cpu, width }: { cpu?: CpuUsageAnalysisInfo; width: number }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    if (!cpu) {
        return (
            <EmptySet compact className="justify-content-center">
                {t("performanceMetrics.cpu.empty")}
            </EmptySet>
        );
    }
    return (
        <>
            <div className="overview-stats gap-2">
                <StatTile
                    label={t("performanceMetrics.cpu.processCpu")}
                    icon="hammer-driver"
                    value={formatPercentage(cpu.CurrentCpuUsage)}
                />
                <StatTile
                    label={t("performanceMetrics.cpu.machineCpu")}
                    icon="studio-configuration"
                    value={formatPercentage(cpu.CurrentMachineCpuUsage)}
                />
                <StatTile
                    label={t("performanceMetrics.cpu.averageCpu")}
                    icon="graph-range"
                    value={formatPercentage(cpu.AverageCpuUsage)}
                />
                <StatTile
                    label={t("performanceMetrics.cpu.kernelTime")}
                    icon="settings"
                    value={formatPercentage(cpu.KernelTimePercentage)}
                />
                <StatTile
                    label={t("performanceMetrics.cpu.cores")}
                    icon="processor"
                    value={formatNumber(cpu.NumberOfCores)}
                />
                <StatTile
                    label={t("performanceMetrics.cpu.utilizedCores")}
                    icon="swap"
                    value={formatNumber(cpu.UtilizedCores)}
                />
            </div>
            <div className="d-flex gap-3">
                <ThreadList
                    className="flex-fill"
                    title={t("performanceMetrics.cpu.topCurrentThreads")}
                    threads={cpu.TopCurrentCpuUsageThreads}
                    width={Math.floor(width / 2)}
                />
                <ThreadList
                    className="flex-fill"
                    title={t("performanceMetrics.cpu.topOverallThreads")}
                    threads={cpu.TopOverallCpuUsageThreads}
                    width={Math.floor(width / 2)}
                />
            </div>
        </>
    );
}

type ThreadItem = { name: string };

function useThreadListColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const threadListColumns: ColumnDef<ThreadItem>[] = useMemo(
        () => [
            {
                header: t("performanceMetrics.cpu.threadColumn"),
                accessorKey: "name",
                size: getSize(100),
            },
        ],
        [getSize, t]
    );

    return { threadListColumns };
}

function ThreadList({
    title,
    threads,
    className,
    width,
}: {
    title: string;
    threads: string[];
    className?: string;
    width: number;
}) {
    const data = useMemo<ThreadItem[]>(() => (threads ?? []).map((name) => ({ name })), [threads]);
    const { threadListColumns } = useThreadListColumns(width);

    const table = useReactTable({
        data,
        columns: threadListColumns,
        enableSorting: data.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: data.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    if (!threads || threads.length === 0) {
        return null;
    }

    return (
        <div className={classNames("overflow-hidden", className)}>
            <div className="small-label ms-1 mb-1">{title}</div>
            <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(data.length, 300)} />
        </div>
    );
}

// --- Memory tab ---

type MemoryMetric = { label: string; value: string };

function useMemoryMetricColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const memoryMetricColumns: ColumnDef<MemoryMetric>[] = useMemo(
        () => [
            {
                header: t("performanceMetrics.memory.columns.metric"),
                accessorKey: "label",
                size: getSize(75),
            },
            {
                header: t("performanceMetrics.memory.columns.value"),
                accessorKey: "value",
                cell: MemoryValueCell,
                size: getSize(25),
            },
        ],
        [getSize, t]
    );

    return { memoryMetricColumns };
}

function MemoryValueCell({ getValue }: { getValue: () => unknown }) {
    return <span>{(getValue() as string) ?? "-"}</span>;
}

function MemoryTab({ memory, width }: { memory?: MemoryAnalysisInfo; width: number }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const metrics = useMemo<MemoryMetric[]>(() => {
        if (!memory) {
            return [];
        }
        return [
            {
                label: t("performanceMetrics.memory.metrics.managedAllocations"),
                value: memory.Managed?.ManagedAllocations,
            },
            {
                label: t("performanceMetrics.memory.metrics.luceneManagedTermCache"),
                value: memory.Managed?.LuceneManagedAllocationsForTermCache,
            },
            {
                label: t("performanceMetrics.memory.metrics.unmanagedAllocations"),
                value: memory.Unmanaged?.UnmanagedAllocations,
            },
            {
                label: t("performanceMetrics.memory.metrics.encryptionBuffersInUse"),
                value: memory.Unmanaged?.EncryptionBuffersInUse,
            },
            {
                label: t("performanceMetrics.memory.metrics.encryptionBuffersPool"),
                value: memory.Unmanaged?.EncryptionBuffersPool,
            },
            {
                label: t("performanceMetrics.memory.metrics.encryptionLockedMemory"),
                value: memory.Unmanaged?.EncryptionLockedMemory,
            },
            {
                label: t("performanceMetrics.memory.metrics.luceneUnmanagedSorting"),
                value: memory.Unmanaged?.LuceneUnmanagedAllocationsForSorting,
            },
            {
                label: t("performanceMetrics.memory.metrics.luceneUnmanagedTermCache"),
                value: memory.Unmanaged?.LuceneUnmanagedAllocationsForTermCache,
            },
        ];
    }, [memory, t]);

    const { memoryMetricColumns } = useMemoryMetricColumns(width);

    const table = useReactTable({
        data: metrics,
        columns: memoryMetricColumns,
        enableSorting: metrics.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: metrics.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(metrics.length, 400);

    if (!memory) {
        return (
            <EmptySet compact className="justify-content-center">
                {t("performanceMetrics.memory.empty")}
            </EmptySet>
        );
    }

    const warnColor = memory.IsHighDirty ? "warning" : undefined;

    return (
        <>
            <div className="overview-stats gap-2">
                <StatTile
                    label={t("performanceMetrics.memory.workingSet")}
                    icon="storage-used"
                    value={memory.WorkingSet}
                />
                <StatTile
                    label={t("performanceMetrics.memory.physicalMemory")}
                    icon="memory"
                    value={memory.PhysicalMemory}
                />
                <StatTile
                    label={t("performanceMetrics.memory.availableMemory")}
                    icon="storage-free"
                    value={memory.AvailableMemory}
                />
                <StatTile
                    label={t("performanceMetrics.memory.availableForProcessing")}
                    icon="swap"
                    value={memory.AvailableMemoryForProcessing}
                />
                <StatTile
                    label={t("performanceMetrics.memory.dirtyMemory")}
                    icon="clean"
                    iconColor={warnColor}
                    value={memory.DirtyMemory}
                    valueColor={warnColor}
                />
                <StatTile label={t("performanceMetrics.memory.memoryMapped")} icon="map" value={memory.MemoryMapped} />
            </div>
            <VirtualTable table={table} heightInPx={heightInPx} />
        </>
    );
}

// --- GC tab ---

type GenerationRow = { label: string; size: GenerationInfoSize };

function useGcGenerationColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const gcGenerationColumns: ColumnDef<GenerationRow>[] = useMemo(
        () => [
            { header: t("performanceMetrics.gc.columns.generation"), accessorKey: "label", size: getSize(22) },
            {
                header: t("performanceMetrics.gc.columns.sizeBefore"),
                id: "sizeBefore",
                accessorFn: (row) => row.size?.SizeBeforeBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(18),
            },
            {
                header: t("performanceMetrics.gc.columns.sizeAfter"),
                id: "sizeAfter",
                accessorFn: (row) => row.size?.SizeAfterBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(18),
            },
            {
                header: t("performanceMetrics.gc.columns.fragmentationBefore"),
                id: "fragBefore",
                accessorFn: (row) => row.size?.FragmentationBeforeBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(21),
            },
            {
                header: t("performanceMetrics.gc.columns.fragmentationAfter"),
                id: "fragAfter",
                accessorFn: (row) => row.size?.FragmentationAfterBytes ?? 0,
                cell: ({ getValue }) => genUtils.formatBytesToSize(getValue<number>()),
                size: getSize(21),
            },
        ],
        [getSize, t]
    );

    return { gcGenerationColumns };
}

function GcTab({ gc, width }: { gc?: GcMemoryInfo; width: number }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const generations = useMemo<GenerationRow[]>(() => {
        if (!gc) {
            return [];
        }
        return [
            { label: t("performanceMetrics.gc.generation", { generation: 0 }), size: gc.Gen0HeapSize },
            { label: t("performanceMetrics.gc.generation", { generation: 1 }), size: gc.Gen1HeapSize },
            { label: t("performanceMetrics.gc.generation", { generation: 2 }), size: gc.Gen2HeapSize },
            { label: t("performanceMetrics.gc.largeObjectHeap"), size: gc.LargeObjectHeapSize },
            { label: t("performanceMetrics.gc.pinnedObjectHeap"), size: gc.PinnedObjectHeapSize },
        ];
    }, [gc, t]);

    const { gcGenerationColumns } = useGcGenerationColumns(width);

    const table = useReactTable({
        data: generations,
        columns: gcGenerationColumns,
        enableSorting: generations.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: generations.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(generations.length, 400);

    if (!gc) {
        return (
            <EmptySet compact className="justify-content-center">
                {t("performanceMetrics.gc.empty")}
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-2">
                <StatTile
                    label={t("performanceMetrics.gc.lastGeneration")}
                    icon="gc"
                    value={t("performanceMetrics.gc.generation", { generation: gc.Generation })}
                />
                <StatTile label={t("performanceMetrics.gc.gcIndex")} icon="hash" value={formatNumber(gc.Index)} />
                <StatTile
                    label={t("performanceMetrics.gc.pauseTime")}
                    icon="pause"
                    value={formatPercentage(gc.PauseTimePercentage)}
                />
                <StatTile
                    label={t("performanceMetrics.gc.totalHeapAfter")}
                    icon="memory"
                    value={genUtils.formatBytesToSize(gc.TotalHeapSizeAfterBytes)}
                />
                <StatTile
                    label={t("performanceMetrics.gc.concurrent")}
                    icon="shuffle"
                    value={gc.Concurrent ? t("yes") : t("no")}
                />
                <StatTile
                    label={t("performanceMetrics.gc.compacted")}
                    icon="compact"
                    value={gc.Compacted ? t("yes") : t("no")}
                    valueColor={gc.Compacted ? "success" : undefined}
                />
            </div>
            <div>
                <div className="small-label ms-1 mb-1">{t("performanceMetrics.gc.heapByGeneration")}</div>
                <VirtualTable table={table} heightInPx={heightInPx} />
            </div>
            {gc.PauseDurationsInMs?.length > 0 && (
                <div className="text-muted">
                    {t("performanceMetrics.gc.pauseDurations", {
                        durations: gc.PauseDurationsInMs.map((ms) =>
                            t("performanceMetrics.durationMs", { duration: ms })
                        ).join(", "),
                    })}
                </div>
            )}
        </>
    );
}

// --- Network tab ---

function useNetworkColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const tcpColumns: ColumnDef<TcpConnections>[] = useMemo(
        () => [
            {
                header: t("performanceMetrics.network.tcpColumns.tcpState"),
                id: "state",
                accessorFn: (row) => row.TcpState ?? "",
                size: getSize(25),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.network.tcpColumns.connections"),
                id: "connections",
                accessorFn: (row) => row.NumberOfConnectionsInState ?? 0,
                cell: ({ getValue }) => formatNumber(getValue<number>()),
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.network.tcpColumns.topRemoteEndpoints"),
                id: "endpoints",
                accessorFn: (row) => formatTopConnections(row.TopConnectionsInState),
                size: getSize(57),
            },
        ],
        [getSize, t]
    );

    const pingColumns: ColumnDef<PingResult>[] = useMemo(
        () => [
            {
                header: t("performanceMetrics.network.pingColumns.targetNode"),
                accessorKey: "Url",
                size: getSize(35),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.network.pingColumns.setupAlive"),
                id: "setup",
                accessorFn: (row) => row.SetupAlive?.Time ?? 0,
                cell: ({ row }) => formatPingMs(row.original.SetupAlive?.Time, t),
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.network.pingColumns.tcpPing"),
                id: "tcp",
                accessorFn: (row) => row.TcpInfo?.ReceiveTime ?? 0,
                cell: PingTcpCell,
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.network.pingColumns.status"),
                id: "status",
                accessorFn: (row) => (row.SetupAlive?.Error || row.TcpInfo?.Error ? 1 : 0),
                cell: PingStatusCell,
                size: getSize(29),
                enableSorting: true,
            },
        ],
        [getSize, t]
    );

    return { tcpColumns, pingColumns };
}

function PingTcpCell({ row }: { row: { original: PingResult } }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const tcpPing = row.original.TcpInfo?.ReceiveTime;
    const pingClass = tcpPing > 5000 ? "text-danger" : tcpPing > 2000 ? "text-warning" : "";
    return <span className={pingClass}>{formatPingMs(tcpPing, t)}</span>;
}

function PingStatusCell({ row }: { row: { original: PingResult } }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const setupError = row.original.SetupAlive?.Error;
    const tcpError = row.original.TcpInfo?.Error;
    const hasError = Boolean(setupError || tcpError);
    return hasError ? (
        <span className="text-danger" title={[setupError, tcpError].filter(Boolean).join(" | ")}>
            <Icon icon="warning" margin="m-0" /> {t("performanceMetrics.network.pingError")}
        </span>
    ) : (
        <span className="hstack gap-1 text-success">
            <Icon icon="check" margin="m-0" /> {t("performanceMetrics.network.pingOk")}
        </span>
    );
}

// Network info is not in the summary; fetch it on demand from the analyzer network endpoint.
function NetworkTab({ packageId, nodeTag, width }: { packageId: string; nodeTag: string; width: number }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    const network = useAsync(
        () => manageServerService.getDebugPackageNetworkInfo(packageId, nodeTag),
        [packageId, nodeTag]
    );

    const { tcpColumns, pingColumns } = useNetworkColumns(width);

    const tcpTable = useReactTable({
        data: network.result?.TcpConnections ?? [],
        columns: tcpColumns,
        enableSorting: (network.result?.TcpConnections ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (network.result?.TcpConnections ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "connections", desc: true }] },
        getRowId: (_, index) => String(index),
    });

    const pingTable = useReactTable({
        data: network.result?.PingTestResults ?? [],
        columns: pingColumns,
        enableSorting: (network.result?.PingTestResults ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (network.result?.PingTestResults ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "tcp", desc: false }] },
        getRowId: (row) => row.Url ?? "",
    });

    const tcpCount = network.result?.TcpConnections?.length ?? 0;
    const pingCount = network.result?.PingTestResults?.length ?? 0;
    const tcpHeightInPx = virtualTableUtils.getHeightInPx(tcpCount, 300);
    const pingHeightInPx = virtualTableUtils.getHeightInPx(pingCount, 300);

    if (network.loading) {
        return (
            <div className="hstack gap-2 justify-content-center text-muted py-3">
                <Spinner size="sm" /> {t("performanceMetrics.network.loading", { nodeTag })}
            </div>
        );
    }

    if (network.error) {
        return <RichAlert variant="warning">{t("performanceMetrics.network.loadError", { nodeTag })}</RichAlert>;
    }

    const info = network.result;
    if (!info) {
        return (
            <EmptySet compact className="justify-content-center">
                {t("performanceMetrics.network.empty")}
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-2">
                <StatTile
                    label={t("performanceMetrics.network.activeTcpConnections")}
                    icon="global"
                    value={formatNumber(info.TotalActiveTcpConnections)}
                />
                <StatTile
                    label={t("performanceMetrics.network.connectionStates")}
                    icon="link"
                    value={formatNumber(info.TcpConnections?.length)}
                />
            </div>
            {tcpCount === 0 ? (
                <EmptySet compact className="justify-content-center">
                    {t("performanceMetrics.network.tcpEmpty")}
                </EmptySet>
            ) : (
                <VirtualTable table={tcpTable} heightInPx={tcpHeightInPx} />
            )}
            <div>
                <div className="small-label ms-1 mb-1">{t("performanceMetrics.network.pingTitle", { nodeTag })}</div>
                {pingCount === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        {t("performanceMetrics.network.pingEmpty")}
                    </EmptySet>
                ) : (
                    <VirtualTable table={pingTable} heightInPx={pingHeightInPx} />
                )}
            </div>
        </>
    );
}

// --- Threads tab ---

function useThreadColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const threadColumns: ColumnDef<ThreadInfo>[] = useMemo(
        () => [
            {
                header: t("performanceMetrics.threads.columns.thread"),
                id: "name",
                accessorFn: (row) => row.Name ?? "",
                cell: ThreadNameCell,
                size: getSize(22),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.cpu"),
                id: "cpu",
                accessorFn: (row) => row.CpuUsage ?? 0,
                cell: ({ row }) => formatPercentage(row.original.CpuUsage),
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.state"),
                id: "state",
                accessorFn: (row) => row.State ?? "",
                size: getSize(13),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.processorTime"),
                id: "processorTime",
                accessorFn: (row) => genUtils.timeSpanToSeconds(row.TotalProcessorTime) ?? 0,
                cell: ({ row }) => row.original.TotalProcessorTime,
                size: getSize(16),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.unmanagedAlloc"),
                id: "unmanaged",
                accessorFn: (row) => row.UnmanagedAllocationsInBytes ?? 0,
                cell: ({ row }) => genUtils.formatBytesToSize(row.original.UnmanagedAllocationsInBytes ?? 0),
                size: getSize(16),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.ioRead"),
                id: "ioRead",
                accessorFn: (row) => row.IoStats?.ReadBytes ?? 0,
                cell: ({ row }) => formatThreadIo(row.original.IoStats?.ReadBytes),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: t("performanceMetrics.threads.columns.ioWrite"),
                id: "ioWrite",
                accessorFn: (row) => row.IoStats?.WriteBytes ?? 0,
                cell: ({ row }) => formatThreadIo(row.original.IoStats?.WriteBytes),
                size: getSize(11),
                enableSorting: true,
            },
        ],
        [getSize, t]
    );

    return { threadColumns };
}

function ThreadNameCell({ row }: { row: { original: ThreadInfo } }) {
    return (
        <>
            <div className="text-break">{row.original.Name}</div>
            <div className="small-label">#{row.original.Id}</div>
        </>
    );
}

// Thread runtime info is not in the summary; fetch on demand from the analyzer threads/runaway endpoint.
function ThreadsTab({ packageId, nodeTag, width }: { packageId: string; nodeTag: string; width: number }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    const threads = useAsync(
        () => manageServerService.getDebugPackageThreadsInfo(packageId, nodeTag),
        [packageId, nodeTag]
    );

    const { threadColumns } = useThreadColumns(width);

    const table = useReactTable({
        data: threads.result?.List ?? [],
        columns: threadColumns,
        enableSorting: (threads.result?.List ?? []).length > analyzerConstants.minRowsForControls,
        enableColumnFilters: (threads.result?.List ?? []).length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: { sorting: [{ id: "cpu", desc: true }] },
        getRowId: (row) => String(row.Id),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(threads.result?.List?.length ?? 0, 500);

    if (threads.loading) {
        return (
            <div className="hstack gap-2 justify-content-center text-muted py-3">
                <Spinner size="sm" /> {t("performanceMetrics.threads.loading", { nodeTag })}
            </div>
        );
    }

    if (threads.error) {
        return <RichAlert variant="warning">{t("performanceMetrics.threads.loadError", { nodeTag })}</RichAlert>;
    }

    const info = threads.result;
    if (!info) {
        return (
            <EmptySet compact className="justify-content-center">
                {t("performanceMetrics.threads.empty")}
            </EmptySet>
        );
    }

    return (
        <>
            <div className="overview-stats gap-2">
                <StatTile
                    label={t("performanceMetrics.threads.processCpu")}
                    icon="hammer-driver"
                    value={formatPercentage(info.ProcessCpuUsage)}
                />
                <StatTile
                    label={t("performanceMetrics.threads.threadsCount")}
                    icon="thread-stack-trace"
                    value={formatNumber(info.ThreadsCount)}
                />
                <StatTile
                    label={t("performanceMetrics.threads.dedicatedThreads")}
                    icon="stack-traces"
                    value={formatNumber(info.DedicatedThreadsCount)}
                />
                <StatTile
                    label={t("performanceMetrics.threads.activeCores")}
                    icon="processor"
                    value={formatNumber(info.ActiveCores)}
                />
            </div>
            {table.getRowCount() === 0 ? (
                <EmptySet compact className="justify-content-center">
                    {t("performanceMetrics.threads.noThreads")}
                </EmptySet>
            ) : (
                <VirtualTable table={table} heightInPx={heightInPx} />
            )}
        </>
    );
}

function formatPingMs(ms: number | undefined, t: StudioTranslate<"debugPackageAnalyzer">): string {
    return ms == null ? "-" : t("performanceMetrics.durationMs", { duration: formatNumber(ms) });
}

function formatTopConnections(top: { [endpoint: string]: number }): string {
    const entries = Object.entries(top ?? {});
    if (entries.length === 0) {
        return "-";
    }
    return entries
        .sort((a, b) => b[1] - a[1])
        .map(([endpoint, count]) => `${endpoint} (${count})`)
        .join(", ");
}

function formatThreadIo(bytes: number | undefined): string {
    return bytes == null ? "-" : genUtils.formatBytesToSize(bytes);
}
