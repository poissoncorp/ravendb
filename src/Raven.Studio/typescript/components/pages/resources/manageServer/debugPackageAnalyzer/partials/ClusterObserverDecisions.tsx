import { useMemo, useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import StatTile from "./StatTile";
import SegmentedControl from "components/common/SegmentedControl";
import genUtils from "common/generalUtils";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import "./AnalysisResults.scss";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type ClusterObserverDecisionsDto = Raven.Server.ServerWide.Maintenance.ClusterObserverDecisions;
type ClusterObserverLogEntry = Raven.Server.ServerWide.Maintenance.ClusterObserverLogEntry;

interface NodeObserverResult {
    nodeTag: string;
    status: "success" | "failure";
    decisions?: ClusterObserverDecisionsDto;
}

const maxObserverEntriesShown = 2_000;

interface ClusterObserverDecisionsProps {
    summary: DebugPackageAnalysisSummary;
}

interface ObserverBodyProps {
    results: NodeObserverResult[];
    width: number;
}

function ObserverBody({ results, width }: ObserverBodyProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    // fulfilled nodes, ranked so the node that actually captured decisions (the leader) is the default
    const ranked = useMemo(
        () =>
            results
                .filter(
                    (result): result is NodeObserverResult & { decisions: ClusterObserverDecisionsDto } =>
                        !!result.decisions
                )
                .sort((a, b) => (b.decisions.ObserverLog?.length ?? 0) - (a.decisions.ObserverLog?.length ?? 0)),
        [results]
    );

    const [selectedNode, setSelectedNode] = useState<string>(ranked[0]?.nodeTag ?? null);

    const selected = ranked.find((result) => result.nodeTag === selectedNode);
    const decisions = selected?.decisions;
    const log = decisions?.ObserverLog ?? [];

    const logEntries = useMemo(() => {
        // newest first; slice before reversing so we only copy at most maxObserverEntriesShown entries.
        // filtering by database/message is handled by the table's built-in per-column filters.
        return log.length > maxObserverEntriesShown
            ? log.slice(-maxObserverEntriesShown).reverse()
            : [...log].reverse();
    }, [log]);

    const totalEntries = log.length;

    const { logColumns } = useLogColumns(width);

    const table = useReactTable({
        data: logEntries,
        columns: logColumns,
        enableSorting: logEntries.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: true,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (_row, index) => String(index),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(logEntries.length, 500);

    return (
        <div className="vstack gap-3">
            <div className="hstack gap-2 flex-wrap">
                <StatTile
                    className="flex-fill"
                    label={t("clusterObserverDecisions.stats.leaderNode")}
                    icon="node-leader"
                    iconColor="node"
                    value={decisions?.LeaderNode ? <span>{decisions.LeaderNode}</span> : t("notAvailable")}
                />
                <StatTile
                    className="flex-fill"
                    label={t("clusterObserverDecisions.stats.term")}
                    icon="document"
                    value={decisions?.Term?.toLocaleString() ?? t("notAvailable")}
                />
                <StatTile
                    className="flex-fill"
                    label={t("clusterObserverDecisions.stats.iteration")}
                    icon="refresh"
                    value={decisions?.Iteration?.toLocaleString() ?? t("notAvailable")}
                />
                <StatTile
                    className="flex-fill"
                    label={t("clusterObserverDecisions.stats.observer")}
                    icon="play"
                    iconColor={decisions?.Suspended ? "warning" : "success"}
                    value={
                        decisions?.Suspended
                            ? t("clusterObserverDecisions.stats.suspended")
                            : t("clusterObserverDecisions.stats.running")
                    }
                    valueColor={decisions?.Suspended ? "warning" : "success"}
                />
            </div>

            <div className="vstack gap-2">
                <div className="hstack gap-2 align-items-center flex-wrap">
                    <h4 className="m-0">{t("clusterObserverDecisions.log.title")}</h4>
                    {totalEntries > maxObserverEntriesShown && (
                        <span className="small-label text-muted">
                            {t("clusterObserverDecisions.log.latestShown", {
                                limit: maxObserverEntriesShown.toLocaleString(),
                            })}
                        </span>
                    )}
                </div>
                {ranked.length > 1 && (
                    <SegmentedControl
                        items={ranked.map((result) => ({
                            value: result.nodeTag,
                            label: t("nodeName", { nodeTag: result.nodeTag }),
                            count: result.decisions.ObserverLog?.length ?? 0,
                        }))}
                        selected={selectedNode}
                        onSelect={setSelectedNode}
                    />
                )}

                {logEntries.length === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        {t("clusterObserverDecisions.log.empty")}
                    </EmptySet>
                ) : (
                    <VirtualTable table={table} heightInPx={heightInPx} />
                )}
            </div>
        </div>
    );
}

// The exported component passes width down so ObserverBody can size its table columns proportionally.
// ClusterObserverDecisions itself doesn't render a table directly, so SizeGetter wraps ObserverBody.
export default function ClusterObserverDecisions({ summary }: ClusterObserverDecisionsProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    const packageId = summary.PackageId;
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);

    const observer = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodeTags.map((tag) => manageServerService.getDebugPackageClusterObserverDecisions(packageId, tag))
        );
        return nodeTags.map((tag, index): NodeObserverResult => {
            const outcome = settled[index];
            if (outcome.status === "fulfilled" && outcome.value) {
                return { nodeTag: tag, status: "success", decisions: outcome.value };
            }
            return { nodeTag: tag, status: "failure" };
        });
    }, [packageId, nodeTags]);

    const results = observer.result ?? [];
    const hasAnyDecisions = results.some((result) => result.decisions);

    return (
        <div className="panel-bg-1 rounded cluster-observer-decisions">
            <div className="p-4">
                <h3 className="mb-3">{t("clusterObserverDecisions.title")}</h3>
                {observer.loading ? (
                    <div className="hstack gap-2 justify-content-center text-muted py-3">
                        <Spinner size="sm" data-testid="loader" /> {t("clusterObserverDecisions.loading")}
                    </div>
                ) : !hasAnyDecisions ? (
                    <EmptySet compact className="justify-content-center">
                        {t("clusterObserverDecisions.empty")}
                    </EmptySet>
                ) : (
                    <SizeGetter render={({ width }) => <ObserverBody results={results} width={width} />} />
                )}
            </div>
        </div>
    );
}

function useLogColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const logColumns: ColumnDef<ClusterObserverLogEntry>[] = useMemo(
        () => [
            {
                header: t("clusterObserverDecisions.log.columns.date"),
                accessorKey: "Date",
                enableColumnFilter: false,
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return v ? genUtils.formatUtcDateAsLocal(v) : "-";
                },
                size: getSize(20),
            },
            {
                header: t("clusterObserverDecisions.log.columns.database"),
                accessorKey: "Database",
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return <span title={v || undefined}>{v || "-"}</span>;
                },
                size: getSize(20),
            },
            {
                header: t("clusterObserverDecisions.log.columns.message"),
                accessorKey: "Message",
                cell: ({ getValue }) => {
                    const v = getValue<string>();
                    return <span title={v}>{v}</span>;
                },
                size: getSize(60),
            },
        ],
        [getSize, t]
    );

    return { logColumns };
}
