import React, { useMemo } from "react";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import NodeTagPill from "./NodeTagPill";
import StatTile from "./StatTile";
import { formatUpTime, osIcon, parseUpTimeSeconds } from "./analyzerUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type NodeSummary = DebugPackageAnalysisSummary["SummaryPerNode"][string];
type ClusterNodeInfo = NodeSummary["ClusterNodeInfo"];
// unreachable nodes carry only NodeTag and NodeUrl (from the topology), so a missing NodeState marks an offline row
type ClusterNodeRow = Partial<ClusterNodeInfo> & Pick<ClusterNodeInfo, "NodeTag" | "NodeUrl">;

interface ClusterOverviewProps {
    summary: DebugPackageAnalysisSummary;
}

interface ClusterOverviewWithSizeProps extends ClusterOverviewProps {
    width: number;
}

function useClusterOverviewColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const clusterColumns: ColumnDef<ClusterNodeRow>[] = useMemo(
        () => [
            {
                header: t("clusterOverview.columns.nodeTag"),
                accessorKey: "NodeTag",
                cell: ({ getValue }) => <NodeTagPill tag={getValue<string>()} />,
                size: getSize(8),
                enableSorting: true,
            },
            {
                header: t("clusterOverview.columns.role"),
                accessorKey: "NodeState",
                cell: ClusterRoleCell,
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: t("clusterOverview.columns.type"),
                accessorKey: "NodeType",
                size: getSize(10),
                enableSorting: true,
            },
            {
                header: t("clusterOverview.columns.os"),
                accessorKey: "OsName",
                cell: ClusterOsCell,
                size: getSize(18),
                enableSorting: true,
            },
            {
                header: t("clusterOverview.columns.serverVersion"),
                accessorKey: "ServerVersion",
                size: getSize(14),
                enableSorting: true,
            },
            {
                header: t("clusterOverview.columns.uptime"),
                id: "uptime",
                accessorFn: (row) => parseUpTimeSeconds(row.UpTime),
                cell: ({ row }) => formatUpTime(row.original.UpTime),
                size: getSize(12),
                enableSorting: true,
            },
            {
                header: "URL",
                accessorKey: "NodeUrl",
                cell: ClusterUrlCell,
                size: getSize(28),
            },
        ],
        [getSize, t]
    );

    return { clusterColumns };
}

export default function ClusterOverview({ summary }: ClusterOverviewProps) {
    return <SizeGetter render={({ width }) => <ClusterOverviewWithSize summary={summary} width={width} />} />;
}

function ClusterOverviewWithSize({ summary, width }: ClusterOverviewWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const nodes = useMemo(() => Object.values(summary.SummaryPerNode ?? {}) as NodeSummary[], [summary]);
    const nodeInfos = useMemo(() => nodes.map((n) => n.ClusterNodeInfo).filter(Boolean), [nodes]);
    const rows = useMemo<ClusterNodeRow[]>(
        () =>
            [
                ...nodeInfos,
                ...Object.entries(summary.UnreachableNodes ?? {}).map(([NodeTag, NodeUrl]) => ({ NodeTag, NodeUrl })),
            ].sort((a, b) => a.NodeTag.localeCompare(b.NodeTag)),
        [nodeInfos, summary]
    );

    const leader = nodeInfos.find((n) => n.NodeState === "Leader");

    const totalDatabases = useMemo(() => {
        const names = new Set<string>();
        nodes.forEach((n) => (n.DatabasesOverview?.Items ?? []).forEach((item) => names.add(item.Database)));
        return names.size;
    }, [nodes]);

    const offlineNodes = Object.keys(summary.UnreachableNodes ?? {}).length;
    const onlineNodes = nodes.length;
    const totalNodes = onlineNodes + offlineNodes;
    const allNodesOnline = offlineNodes === 0;

    const clusterUpTime = useMemo(() => {
        let best: string | null = null;
        let bestSeconds = -1;
        nodeInfos.forEach((n) => {
            const seconds = parseUpTimeSeconds(n.UpTime);
            if (seconds > bestSeconds) {
                bestSeconds = seconds;
                best = n.UpTime;
            }
        });
        return best;
    }, [nodeInfos]);

    const { clusterColumns } = useClusterOverviewColumns(width);

    const table = useReactTable({
        data: rows,
        columns: clusterColumns,
        renderFallbackValue: "-",
        enableSorting: rows.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: rows.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        initialState: {
            sorting: [{ id: "NodeTag", desc: false }],
        },
        getRowId: (row) => row.NodeTag,
    });

    const heightInPx = virtualTableUtils.getHeightInPx(rows.length, 300);

    return (
        <div className="cluster-overview">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack">
                    <h3 className="mb-3">{t("clusterOverview.title")}</h3>
                    {nodeInfos.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {t("clusterOverview.empty")}
                        </EmptySet>
                    ) : (
                        <>
                            <div className="overview-stats gap-2 mb-3">
                                <StatTile
                                    label={t("clusterOverview.stats.nodesStatus")}
                                    icon={allNodesOnline ? "check" : "warning"}
                                    iconColor={allNodesOnline ? "success" : "warning"}
                                    value={t("clusterOverview.stats.nodesStatusValue", {
                                        online: onlineNodes,
                                        total: totalNodes,
                                    })}
                                />
                                <StatTile
                                    label={t("clusterOverview.stats.leaderNode")}
                                    icon="node-leader"
                                    iconColor="node"
                                    value={leader?.NodeTag ?? "-"}
                                />
                                <StatTile
                                    label={t("clusterOverview.stats.clusterUptime")}
                                    icon="clock"
                                    value={clusterUpTime != null ? formatUpTime(clusterUpTime) : "-"}
                                />
                                <StatTile
                                    label={t("clusterOverview.stats.totalDatabases")}
                                    icon="database"
                                    value={String(totalDatabases)}
                                />
                                <StatTile
                                    label={t("clusterOverview.stats.licenseTier")}
                                    icon="license"
                                    value={t("notAvailable")}
                                />
                            </div>

                            <VirtualTable table={table} heightInPx={heightInPx} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function ClusterRoleCell({ getValue }: { getValue: () => unknown }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const state = getValue() as string;
    if (!state) {
        return (
            <span className="hstack gap-1 text-danger">
                <Icon icon="disconnected" margin="m-0" /> {t("clusterOverview.roles.offline")}
            </span>
        );
    }
    switch (state) {
        case "Leader":
            return (
                <span className="hstack gap-1">
                    <Icon icon="node-leader" margin="m-0" /> {t("clusterOverview.roles.leader")}
                </span>
            );
        case "Follower":
            return (
                <span className="hstack gap-1">
                    <Icon icon="cluster-member" margin="m-0" /> {t("clusterOverview.roles.follower")}
                </span>
            );
        case "Candidate":
            return (
                <span className="hstack gap-1">
                    <Icon icon="question" margin="m-0" /> {t("clusterOverview.roles.candidate")}
                </span>
            );
        case "Passive":
            return (
                <span className="hstack gap-1">
                    <Icon icon="node" margin="m-0" /> {t("clusterOverview.roles.passive")}
                </span>
            );
        default:
            return <span>{state}</span>;
    }
}

function ClusterOsCell({ row }: { row: { original: ClusterNodeRow } }) {
    if (!row.original.OsName) {
        return <>-</>;
    }
    return (
        <>
            <Icon icon={osIcon(row.original.OsType)} /> {row.original.OsName}
        </>
    );
}

function ClusterUrlCell({ getValue }: { getValue: () => unknown }) {
    const url = getValue() as string;
    if (!url) {
        return <>-</>;
    }
    return (
        <a href={url} target="_blank" rel="noreferrer">
            {url}
        </a>
    );
}
