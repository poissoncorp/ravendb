import React, { useMemo, useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import Table from "react-bootstrap/Table";
import ProgressBar from "react-bootstrap/ProgressBar";
import classNames from "classnames";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import { StatePill } from "components/common/StatePill";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import genUtils from "common/generalUtils";
import moment from "moment";
import {
    ClusterDebugNodeInfo,
    mapRaftDebugView,
} from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import ClusterDebugGlobalInfo from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterDebugGlobalInfo";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { CellWithCopyWrapper } from "components/common/virtualTable/cells/CellWithCopy";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { analyzerConstants } from "./analyzerConstants";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import "../../advanced/clusterDebug/partials/ClusterDebugSummary.scss";
import "../../advanced/clusterDebug/partials/ClusterDebugEntries.scss";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;
type RaftDebugView = Raven.Server.Rachis.RaftDebugView;
type PeerConnection = Raven.Server.Rachis.RaftDebugView.PeerConnection;
type LogEntry = Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry;

interface NodeRaftResult {
    nodeTag: string;
    status: "success" | "failure";
    info?: ClusterDebugNodeInfo;
    view?: RaftDebugView;
}

interface ClusterRaftDebugProps {
    summary: DebugPackageAnalysisSummary;
}

const maxEntriesShown = 500;

// The latest commit time across all captured node views, used as the snapshot's reference "now".
// Falls back to wall-clock only when nothing valid was captured (no nodes are mapped in that case anyway).
function mostRecentCommitTime(views: (RaftDebugView | null)[]): moment.Moment {
    let latest: moment.Moment | null = null;
    for (const view of views) {
        if (!view) {
            continue;
        }
        const commit = moment.utc(view.Log.LastCommitedTime);
        if (commit.isValid() && (latest === null || commit.isAfter(latest))) {
            latest = commit;
        }
    }
    return latest ?? moment.utc();
}

// Recreates the live Cluster Debug view from the package's per-node raft log (cluster/log endpoint).
// Reuses the live view's mapRaftDebugView + ClusterDebugGlobalInfo; the summary is adapted for a static
// snapshot (no live-cluster "current node" / server URLs, absolute timestamps instead of now-relative).
export default function ClusterRaftDebug({ summary }: ClusterRaftDebugProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();
    const packageId = summary.PackageId;
    const nodeTags = useMemo(() => Object.keys(summary.SummaryPerNode ?? {}).sort(), [summary]);

    const raft = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodeTags.map((tag) => manageServerService.getDebugPackageClusterLog(packageId, tag))
        );
        const views = settled.map((outcome) => (outcome.status === "fulfilled" ? outcome.value : null));

        // For an offline snapshot there is no live "now", so we measure the choked heuristic against the most
        // recent commit captured anywhere in the cluster (the closest proxy for the moment of capture). A node
        // lagging behind that reference still flags as choked; the whole package no longer does.
        const referenceTimeUtc = mostRecentCommitTime(views);

        return nodeTags.map((tag, index): NodeRaftResult => {
            const view = views[index];
            if (view) {
                return { nodeTag: tag, status: "success", view, info: mapRaftDebugView(view, referenceTimeUtc) };
            }
            return { nodeTag: tag, status: "failure" };
        });
    }, [packageId, nodeTags]);

    const loadableNodes = useMemo(
        () =>
            (raft.result ?? []).map((result) => ({
                nodeTag: result.nodeTag,
                data: result.info,
                status: result.status,
            })),
        [raft.result]
    );
    const results = raft.result ?? [];

    return (
        <div className="cluster-raft-debug">
            {raft.loading ? (
                <div className="hstack gap-2 justify-content-center text-muted py-3">
                    <Spinner size="sm" data-testid="loader" /> {t("clusterRaftDebug.loading")}
                </div>
            ) : results.length === 0 ? (
                <EmptySet compact className="justify-content-center">
                    {t("clusterRaftDebug.empty")}
                </EmptySet>
            ) : (
                <div className="vstack gap-3">
                    <div className="panel-bg-1 rounded">
                        <div className="p-4">
                            <h3 className="mb-3">{t("clusterRaftDebug.title")}</h3>
                            <ClusterDebugGlobalInfo nodes={loadableNodes} cardClassName="panel-bg-2" fillWidth />
                            <h4 className="mt-3">{t("clusterRaftDebug.summary.title")}</h4>
                            <RaftSummaryTable results={results} />
                        </div>
                        <LogEntriesCard results={results} />
                    </div>
                </div>
            )}
        </div>
    );
}

function RaftSummaryTable({ results }: { results: NodeRaftResult[] }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const hasAnyCriticalError = results.some((r) => !!r.info?.criticalError);

    return (
        <Table bordered responsive className="mb-1 rounded-1 overflow-hidden cluster-debug-summary">
            <thead>
                <tr>
                    <th></th>
                    {results.map((result) => (
                        <th key={result.nodeTag}>
                            <div className="d-flex gap-1 align-items-center">
                                <span>
                                    <Icon
                                        icon={result.info?.role === "Leader" ? "node-leader" : "cluster-member"}
                                        color="node"
                                    />
                                    <span className="text-nowrap">
                                        {t("clusterRaftDebug.nodeName", { nodeTag: result.nodeTag })}
                                    </span>
                                </span>
                            </div>
                        </th>
                    ))}
                </tr>
            </thead>
            <tbody>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.rolePhase")}
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <ul>
                                        <li>
                                            <StudioTrans
                                                ns="debugPackageAnalyzer"
                                                i18nKey="clusterRaftDebug.summary.rolePhaseInfoRole"
                                                components={{ br: <br />, i: <i /> }}
                                            />
                                        </li>
                                        <li>
                                            <StudioTrans
                                                ns="debugPackageAnalyzer"
                                                i18nKey="clusterRaftDebug.summary.rolePhaseInfoPhase"
                                                components={{ br: <br />, i: <i /> }}
                                            />
                                        </li>
                                    </ul>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.role}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.progress")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.progressInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? (
                                <PopoverWithHoverWrapper
                                    inline={false}
                                    targetStyle={{ width: "100%" }}
                                    message={
                                        <StudioTrans
                                            ns="debugPackageAnalyzer"
                                            i18nKey="clusterRaftDebug.summary.progressDetails"
                                            components={{ strong: <strong />, br: <br /> }}
                                            options={{
                                                firstEntryIndex: result.info.firstEntryIndex.toLocaleString(),
                                                commitIndex: result.info.commitIndex.toLocaleString(),
                                                lastLogEntryIndex: result.info.lastLogEntryIndex.toLocaleString(),
                                            }}
                                        />
                                    }
                                >
                                    <ProgressBar
                                        variant={result.info.progress === 100 ? "success" : "progress"}
                                        striped={result.info.progress < 100}
                                        now={result.info.progress}
                                        animated={result.info.progress < 100}
                                        label={`${result.info.progress}%`}
                                    />
                                </PopoverWithHoverWrapper>
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.queueSize")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.queueSizeInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.queueSize.toLocaleString()}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.lastCommitIndex")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.lastCommitIndexInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td
                            key={result.nodeTag}
                            className={classNames(
                                "align-content-center",
                                result.info?.chocked && "bg-faded-warning text-warning"
                            )}
                        >
                            {result.info ? (
                                <>
                                    {result.info.commitIndex.toLocaleString()}
                                    {result.info.chocked && (
                                        <PopoverWithHoverWrapper
                                            message={
                                                <StudioTrans
                                                    ns="debugPackageAnalyzer"
                                                    i18nKey="clusterRaftDebug.summary.noCommitsWarning"
                                                    components={{
                                                        prefix: <span className="text-warning" />,
                                                        icon: <Icon icon="warning" />,
                                                        span: <span />,
                                                    }}
                                                />
                                            }
                                        >
                                            <Icon icon="warning" margin="ms-1" />
                                        </PopoverWithHoverWrapper>
                                    )}
                                </>
                            ) : (
                                <Unavailable />
                            )}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.lastCommittedDate")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.lastCommittedDateInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => {
                        const asAgo = result.info?.lastCommitedTime
                            ? genUtils.formatDurationByDate(moment.utc(result.info.lastCommitedTime), true)
                            : null;
                        return (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    <PopoverWithHoverWrapper
                                        message={asAgo ? <>{result.info.lastCommitedTime}</> : null}
                                    >
                                        <div>{asAgo ?? t("notAvailable")}</div>
                                    </PopoverWithHoverWrapper>
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        );
                    })}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.lastAppendDate")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.lastAppendDateInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => {
                        const asAgo = result.info?.lastAppendedTime
                            ? genUtils.formatDurationByDate(moment.utc(result.info.lastAppendedTime), true)
                            : null;
                        return (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    <PopoverWithHoverWrapper message={asAgo ? result.info.lastAppendedTime : null}>
                                        <div>{asAgo ?? t("notAvailable")}</div>
                                    </PopoverWithHoverWrapper>
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        );
                    })}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.localVersion")}
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <ul>
                                        <li>{t("clusterRaftDebug.summary.localVersionInfoCommands")}</li>
                                        <li>{t("clusterRaftDebug.summary.localVersionInfoValue")}</li>
                                    </ul>
                                </>
                            }
                        >
                            <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <>{result.info.localVersion}</> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                <tr>
                    <th>
                        {t("clusterRaftDebug.summary.connection")}
                        <PopoverWithHoverWrapper message={t("clusterRaftDebug.summary.connectionInfo")}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </th>
                    {results.map((result) => (
                        <td className="align-content-center" key={result.nodeTag}>
                            {result.info ? <Connections connections={result.info.connections} /> : <Unavailable />}
                        </td>
                    ))}
                </tr>
                {hasAnyCriticalError && (
                    <tr>
                        <th>{t("clusterRaftDebug.summary.criticalError")}</th>
                        {results.map((result) => (
                            <td className="align-content-center" key={result.nodeTag}>
                                {result.info ? (
                                    result.info.criticalError ? (
                                        <StatePill bg="danger">
                                            {t("clusterRaftDebug.summary.criticalErrorPill")}
                                        </StatePill>
                                    ) : (
                                        <>-</>
                                    )
                                ) : (
                                    <Unavailable />
                                )}
                            </td>
                        ))}
                    </tr>
                )}
            </tbody>
        </Table>
    );
}

function Unavailable() {
    const t = useStudioTranslation("debugPackageAnalyzer");
    return <span className="text-muted">{t("notAvailable")}</span>;
}

function Connections({ connections }: { connections: PeerConnection[] }) {
    if (!connections?.length) {
        return <>-</>;
    }
    return (
        <div className="hstack gap-1 flex-wrap">
            {connections.map((connection) => (
                <StatePill key={connection.Destination} bg={connection.Connected ? "success" : "danger"}>
                    <Icon icon={connection.Connected ? "connected" : "disconnected"} margin="m-0" />{" "}
                    {connection.Destination}
                </StatePill>
            ))}
        </div>
    );
}

function LogEntriesCard({ results }: { results: NodeRaftResult[] }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const withLogs = useMemo(() => results.filter((r) => r.view), [results]);
    const [selectedNodeTag, setSelectedNodeTag] = useState<string | null>(null);

    const activeNodeTag = selectedNodeTag ?? withLogs[0]?.nodeTag ?? null;
    const selected = withLogs.find((r) => r.nodeTag === activeNodeTag);

    const log = selected?.view?.Log;
    const entries = log?.Logs ?? [];
    const shown = useMemo(() => entries.slice(-maxEntriesShown), [entries]);
    const commitIndex = log?.CommitIndex ?? 0;

    return (
        <div className="p-4 pt-0">
            <h3 className="hstack align-items-center">{t("clusterRaftDebug.logEntries.title")}</h3>
            <div className="cluster-debug-entries">
                {withLogs.length === 0 ? (
                    <EmptySet compact className="justify-content-center">
                        {t("clusterRaftDebug.logEntries.empty")}
                    </EmptySet>
                ) : (
                    <>
                        <ul className="nav nav-tabs mb-2">
                            {withLogs.map((result) => (
                                <li key={result.nodeTag} className="nav-item">
                                    <button
                                        type="button"
                                        className={classNames("nav-link no-decor", {
                                            active: result.nodeTag === activeNodeTag,
                                        })}
                                        onClick={() => setSelectedNodeTag(result.nodeTag)}
                                    >
                                        <div className="d-flex gap-1 align-items-center">
                                            <Icon
                                                icon={result.info?.role === "Leader" ? "node-leader" : "cluster-member"}
                                                color="node"
                                            />
                                            <span className="text-nowrap">
                                                {t("clusterRaftDebug.nodeName", { nodeTag: result.nodeTag })}
                                            </span>
                                        </div>
                                    </button>
                                </li>
                            ))}
                        </ul>
                        {entries.length === 0 ? (
                            <EmptySet compact className="justify-content-center">
                                {t("clusterRaftDebug.logEntries.empty")}
                            </EmptySet>
                        ) : (
                            <>
                                {log && (
                                    <div className="px-1 py-2 small text-muted">
                                        {t("clusterRaftDebug.logEntries.captured", {
                                            context: entries.length > maxEntriesShown ? "truncated" : "all",
                                            captured: entries.length.toLocaleString(),
                                            total: log.TotalEntries.toLocaleString(),
                                            limit: maxEntriesShown,
                                        })}
                                    </div>
                                )}
                                <SizeGetter
                                    render={({ width }) => (
                                        <LogEntriesTable shown={shown} commitIndex={commitIndex} width={width} />
                                    )}
                                />
                            </>
                        )}
                    </>
                )}
            </div>
        </div>
    );
}

function useLogEntryColumns(availableWidth: number, commitIndex: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(availableWidth);
    const getSize = useMemo(() => virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    const columns = useMemo<ColumnDef<LogEntry>[]>(
        () => [
            {
                header: t("clusterRaftDebug.logEntries.columns.index"),
                accessorKey: "Index",
                cell: CellWithCopyWrapper,
                size: getSize(10),
            },
            {
                id: "commandType",
                header: t("clusterRaftDebug.logEntries.columns.commandType"),
                accessorKey: "CommandType",
                cell: CellWithCopyWrapper,
                size: getSize(30),
            },
            {
                header: t("clusterRaftDebug.logEntries.columns.created"),
                accessorKey: "CreateAt",
                cell: CellWithCopyWrapper,
                size: getSize(25),
            },
            {
                header: t("clusterRaftDebug.logEntries.columns.size"),
                accessorFn: (row) => genUtils.formatBytesToSize(row.SizeInBytes),
                cell: CellWithCopyWrapper,
                size: getSize(10),
            },
            {
                header: t("clusterRaftDebug.logEntries.columns.term"),
                accessorKey: "Term",
                cell: CellWithCopyWrapper,
                size: getSize(7),
            },
            {
                header: t("clusterRaftDebug.logEntries.columns.status"),
                accessorFn: (row) =>
                    row.Index <= commitIndex
                        ? t("clusterRaftDebug.logEntries.statusCommitted")
                        : t("clusterRaftDebug.logEntries.statusAppended"),
                cell: CellWithCopyWrapper,
                size: getSize(18),
            },
        ],
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [getSize, commitIndex, t]
    );

    return { columns };
}

function LogEntriesTable({ shown, commitIndex, width }: { shown: LogEntry[]; commitIndex: number; width: number }) {
    const { columns } = useLogEntryColumns(width, commitIndex);

    const table = useReactTable({
        data: shown,
        columns,
        enableSorting: shown.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: shown.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(shown.length, 500)} />;
}
