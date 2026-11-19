import React, { memo, useMemo } from "react";
import Spinner from "react-bootstrap/Spinner";
import { Icon } from "components/common/Icon";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { EmptySet } from "components/common/EmptySet";
import NodeTagPill from "./NodeTagPill";
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
import SummaryBar from "./SummaryBar";
import SizeGetter from "components/common/SizeGetter";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
type OngoingTaskType = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

const EMPTY_TASKS: OngoingTask[] = [];

interface DatabaseOngoingTasksProps {
    packageId: string;
    database: string;
    nodes: string[];
    selectedNode: string;
}

interface DatabaseOngoingTasksWithSizeProps extends DatabaseOngoingTasksProps {
    width: number;
}

function useDatabaseOngoingTasksColumns(availableWidth: number) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const bodyWidth = virtualTableUtils.getTableBodyWidth(
        availableWidth - analyzerConstants.panelHorizontalPaddingInPx
    );

    const taskColumns: ColumnDef<OngoingTask>[] = useMemo(() => {
        const getSize = virtualTableUtils.getCellSizeProvider(bodyWidth);
        return [
            {
                header: t("databaseOngoingTasks.columns.task"),
                id: "type",
                accessorFn: (task) => taskTypeLabel(task.TaskType, t),
                cell: ({ getValue }) => <span>{getValue<string>()}</span>,
                size: getSize(25),
            },
            {
                header: t("databaseOngoingTasks.columns.name"),
                accessorKey: "TaskName",
                cell: TaskNameCell,
                size: getSize(33),
            },
            {
                header: t("databaseOngoingTasks.columns.responsibleNode"),
                id: "responsibleNode",
                accessorFn: (task) => task.ResponsibleNode?.NodeTag ?? "",
                cell: TaskResponsibleNodeCell,
                size: getSize(20),
            },
            {
                header: t("databaseOngoingTasks.columns.state"),
                id: "state",
                accessorFn: (task) => task.TaskState ?? "",
                cell: ({ row }) => <TaskStateBadge task={row.original} />,
                size: getSize(22),
            },
        ];
    }, [bodyWidth, t]);

    return { taskColumns };
}

function taskTypeLabel(type: OngoingTaskType, t: StudioTranslate<"debugPackageAnalyzer">): string {
    const taskTypeLabels: Partial<Record<OngoingTaskType, string>> = {
        Replication: t("taskTypes.externalReplication"),
        RavenEtl: "RavenDB ETL",
        SqlEtl: "SQL ETL",
        OlapEtl: "OLAP ETL",
        ElasticSearchEtl: "Elasticsearch ETL",
        QueueEtl: t("taskTypes.queueEtl"),
        QueueSink: t("taskTypes.queueSink"),
        Backup: t("taskTypes.backup"),
        Subscription: t("taskTypes.subscription"),
        PullReplicationAsHub: t("taskTypes.replicationHub"),
        PullReplicationAsSink: t("taskTypes.replicationSink"),
        SnowflakeEtl: "Snowflake ETL",
        EmbeddingsGeneration: t("taskTypes.embeddingsGeneration"),
        GenAi: t("taskTypes.genAi"),
    };
    return taskTypeLabels[type] ?? type;
}

// Each node reports the full task list for the database - fetch from all nodes and merge by TaskId
// so we get the most complete picture even if one node's data is missing from the package.
// Then filter the displayed rows to tasks running on the selected node.
export default memo(function DatabaseOngoingTasks({
    packageId,
    database,
    nodes,
    selectedNode,
}: DatabaseOngoingTasksProps) {
    return (
        <SizeGetter
            render={({ width }) => (
                <DatabaseOngoingTasksWithSize
                    packageId={packageId}
                    database={database}
                    nodes={nodes}
                    selectedNode={selectedNode}
                    width={width}
                />
            )}
        />
    );
});

function DatabaseOngoingTasksWithSize({
    packageId,
    database,
    nodes,
    selectedNode,
    width,
}: DatabaseOngoingTasksWithSizeProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const { manageServerService } = useServices();

    const tasksAsync = useAsync(async () => {
        const settled = await Promise.allSettled(
            nodes.map((tag) => manageServerService.getDebugPackageDatabaseOngoingTasks(packageId, tag, database))
        );
        const byId = new Map<number, OngoingTask>();
        settled.forEach((outcome) => {
            if (outcome.status === "fulfilled" && outcome.value) {
                (outcome.value.OngoingTasks ?? []).forEach((task) => {
                    if (!byId.has(task.TaskId)) {
                        byId.set(task.TaskId, task);
                    }
                });
            }
        });
        return Array.from(byId.values());
    }, [packageId, database, nodes]);

    const allTasks = tasksAsync.result ?? EMPTY_TASKS;
    const tasks = useMemo(
        () => (selectedNode ? allTasks.filter((task) => task.ResponsibleNode?.NodeTag === selectedNode) : allTasks),
        [allTasks, selectedNode]
    );

    const enabledCount = tasks.filter((task) => !task.Error && task.TaskState === "Enabled").length;
    const disabledCount = tasks.filter((task) => !task.Error && task.TaskState === "Disabled").length;

    const { taskColumns } = useDatabaseOngoingTasksColumns(width);

    const table = useReactTable({
        data: tasks,
        columns: taskColumns,
        enableSorting: tasks.length > analyzerConstants.minRowsForControls,
        enableColumnFilters: tasks.length > analyzerConstants.minRowsForControls,
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (row) => String(row.TaskId),
    });

    const heightInPx = virtualTableUtils.getHeightInPx(tasks.length, 400);

    const emptyMessage =
        allTasks.length === 0
            ? t("databaseOngoingTasks.empty", { database })
            : t("databaseOngoingTasks.emptyOnNode", { nodeTag: selectedNode, database });

    return (
        <div className="database-ongoing-tasks">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="mb-0">{t("databaseOngoingTasks.title")}</h3>
                    {tasksAsync.loading ? (
                        <div className="hstack gap-2 justify-content-center text-muted py-3">
                            <Spinner size="sm" /> {t("databaseOngoingTasks.loading")}
                        </div>
                    ) : tasks.length === 0 ? (
                        <EmptySet compact className="justify-content-center">
                            {emptyMessage}
                        </EmptySet>
                    ) : (
                        <>
                            <SummaryBar
                                items={[
                                    {
                                        icon: "ongoing-tasks",
                                        count: tasks.length,
                                        label: selectedNode
                                            ? t("databaseOngoingTasks.summary.onNode", {
                                                  count: tasks.length,
                                                  nodeTag: selectedNode,
                                              })
                                            : t("databaseOngoingTasks.summary.total", { count: tasks.length }),
                                    },
                                    {
                                        icon: "ongoing-tasks",
                                        iconAddon: "check",
                                        count: enabledCount,
                                        label: t("databaseOngoingTasks.summary.active", { count: enabledCount }),
                                        colorClass: "text-success",
                                    },
                                    {
                                        icon: "ongoing-tasks",
                                        iconAddon: "cancel",
                                        count: disabledCount,
                                        label: t("databaseOngoingTasks.summary.disabled", { count: disabledCount }),
                                        colorClass: "text-warning",
                                    },
                                ]}
                            />
                            <VirtualTable table={table} heightInPx={heightInPx} />
                        </>
                    )}
                </div>
            </div>
        </div>
    );
}

function TaskNameCell({ getValue }: { getValue: () => unknown }) {
    const v = getValue() as string;
    return <span title={v}>{v}</span>;
}

function TaskResponsibleNodeCell({ row }: { row: { original: OngoingTask } }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    return row.original.ResponsibleNode?.NodeTag ? (
        <span className="hstack gap-1 align-items-center">
            <NodeTagPill tag={row.original.ResponsibleNode.NodeTag} />
            {row.original.PinToMentorNode && <span className="small-label">{t("databaseOngoingTasks.pinned")}</span>}
        </span>
    ) : (
        <span className="text-muted">{t("databaseOngoingTasks.notAssigned")}</span>
    );
}

function TaskStateBadge({ task }: { task: OngoingTask }) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    if (task.Error) {
        return (
            <span className="hstack gap-1 text-danger">
                <Icon icon="danger" margin="m-0" /> {t("databaseOngoingTasks.states.error")}
            </span>
        );
    }
    switch (task.TaskState) {
        case "Enabled":
            return (
                <span className="hstack gap-1 text-success">
                    <Icon icon="check" margin="m-0" /> {t("databaseOngoingTasks.states.active")}
                </span>
            );
        case "Disabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="cancel" margin="m-0" /> {t("databaseOngoingTasks.states.disabled")}
                </span>
            );
        case "PartiallyEnabled":
            return (
                <span className="hstack gap-1 text-warning">
                    <Icon icon="warning" margin="m-0" /> {t("databaseOngoingTasks.states.partiallyEnabled")}
                </span>
            );
        default:
            return <span className="text-muted">{task.TaskState}</span>;
    }
}
