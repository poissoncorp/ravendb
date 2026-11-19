import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskInfo,
    OngoingTaskSharedInfo,
} from "components/models/tasks";
import useBoolean from "hooks/useBoolean";
import React, { ReactNode, useCallback, useEffect, useReducer, useState } from "react";
import router from "plugins/router";
import { RichPanelDetailItem, RichPanelName } from "components/common/RichPanel";
import Spinner from "react-bootstrap/Spinner";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { Icon } from "components/common/Icon";
import { OngoingTaskOperationConfirmType } from "./OngoingTaskOperationConfirm";
import assertUnreachable from "components/utils/assertUnreachable";
import messagePublisher from "common/messagePublisher";
import { getRequestErrorMessage } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";

import {
    ongoingTasksReducer,
    ongoingTasksReducerInitializer,
} from "components/pages/database/tasks/ongoingTasks/partials/OngoingTasksReducer";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { getLicenseLimitReachStatus } from "components/utils/licenseLimitsUtils";
import { useAppUrls } from "hooks/useAppUrls";
import appUrl from "common/appUrl";
import { CounterBadge } from "components/common/CounterBadge";
import {
    TaskCardCategory,
    TaskCardDisabledCondition,
    TaskCardInfo,
} from "components/pages/database/tasks/shared/AddTaskCardList";
import { useTaskCardFilters } from "components/pages/database/tasks/shared/useTaskCardFilters";
import { ongoingTaskCapabilities, OngoingTaskTarget } from "./ongoingTaskCapabilities";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getAccessRequiredMessage } from "components/utils/accessUtils";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import {
    getServerWideShortName,
    serverWideConnectionStringPrefix,
} from "components/pages/database/settings/connectionStrings/connectionStringsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { createTranslator } from "common/i18n/i18n";
import ModifyOngoingTaskResult = Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult;

export interface BaseOngoingTaskPanelProps<T extends OngoingTaskInfo> {
    data: T;
    isSelected: (id: number) => boolean;
    toggleSelection: (checked: boolean, taskName: OngoingTaskSharedInfo) => void;
    onToggleDetails?: (newState: boolean) => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: (id: number) => boolean;
    isTogglingState: (id: number) => boolean;
}

export function useTasksOperations(editUrl: string, props: BaseOngoingTaskPanelProps<OngoingTaskInfo>) {
    const { onToggleDetails } = props;
    const { value: detailsVisible, toggle: toggleDetailsVisible } = useBoolean(false);

    const onEdit = useCallback(() => {
        router.navigate(editUrl);
    }, [editUrl]);

    const toggleDetails = useCallback(() => {
        toggleDetailsVisible();
        onToggleDetails?.(!detailsVisible);
    }, [onToggleDetails, toggleDetailsVisible, detailsVisible]);

    return {
        detailsVisible,
        toggleDetails,
        onEdit,
    };
}

export function OngoingTaskResponsibleNode(props: { task: OngoingTaskInfo }) {
    const t = useStudioTranslation("shared");
    const { task } = props;
    const preferredMentor = task.shared.mentorNodeTag;
    const currentNode = task.shared.responsibleNodeTag;

    const db = useAppSelector(databaseSelectors.activeDatabase);

    if (db?.isSharded) {
        // for sharded databases there are multiple responsible nodes, so user
        // can see it inside details only
        return null;
    }

    const usingNotPreferredNode = preferredMentor && currentNode ? preferredMentor !== currentNode : false;

    if (currentNode) {
        return (
            <div className="node">
                {usingNotPreferredNode ? (
                    <>
                        <span className="text-danger pulse" title={t("responsibleNode.preferredNodeTitle")}>
                            <Icon icon="cluster-node" />
                            {preferredMentor}
                        </span>

                        <span className="text-success" title={t("responsibleNode.temporaryNodeTitle")}>
                            <Icon icon="arrow-right" color="danger" className="pulse" />
                            {currentNode}
                        </span>
                    </>
                ) : (
                    <span
                        title={
                            task.shared.taskType === "PullReplicationAsHub"
                                ? t("responsibleNode.hubNodeTitle")
                                : t("responsibleNode.clusterNodeTitle")
                        }
                    >
                        <Icon icon="cluster-node" />
                        {currentNode}
                    </span>
                )}
            </div>
        );
    }

    return (
        <div title={t("responsibleNode.noNodeTitle")}>
            <Icon icon="cluster-node" /> {t("responsibleNode.notAvailable")}
        </div>
    );
}

export function OngoingTaskName(props: { task: OngoingTaskInfo; canEdit: boolean; editUrl: string }) {
    const t = useStudioTranslation("shared");
    const { task, editUrl, canEdit } = props;
    return (
        <RichPanelName>
            {canEdit ? (
                <a href={editUrl} title={t("taskNameTitle", { name: task.shared.taskName })}>
                    {task.shared.taskName}
                </a>
            ) : (
                <span className="text-muted">{task.shared.taskName}</span>
            )}
        </RichPanelName>
    );
}

interface OngoingTaskStatusProps {
    task: OngoingTaskInfo;
    canEdit: boolean;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isTogglingState: boolean;
    id?: string;
}

export function OngoingTaskStatus(props: OngoingTaskStatusProps) {
    const t = useStudioTranslation("shared");
    const { task, canEdit, onTaskOperation, isTogglingState, id } = props;
    return (
        <Dropdown id={id}>
            <Dropdown.Toggle
                disabled={!canEdit || isTogglingState}
                variant={task.shared.taskState === "Disabled" ? "warning" : "secondary"}
            >
                {isTogglingState && <Spinner size="sm" />} {t("status.taskState", { context: task.shared.taskState })}
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={() => onTaskOperation("enable", [task.shared])}>
                    <Icon icon="play" color="success" /> {t("status.enable")}
                </Dropdown.Item>
                <Dropdown.Item onClick={() => onTaskOperation("disable", [task.shared])}>
                    <Icon icon="stop" color="danger" />
                    {t("status.disable")}
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
}

interface OngoingTaskActionsProps {
    canEdit: boolean;
    task: OngoingTaskInfo;
    toggleDetails?: () => void;
    onEdit: () => void;
    onTaskOperation: (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => void;
    isDeleting: boolean;
    isDetailsOpen?: boolean;
    isEtl?: boolean;
}

export function OngoingTaskActions(props: OngoingTaskActionsProps) {
    const t = useStudioTranslation("shared");
    const { canEdit, task, onEdit, toggleDetails, onTaskOperation, isDeleting, isDetailsOpen, isEtl } = props;

    return (
        <div className="actions">
            <ButtonGroup>
                {!isEtl && (
                    <Button variant="secondary" onClick={toggleDetails} title={t("actions.detailsTitle")}>
                        <Icon icon={isDetailsOpen ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                )}
                {!task.shared.serverWide && (
                    <Button variant="secondary" onClick={onEdit} title={t("actions.editTitle")}>
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                )}
                {!task.shared.serverWide && (
                    <ButtonWithSpinner
                        variant="danger"
                        disabled={!canEdit}
                        isSpinning={isDeleting}
                        onClick={() => onTaskOperation("delete", [task.shared])}
                        title={t("actions.deleteTitle")}
                        spinnerMargin="m-0"
                        icon={{
                            icon: "trash",
                            margin: "m-0",
                        }}
                    ></ButtonWithSpinner>
                )}
            </ButtonGroup>
        </div>
    );
}

export function ConnectionStringItem(props: {
    canEdit: boolean;
    connectionStringName: string;
    connectionStringType: StudioConnectionType;
    databaseName: string;
    connectionStringDefined: boolean;
}) {
    const t = useStudioTranslation("shared");
    const { canEdit, connectionStringDefined, connectionStringName, connectionStringType, databaseName } = props;

    const isServerWide = connectionStringName?.startsWith(serverWideConnectionStringPrefix);

    const connectionStringsUrl = isServerWide
        ? appUrl.forServerWideConnectionStrings(connectionStringType, getServerWideShortName(connectionStringName))
        : appUrl.forConnectionStrings(databaseName, connectionStringType, connectionStringName);

    if (connectionStringDefined) {
        return (
            <RichPanelDetailItem label={t("connectionString.label")}>
                {canEdit ? (
                    <a title={t("connectionString.nameTitle")} target="_blank" href={connectionStringsUrl}>
                        {connectionStringName}
                    </a>
                ) : (
                    <div>{connectionStringName}</div>
                )}
            </RichPanelDetailItem>
        );
    }

    return (
        <RichPanelDetailItem label={t("connectionString.label")}>
            <Icon icon="danger" color="danger" />
            <span className="text-danger">{t("connectionString.notDefined")}</span>
        </RichPanelDetailItem>
    );
}

export function DestinationUrlItem({ destinationUrl, label }: { destinationUrl: string; label?: string }) {
    const t = useStudioTranslation("shared");

    if (!destinationUrl) {
        return null;
    }

    return (
        <RichPanelDetailItem label={label ?? t("destinationUrl")}>
            <a href={destinationUrl} target="_blank">
                {destinationUrl}
            </a>
        </RichPanelDetailItem>
    );
}

// Mode is a [Flags] enum, so the bidirectional case arrives as a combined string (e.g. "HubToSink, SinkToHub").
// Use includes() to stay robust to the flag formatting and render a readable label.
export function formatReplicationMode(mode: Raven.Client.Documents.Operations.Replication.PullReplicationMode): string {
    const t = createTranslator("shared");
    const hubToSink = mode?.includes("HubToSink");
    const sinkToHub = mode?.includes("SinkToHub");

    if (hubToSink && sinkToHub) {
        return t("replicationMode.both");
    }
    if (hubToSink) {
        return t("replicationMode.hubToSink");
    }
    if (sinkToHub) {
        return t("replicationMode.sinkToHub");
    }
    return null;
}

export function EmptyScriptsWarning(props: { task: AnyEtlOngoingTaskInfo }) {
    const t = useStudioTranslation("shared");
    const emptyScripts = findScriptsWithOutMatchingDocuments(props.task);

    if (!emptyScripts.length) {
        return null;
    }

    return (
        <RichPanelDetailItem className="text-warning">
            <small>
                <Icon icon="warning" />
                {t("emptyScriptsWarning", { scripts: emptyScripts.join(", ") })}
            </small>
        </RichPanelDetailItem>
    );
}

function findScriptsWithOutMatchingDocuments(
    data: OngoingTaskInfo<OngoingTaskSharedInfo, OngoingEtlTaskNodeInfo>
): string[] {
    const perScriptCounts = new Map<string, number>();
    data.nodesInfo.forEach((node) => {
        if (node.etlProgress) {
            node.etlProgress.forEach((progress) => {
                const transformationName = progress.transformationName;
                perScriptCounts.set(
                    transformationName,
                    (perScriptCounts.get(transformationName) ?? 0) + progress.global.total
                );
            });
        }
    });

    return Array.from(perScriptCounts.entries())
        .filter((x) => x[1] === 0)
        .map((x) => x[0]);
}

export function taskKey(task: OngoingTaskSharedInfo) {
    // we don't want to use taskId here - as it changes after edit
    return task.taskType + "-" + task.taskName;
}

interface OperationConfirm {
    type: OngoingTaskOperationConfirmType;
    onConfirm: () => void;
    taskSharedInfos: OngoingTaskSharedInfo[];
}

export function useOngoingTasksOperations(reload: () => void) {
    const t = useStudioTranslation("shared");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const [togglingTaskIds, setTogglingTaskIds] = useState<number[]>([]);
    const [deletingTaskIds, setDeletingTaskIds] = useState<number[]>([]);

    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const toggleOngoingTasks = async (enable: boolean, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setTogglingTaskIds((ids) => [...ids, ...taskSharedInfos.map((x) => x.taskId)]);
            const toggleRequests: Promise<ModifyOngoingTaskResult>[] = [];

            for (const task of taskSharedInfos) {
                if ((task.taskState === "Enabled" || task.taskState === "PartiallyEnabled") && enable) {
                    continue;
                }
                if (task.taskState === "Disabled" && !enable) {
                    continue;
                }

                toggleRequests.push(tasksService.toggleOngoingTask(databaseName, task, enable));
            }

            if (toggleRequests.length === 0) {
                return;
            }

            await Promise.all(toggleRequests);
            messagePublisher.reportSuccess(
                t("operations.toggleSuccess", {
                    context: enable ? "enabled" : "disabled",
                    count: toggleRequests.length,
                })
            );
            reload();
        } finally {
            setTogglingTaskIds((ids) => ids.filter((x) => !taskSharedInfos.map((x) => x.taskId).includes(x)));
        }
    };

    const deleteOngoingTasks = async (taskSharedInfos: OngoingTaskSharedInfo[]) => {
        try {
            setDeletingTaskIds((ids) => [...ids, ...taskSharedInfos.map((x) => x.taskId)]);

            const deleteRequests: Promise<ModifyOngoingTaskResult>[] = taskSharedInfos.map((task) =>
                tasksService.deleteOngoingTask(databaseName, task)
            );

            await Promise.all(deleteRequests);

            messagePublisher.reportSuccess(t("operations.deleteSuccess", { count: deleteRequests.length }));
            reload();
        } finally {
            setDeletingTaskIds((ids) => ids.filter((x) => !taskSharedInfos.map((x) => x.taskId).includes(x)));
        }
    };

    const onTaskOperation = (type: OngoingTaskOperationConfirmType, taskSharedInfos: OngoingTaskSharedInfo[]) => {
        switch (type) {
            case "enable": {
                setOperationConfirm({
                    type: "enable",
                    onConfirm: () => toggleOngoingTasks(true, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "disable": {
                setOperationConfirm({
                    type: "disable",
                    onConfirm: () => toggleOngoingTasks(false, taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            case "delete": {
                setOperationConfirm({
                    type: "delete",
                    onConfirm: () => deleteOngoingTasks(taskSharedInfos),
                    taskSharedInfos,
                });
                break;
            }
            default:
                assertUnreachable(type);
        }
    };

    return {
        onTaskOperation,
        operationConfirm,
        cancelOperationConfirm: () => setOperationConfirm(null),
        isDeleting: (id: number) => deletingTaskIds.includes(id),
        isTogglingState: (id: number) => togglingTaskIds.includes(id),
        isDeletingAny: deletingTaskIds.length > 0,
        isTogglingStateAny: togglingTaskIds.length > 0,
    };
}

export function useNewOngoingTasks({ isAiOnly = false }: { isAiOnly?: boolean }) {
    const t = useStudioTranslation("shared");
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { tasksService } = useServices();
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(db?.name, location);
                dispatch({
                    type: "TasksLoaded",
                    location,
                    tasks,
                });
            } catch (e) {
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: getRequestErrorMessage(e),
                });
            }
        },
        [db, tasksService, dispatch]
    );

    const reload = useCallback(async () => {
        // if database is sharded we need to load from both orchestrator and target node point of view
        // in case of non-sharded - we have single level: node

        if (db?.isSharded) {
            const orchestratorTasks = db.nodes.map((node) => fetchTasks({ nodeTag: node.tag }));
            await Promise.all(orchestratorTasks);
        }

        await Promise.all(tasks.locations.map(fetchTasks));
    }, [tasks, fetchTasks, db]);

    useEffect(() => {
        reload();
    }, []);

    const subscriptionsServerCount = useAppSelector(licenseSelectors.limitsUsage).NumberOfSubscriptionsInCluster;

    const licenseStatus = useAppSelector(licenseSelectors.status);

    const subscriptionsServerLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerCluster"));
    const subscriptionsDatabaseLimit = useAppSelector(
        licenseSelectors.statusValue("MaxNumberOfSubscriptionsPerDatabase")
    );

    const subscriptionsServerLimitStatus = getLicenseLimitReachStatus(
        subscriptionsServerCount,
        subscriptionsServerLimit
    );

    const subscriptionsDatabaseLimitStatus = getLicenseLimitReachStatus(
        tasks.subscriptions.length,
        subscriptionsDatabaseLimit
    );

    const isSubscriptionDisabled =
        subscriptionsServerLimitStatus === "limitReached" || subscriptionsDatabaseLimitStatus === "limitReached";

    const { forCurrentDatabase } = useAppUrls();

    const getSubscriptionLimitReason = () => {
        if (!isSubscriptionDisabled) {
            return null;
        }

        return t("addTask.subscriptionLimit", {
            context: subscriptionsServerLimitStatus === "limitReached" ? "cluster" : "database",
        });
    };

    const isSharded = db?.isSharded;
    const getCanHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation);

    const getDisabledConditions = (opts: {
        accessRequired: databaseAccessLevel;
        isShardingSupported?: boolean;
        customDisabledReason?: ReactNode;
    }): TaskCardDisabledCondition[] => [
        {
            isActive: !getCanHandleOperation(opts.accessRequired),
            message: getAccessRequiredMessage(opts.accessRequired),
        },
        {
            isActive: !opts.isShardingSupported && isSharded,
            message: t("addTask.shardingNotSupported"),
        },
        {
            isActive: !!opts.customDisabledReason,
            message: opts.customDisabledReason,
        },
    ];

    const capabilitiesOf = (
        target: OngoingTaskTarget,
        customDisabledReason?: ReactNode
    ): Pick<TaskCardInfo, "licenseBadge" | "showLicenseBadge" | "disabledConditions"> => {
        const { licenseFlags, licenseBadge, isShardingSupported, accessRequired } = ongoingTaskCapabilities[target];

        return {
            licenseBadge,
            showLicenseBadge: licenseFlags.length > 0 && licenseFlags.every((flag) => !licenseStatus?.[flag]),
            disabledConditions: getDisabledConditions({ accessRequired, isShardingSupported, customDisabledReason }),
        };
    };

    let ongoingTasks: TaskCardCategory[] = [
        {
            categoryName: t("addTask.categories.ai"),
            categoryIcon: "ai",
            tasks: [
                {
                    title: t("addTask.tasks.genAi.title"),
                    description: t("addTask.tasks.genAi.description"),
                    iconName: "genai",
                    variant: "AI",
                    target: "GenAi",
                    link: forCurrentDatabase.editGenAiTaskUrl(),
                    ...capabilitiesOf("GenAi"),
                },
                {
                    title: t("addTask.tasks.embeddingsGeneration.title"),
                    description: t("addTask.tasks.embeddingsGeneration.description"),
                    iconName: "ai-etl",
                    variant: "AI",
                    target: "EmbeddingsGeneration",
                    link: forCurrentDatabase.editEmbeddingsGenerationTaskUrl(),
                    ...capabilitiesOf("EmbeddingsGeneration"),
                },
            ],
        },
        {
            categoryName: t("addTask.categories.replication"),
            categoryIcon: "replication",
            tasks: [
                {
                    title: t("addTask.tasks.externalReplication.title"),
                    description: t("addTask.tasks.externalReplication.description"),
                    iconName: "external-replication",
                    variant: "Replication",
                    target: "ExternalReplication",
                    link: forCurrentDatabase.editExternalReplicationTaskUrl(),
                    ...capabilitiesOf("ExternalReplication"),
                },
                {
                    title: t("addTask.tasks.replicationHub.title"),
                    description: t("addTask.tasks.replicationHub.description"),
                    iconName: "pull-replication-hub",
                    variant: "Replication",
                    target: "ReplicationHub",
                    link: forCurrentDatabase.editReplicationHubTaskUrl(),
                    ...capabilitiesOf("ReplicationHub"),
                },
                {
                    title: t("addTask.tasks.replicationSink.title"),
                    description: t("addTask.tasks.replicationSink.description"),
                    iconName: "pull-replication-agent",
                    variant: "Replication",
                    target: "ReplicationSink",
                    link: forCurrentDatabase.editReplicationSinkTaskUrl(),
                    ...capabilitiesOf("ReplicationSink"),
                },
            ],
        },
        {
            categoryName: t("addTask.categories.backups"),
            categoryIcon: "backup",
            tasks: [
                {
                    title: t("addTask.tasks.periodicBackup.title"),
                    description: t("addTask.tasks.periodicBackup.description"),
                    iconName: "periodic-backup",
                    variant: "Backup",
                    target: "PeriodicBackup",
                    link: forCurrentDatabase.editPeriodicBackupTask("OngoingTasks", false)(),
                    ...capabilitiesOf("PeriodicBackup"),
                },
            ],
        },
        {
            categoryName: t("addTask.categories.subscriptions"),
            categoryIcon: "subscriptions",
            tasks: [
                {
                    title: t("addTask.tasks.subscription.title"),
                    description: t("addTask.tasks.subscription.description"),
                    iconName: "subscriptions",
                    variant: "Subscription",
                    target: "Subscription",
                    link: forCurrentDatabase.editSubscriptionTaskUrl(),
                    ...capabilitiesOf("Subscription", getSubscriptionLimitReason()),
                    counterBadge: (
                        <CounterBadge
                            count={tasks.subscriptions.length}
                            limit={subscriptionsDatabaseLimit}
                            hideNotReached
                        />
                    ),
                },
            ],
        },
        {
            categoryName: "ETL",
            categoryHeaderName: t("addTask.categories.etlHeader"),
            categoryIcon: "etl",
            tasks: [
                {
                    title: "RavenDB ETL",
                    description: t("addTask.tasks.ravenEtl.description"),
                    iconName: "ravendb-etl",
                    variant: "ETL",
                    target: "RavenETL",
                    link: forCurrentDatabase.editRavenEtlTaskUrl(),
                    ...capabilitiesOf("RavenETL"),
                },
                {
                    title: "Elasticsearch ETL",
                    description: t("addTask.tasks.elasticSearchEtl.description"),
                    iconName: "elastic-search-etl",
                    variant: "ETL",
                    target: "ElasticSearchETL",
                    link: forCurrentDatabase.editElasticSearchEtlTaskUrl(),
                    ...capabilitiesOf("ElasticSearchETL"),
                },
                {
                    title: "Kafka ETL",
                    description: t("addTask.tasks.kafkaEtl.description"),
                    iconName: "kafka-etl",
                    variant: "ETL",
                    target: "KafkaETL",
                    link: forCurrentDatabase.editKafkaEtlTaskUrl(),
                    ...capabilitiesOf("KafkaETL"),
                },
                {
                    title: "SQL ETL",
                    description: t("addTask.tasks.sqlEtl.description"),
                    iconName: "sql-etl",
                    variant: "ETL",
                    target: "SqlETL",
                    link: forCurrentDatabase.editSqlEtlTaskUrl(),
                    ...capabilitiesOf("SqlETL"),
                },
                {
                    title: "Snowflake ETL",
                    description: t("addTask.tasks.snowflakeEtl.description"),
                    iconName: "snowflake-etl",
                    variant: "ETL",
                    target: "SnowflakeETL",
                    link: forCurrentDatabase.editSnowflakeEtlTaskUrl(),
                    ...capabilitiesOf("SnowflakeETL"),
                },
                {
                    title: "OLAP ETL",
                    description: t("addTask.tasks.olapEtl.description"),
                    iconName: "olap-etl",
                    variant: "ETL",
                    target: "OlapETL",
                    link: forCurrentDatabase.editOlapEtlTaskUrl(),
                    ...capabilitiesOf("OlapETL"),
                },
                {
                    title: "RabbitMQ ETL",
                    description: t("addTask.tasks.rabbitMqEtl.description"),
                    iconName: "rabbitmq-etl",
                    variant: "ETL",
                    target: "RabbitMqETL",
                    link: forCurrentDatabase.editRabbitMqEtlTaskUrl(),
                    ...capabilitiesOf("RabbitMqETL"),
                },
                {
                    title: t("addTask.tasks.azureQueueStorageEtl.title"),
                    description: t("addTask.tasks.azureQueueStorageEtl.description"),
                    iconName: "azure-queue-storage-etl",
                    variant: "ETL",
                    target: "AzureQueueStorageETL",
                    link: forCurrentDatabase.editAzureQueueStorageEtlTaskUrl(),
                    ...capabilitiesOf("AzureQueueStorageETL"),
                },
                {
                    title: t("addTask.tasks.amazonSqsEtl.title"),
                    description: t("addTask.tasks.amazonSqsEtl.description"),
                    iconName: "amazon-sqs-etl",
                    variant: "ETL",
                    target: "AmazonSqsETL",
                    link: forCurrentDatabase.editAmazonSqsEtlTaskUrl(),
                    ...capabilitiesOf("AmazonSqsETL"),
                },
            ],
        },
        {
            categoryName: t("addTask.categories.sink"),
            categoryHeaderName: t("addTask.categories.sinkHeader"),
            categoryIcon: "hub-sink-replication",
            tasks: [
                {
                    title: t("addTask.tasks.kafkaSink.title"),
                    description: t("addTask.tasks.kafkaSink.description"),
                    iconName: "kafka-sink",
                    variant: "Sink",
                    target: "KafkaSink",
                    link: forCurrentDatabase.editKafkaSinkTaskUrl(),
                    ...capabilitiesOf("KafkaSink"),
                },
                {
                    title: t("addTask.tasks.rabbitMqSink.title"),
                    description: t("addTask.tasks.rabbitMqSink.description"),
                    iconName: "rabbitmq-sink",
                    target: "RabbitMqSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editRabbitMqSinkTaskUrl(),
                    ...capabilitiesOf("RabbitMqSink"),
                },
                {
                    title: t("addTask.tasks.azureServiceBusSink.title"),
                    description: t("addTask.tasks.azureServiceBusSink.description"),
                    iconName: "azure",
                    target: "AzureServiceBusSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editAzureServiceBusSinkTaskUrl(),
                    ...capabilitiesOf("AzureServiceBusSink"),
                },
                {
                    title: t("addTask.tasks.cdcSink.title"),
                    description: t("addTask.tasks.cdcSink.description"),
                    iconName: "cdc-sink",
                    target: "CdcSink",
                    variant: "Sink",
                    link: forCurrentDatabase.editCdcSinkTaskUrl(),
                    ...capabilitiesOf("CdcSink"),
                },
            ],
        },
    ];

    if (isAiOnly) {
        ongoingTasks = ongoingTasks.filter((x) => x.categoryName === t("addTask.categories.ai"));
    }

    return useTaskCardFilters(ongoingTasks);
}
