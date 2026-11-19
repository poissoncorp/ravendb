import React, { useMemo } from "react";
import {
    BaseOngoingTaskPanelProps,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskPeriodicBackupInfo } from "components/models/tasks";
import { useAppUrls } from "hooks/useAppUrls";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import genUtils from "common/generalUtils";
import moment = require("moment");
import assertUnreachable from "../../../../../utils/assertUnreachable";
import timeHelpers from "common/timeHelpers";
import BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
import classNames from "classnames";
import notificationCenter from "common/notifications/notificationCenter";
import backupNow = require("viewmodels/database/tasks/backupNow");
import app from "durandal/app";
import backupNowPeriodicCommand from "commands/database/tasks/backupNowPeriodicCommand";
import Badge from "react-bootstrap/Badge";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import useUniqueId from "components/hooks/useUniqueId";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { createTranslator } from "common/i18n/i18n";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

interface PeriodicBackupPanelProps extends BaseOngoingTaskPanelProps<OngoingTaskPeriodicBackupInfo> {
    forceReload: () => void;
    allowSelect: boolean;
    sourceView: EditPeriodicBackupTaskSourceView;
}

export function formatBackupType(backupType: BackupType, isFull: boolean) {
    const t = createTranslator("ongoingTasks");

    if (!isFull) {
        return t("periodicBackup.backupType.incremental");
    }
    switch (backupType) {
        case "Snapshot":
            return t("periodicBackup.backupType.snapshot");
        case "Backup":
            return t("periodicBackup.backupType.full");
        default:
            assertUnreachable(backupType);
    }
}

function findBackupNowBlockReason(
    t: StudioTranslate<"ongoingTasks">,
    data: OngoingTaskPeriodicBackupInfo,
    runningOnAnotherNode: boolean
): string {
    if (!data.shared.nextBackup) {
        return t("periodicBackup.noBackupDestinations");
    }

    if (runningOnAnotherNode) {
        return t("periodicBackup.backupInProgressOnNode", { nodeTag: data.shared.responsibleNodeTag });
    }

    return null;
}

function Details(props: PeriodicBackupPanelProps) {
    const { data, forceReload } = props;
    const t = useStudioTranslation("ongoingTasks");

    const backupDestinationsHumanized = data.shared.backupDestinations.length
        ? data.shared.backupDestinations.join(", ")
        : t("periodicBackup.noDestinationsDefined");

    const lastFullBackupHumanized = data.shared.lastFullBackup
        ? genUtils.formatDurationByDate(moment.utc(data.shared.lastFullBackup), true)
        : t("periodicBackup.neverBackedUp");

    const lastIncrementalBackupHumanized = data.shared.lastIncrementalBackup
        ? genUtils.formatDurationByDate(moment.utc(data.shared.lastIncrementalBackup), true)
        : null;

    const nextBackupHumanized = useMemo(() => {
        const nextBackup = data.shared.nextBackup;
        if (!nextBackup) {
            return t("notAvailable");
        }

        const now = timeHelpers.utcNowWithSecondPrecision();
        const diff = moment.utc(nextBackup.DateTime).diff(now);

        const backupTypeText = formatBackupType(data.shared.backupType, nextBackup.IsFull);
        const formatDuration = genUtils.formatDuration(moment.duration(diff), true, 2, true);
        const originalDate = nextBackup.OriginalBackupTime;
        return t("periodicBackup.nextBackup", {
            context: originalDate ? "delayed" : undefined,
            duration: formatDuration,
            backupType: backupTypeText,
            date: originalDate ? genUtils.formatUtcDateAsLocal(originalDate) : "",
        });
    }, [data.shared, t]);

    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);

    const isResponsible = data.shared.responsibleNodeTag === localNodeTag;

    const onGoingBackup = data.nodesInfo.map((x) => x.details?.onGoingBackup).find((x) => x);
    const runningOnAnotherNode = onGoingBackup && !isResponsible;

    const onGoingBackupHumanized = useMemo(() => {
        if (!onGoingBackup) {
            return null;
        }

        const fromDuration = genUtils.formatDurationByDate(moment.utc(onGoingBackup.StartTime), true);
        return t("periodicBackup.backupStartedValue", {
            duration: fromDuration,
            backupType: formatBackupType(data.shared.backupType, onGoingBackup.IsFull),
        });
    }, [data.shared, onGoingBackup, t]);

    const retentionPolicyHumanized = useMemo(() => {
        const disabled = data.shared.retentionPolicy ? data.shared.retentionPolicy.Disabled : true;
        if (disabled) {
            return t("periodicBackup.noBackupsRemoved");
        }

        const retentionPolicyPeriod = data.shared.retentionPolicy
            ? data.shared.retentionPolicy.MinimumBackupAgeToKeep
            : "0.0:00:00";

        return genUtils.formatTimeSpan(retentionPolicyPeriod, true);
    }, [data.shared, t]);

    const backupNowBlockReason = findBackupNowBlockReason(t, data, runningOnAnotherNode);

    const backupNowInProgress = !!onGoingBackup;
    const neverBackedUp = !data.shared.lastFullBackup;

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const backupNowVisible = data.shared.serverWide || hasDatabaseAdminAccess;

    const onBackupNow = () => {
        if (onGoingBackup && onGoingBackup.RunningBackupTaskId) {
            notificationCenter.instance.openDetailsForOperationById(
                activeDatabaseTracker.default.database(),
                onGoingBackup.RunningBackupTaskId
            );
            return;
        }

        const backupNowViewModel = new backupNow(formatBackupType(data.shared.backupType, true));

        backupNowViewModel.result.done((confirmResult: backupNowConfirmResult) => {
            if (confirmResult.can) {
                const task = new backupNowPeriodicCommand(
                    databaseName,
                    data.shared.taskId,
                    confirmResult.isFullBackup,
                    data.shared.taskName
                );
                task.execute().done(
                    (backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                        forceReload();

                        const isCurrentNodeResponsibleResult = localNodeTag === backupNowResult.ResponsibleNode;

                        if (backupNowResult && isCurrentNodeResponsibleResult) {
                            // running on this node
                            const operationId = backupNowResult.OperationId;
                            notificationCenter.instance.openDetailsForOperationById(
                                activeDatabaseTracker.default.database(),
                                operationId
                            );
                        }
                    }
                );
            }
        });

        app.showBootstrapDialog(backupNowViewModel);
    };

    return (
        <RichPanelDetails>
            <RichPanelDetailItem label={t("periodicBackup.destinations")}>
                {backupDestinationsHumanized}
            </RichPanelDetailItem>
            <RichPanelDetailItem label={t("periodicBackup.lastExecutedOnNode")}>
                {data.shared.lastExecutingNodeTag || t("notAvailable")}
            </RichPanelDetailItem>
            <RichPanelDetailItem
                label={t("periodicBackup.lastBackup", {
                    context: data.shared.backupType === "Snapshot" ? "snapshot" : "full",
                })}
            >
                {lastFullBackupHumanized}
            </RichPanelDetailItem>
            {lastIncrementalBackupHumanized && (
                <RichPanelDetailItem label={t("periodicBackup.lastIncrementalBackup")}>
                    {lastIncrementalBackupHumanized}
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem label={t("periodicBackup.nextEstimatedBackup")}>
                {nextBackupHumanized}
                {backupNowVisible && (
                    <Badge
                        as="button"
                        onClick={onBackupNow}
                        className={classNames("ms-1 rounded-pill backup-now", {
                            "bg-secondary": !neverBackedUp,
                            "bg-info": backupNowInProgress,
                            "bg-progress": backupNowInProgress,
                            "bg-warning": !backupNowInProgress && neverBackedUp,
                        })}
                        disabled={!!backupNowBlockReason}
                        title={backupNowBlockReason ?? t("periodicBackup.backupNowTitle")}
                        bg="secondary"
                    >
                        <Icon icon="backup" />
                        <span>
                            {isResponsible && backupNowInProgress
                                ? t("periodicBackup.showBackupProgress")
                                : t("periodicBackup.backupNow")}
                        </span>
                    </Badge>
                )}
            </RichPanelDetailItem>
            {onGoingBackupHumanized && (
                <RichPanelDetailItem label={t("periodicBackup.backupStarted")}>
                    {onGoingBackupHumanized}
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem label={t("periodicBackup.retentionPolicy")}>
                {retentionPolicyHumanized}
            </RichPanelDetailItem>
        </RichPanelDetails>
    );
}

function BackupEncryption(props: { encrypted: boolean }) {
    const t = useStudioTranslation("ongoingTasks");

    return (
        <div>
            {props.encrypted ? (
                <Icon icon="encryption" color="success" title={t("periodicBackup.encrypted")} margin="m-0" />
            ) : (
                <Icon icon="unlock" color="muted" title={t("periodicBackup.notEncrypted")} margin="m-0" />
            )}
        </div>
    );
}

export function PeriodicBackupPanel(props: PeriodicBackupPanelProps) {
    const { data, allowSelect, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, sourceView } =
        props;
    const t = useStudioTranslation("ongoingTasks");

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const isServerWide = data.shared.serverWide;
    const editUrl = forCurrentDatabase.editPeriodicBackupTask(sourceView, false, data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    const statusDropdownId = useUniqueId("statusDropdown");

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {allowSelect && canEdit && (
                        <RichPanelSelect>
                            <Form.Check
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="periodic-backup" />
                        {t("taskType.periodicBackup")}
                    </span>
                    <OngoingTaskResponsibleNode task={data} />
                    <BackupEncryption encrypted={data.shared.encrypted} />
                    <ConditionalPopover
                        conditions={{
                            isActive: isServerWide,
                            message: t("periodicBackup.serverWideStatusInfo"),
                        }}
                    >
                        <OngoingTaskStatus
                            task={data}
                            canEdit={canEdit}
                            onTaskOperation={onTaskOperation}
                            isTogglingState={isTogglingState(data.shared.taskId)}
                            id={statusDropdownId}
                        />
                    </ConditionalPopover>

                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                        isDetailsOpen={detailsVisible}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <Details {...props} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
