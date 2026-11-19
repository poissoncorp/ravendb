import React, { useCallback, useEffect, useReducer, useState } from "react";
import { PeriodicBackupPanel } from "../ongoingTasks/panels/PeriodicBackupPanel";
import appUrl from "common/appUrl";
import { useServices } from "hooks/useServices";
import { ongoingTasksReducer, ongoingTasksReducerInitializer } from "../ongoingTasks/partials/OngoingTasksReducer";
import useInterval from "hooks/useInterval";
import useTimeout from "hooks/useTimeout";
import { OngoingTaskInfo, OngoingTaskPeriodicBackupInfo } from "components/models/tasks";
import { BaseOngoingTaskPanelProps, taskKey, useOngoingTasksOperations } from "../shared/shared";
import router from "plugins/router";
import PeriodicBackupStatus = Raven.Client.Documents.Operations.Backups.PeriodicBackupStatus;
import { loadableData } from "components/models/common";
import genUtils from "common/generalUtils";
import moment from "moment";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import { HrHeader } from "components/common/HrHeader";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "components/common/RichPanel";
import { FlexGrow } from "components/common/FlexGrow";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import OngoingTaskOperationConfirm from "../shared/OngoingTaskOperationConfirm";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { useRavenLink } from "components/hooks/useRavenLink";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface manualBackupListModel {
    backupType: Raven.Client.Documents.Operations.Backups.BackupType;
    encrypted: boolean;
    nodeTag: string;
    destinations: string[];
    lastFullBackup: string;
}

function mapManualBackup(dto: PeriodicBackupStatus, t: StudioTranslate<"backups">): manualBackupListModel {
    const destinations: string[] = [];

    if (dto.LocalBackup && dto.LocalBackup.BackupDirectory) {
        destinations.push(t("manualBackup.destinationLocal"));
    }
    if (!dto.UploadToS3.Skipped) {
        destinations.push("S3");
    }
    if (!dto.UploadToGlacier.Skipped) {
        destinations.push("Glacier");
    }
    if (!dto.UploadToGlacier.Skipped) {
        destinations.push("Glacier");
    }
    if (!dto.UploadToAzure.Skipped) {
        destinations.push("Azure");
    }
    if (!dto.UploadToGoogleCloud.Skipped) {
        destinations.push("Google Cloud");
    }
    if (!dto.UploadToFtp.Skipped) {
        destinations.push("Ftp");
    }

    return {
        backupType: dto.BackupType,
        encrypted: dto.IsEncrypted,
        nodeTag: dto.NodeTag,
        destinations,
        lastFullBackup: dto.LastFullBackup,
    };
}

interface ManualBackupProps {
    model: loadableData<manualBackupListModel>;
}

function ManualBackup(props: ManualBackupProps) {
    const t = useStudioTranslation("backups");
    const { model } = props;

    if (model.status === "failure") {
        return (
            <div className="bg-danger">
                {t("manualBackup.loadError", { message: model.error.responseJSON.Message })}
            </div>
        );
    }

    if (model.status === "loading" || model.status === "idle") {
        return (
            <div className="manual-backup d-flex justify-content-center">
                <Spinner className="spinner-gradient" />
            </div>
        );
    }

    const backup = model.data;

    if (!backup) {
        return (
            <div className="manual-backup">
                <EmptySet>{t("manualBackup.empty")}</EmptySet>
            </div>
        );
    }

    const lastFullBackupHumanized = backup.lastFullBackup
        ? genUtils.formatDurationByDate(moment.utc(backup.lastFullBackup), true)
        : t("manualBackup.neverBackedUp");

    return (
        <RichPanel className="destination-item recent-backup">
            <RichPanelHeader className="flex-horizontal p-2">
                <RichPanelDetails className="p-0">
                    <RichPanelDetailItem label={t("manualBackup.recentBackup")} title={lastFullBackupHumanized}>
                        {lastFullBackupHumanized}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem label={t("manualBackup.type")}>
                        {t("manualBackup.backupType", { context: backup.backupType })}
                    </RichPanelDetailItem>
                    <RichPanelDetailItem label={t("manualBackup.destinations")}>
                        {backup.destinations ? backup.destinations.join(", ") : t("manualBackup.noDestinations")}
                    </RichPanelDetailItem>
                </RichPanelDetails>

                <FlexGrow />
                <div className="flex-horizontal align-items-center p-2">
                    {backup.encrypted ? (
                        <div title={t("manualBackup.encrypted")}>
                            <Icon icon="encryption" color="success" margin="m-0" />
                        </div>
                    ) : (
                        <div title={t("manualBackup.notEncrypted")}>
                            <Icon icon="unlock" color="muted" margin="m-0" />
                        </div>
                    )}

                    <div className="text-node ms-3" title={t("manualBackup.nodeTitle")}>
                        <Icon icon="cluster-node" />
                        <span>{backup.nodeTag}</span>
                    </div>
                </div>
            </RichPanelHeader>
        </RichPanel>
    );
}

export function BackupsPage() {
    const t = useStudioTranslation("backups");
    const tCommon = useStudioTranslation("common");
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const { tasksService } = useServices();
    const [manualBackup, setManualBackup] = useState<loadableData<manualBackupListModel>>({
        status: "idle",
        data: null,
    });

    const backupDocsLink = useRavenLink({ hash: "GMBYOH" });

    const db = useAppSelector(databaseSelectors.activeDatabase);
    const [tasks, dispatch] = useReducer(ongoingTasksReducer, db, ongoingTasksReducerInitializer);

    const fetchTasks = useCallback(
        async (location: databaseLocationSpecifier) => {
            try {
                const tasks = await tasksService.getOngoingTasks(db.name, location);
                dispatch({
                    type: "TasksLoaded",
                    location,
                    tasks,
                });
            } catch (e) {
                dispatch({
                    type: "TasksLoadError",
                    location,
                    error: e,
                });
            }
        },
        [db.name, tasksService, dispatch]
    );

    const fetchManualBackup = useCallback(
        async (silent = false) => {
            if (!silent) {
                setManualBackup({
                    data: null,
                    status: "loading",
                });
            }

            try {
                const manualBackup = await tasksService.getManualBackup(db.name);

                setManualBackup({
                    data: manualBackup.Status ? mapManualBackup(manualBackup.Status, t) : null,
                    status: "success",
                });
            } catch (e) {
                setManualBackup({
                    data: null,
                    error: e,
                    status: "failure",
                });
            }
        },
        [db.name, tasksService, t]
    );

    const reload = useCallback(async () => {
        const loadTasks = tasks.locations.map((location) => fetchTasks(location));
        const loadManualBackup = fetchManualBackup(true);
        await Promise.all(loadTasks.concat(loadManualBackup));
    }, [tasks, fetchTasks, fetchManualBackup]);

    useInterval(reload, 10_000);

    const loadMissing = async () => {
        if (tasks.tasks.length > 0) {
            const loadTasks = tasks.tasks[0].nodesInfo.map(async (nodeInfo) => {
                if (nodeInfo.status === "idle") {
                    await fetchTasks(nodeInfo.location);
                }
            });

            await Promise.all(loadTasks);
        }
    };

    useTimeout(loadMissing, 3_000);

    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const initialLocation = DatabaseUtils.getFirstLocation(db, localNodeTag);

    useEffect(() => {
        // noinspection JSIgnoredPromiseFromCall
        fetchTasks({
            nodeTag: initialLocation.nodeTag,
            shardNumber: initialLocation.shardNumber,
        });

        // noinspection JSIgnoredPromiseFromCall
        fetchManualBackup();
    }, [fetchManualBackup, fetchTasks, initialLocation.nodeTag, initialLocation.shardNumber]);

    const canNavigateToServerWideTasks = isClusterAdminOrClusterNode;
    const serverWideTasksUrl = appUrl.forServerWideTasks();

    const navigateToRestoreDatabase = () => {
        const url = appUrl.forDatabases("restore");
        router.navigate(url);
    };

    const { onTaskOperation, operationConfirm, cancelOperationConfirm, isDeleting, isTogglingState } =
        useOngoingTasksOperations(reload);

    const sharedPanelProps: Omit<BaseOngoingTaskPanelProps<OngoingTaskInfo>, "data"> = {
        onTaskOperation: onTaskOperation,
        isDeleting,
        isTogglingState,
        isSelected: notImplemented,
        toggleSelection: notImplemented,
    };

    const createNewPeriodicBackupTask = () => {
        const url = appUrl.forEditPeriodicBackupTask(db.name, "Backups", false);
        router.navigate(url);
    };

    const createManualBackup = () => {
        const url = appUrl.forEditManualBackup(db.name);
        router.navigate(url);
    };

    const backups = tasks.tasks.filter((x) => x.shared.taskType === "Backup") as OngoingTaskPeriodicBackupInfo[];

    const hasPeriodicBackups = useAppSelector(licenseSelectors.statusValue("HasPeriodicBackup"));

    return (
        <div className="content-margin flex-grow-1 flex-stretch-items">
            {operationConfirm && <OngoingTaskOperationConfirm {...operationConfirm} toggle={cancelOperationConfirm} />}

            <div className="flex-vertical">
                {hasDatabaseAdminAccess && (
                    <div className="flex-shrink-0 hstack gap-2 mb-4">
                        <Button
                            variant="secondary"
                            onClick={navigateToRestoreDatabase}
                            title={t("restoreDatabase.title")}
                        >
                            <Icon icon="restore-backup" /> {t("restoreDatabase.button")}
                        </Button>
                        <FlexGrow />
                        <AboutViewFloating>
                            <AccordionItemWrapper targetId="1" icon="about" color="info">
                                <p>
                                    <StudioTrans
                                        ns="backups"
                                        i18nKey="about.backups"
                                        components={{ strong: <strong /> }}
                                    />
                                </p>
                                <div>
                                    {t("about.viewEnablesCreating")}
                                    <ul>
                                        <li className="margin-top-xxs">
                                            <StudioTrans
                                                ns="backups"
                                                i18nKey="about.manualBackup"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                        <li className="margin-top-xxs">
                                            <StudioTrans
                                                ns="backups"
                                                i18nKey="about.periodicBackups"
                                                components={{ strong: <strong />, br: <br /> }}
                                            />
                                        </li>
                                    </ul>
                                </div>
                                <hr />
                                <div className="small-label mb-2">{tCommon("usefulLinks")}</div>
                                <a href={backupDocsLink} target="_blank">
                                    <Icon icon="newtab" /> {t("about.docsLink")}
                                </a>
                            </AccordionItemWrapper>
                        </AboutViewFloating>
                    </div>
                )}

                <div className="flex-shrink-0">
                    <HrHeader>
                        <Icon icon="backup" />
                        <span>{t("manualBackup.heading")}</span>
                    </HrHeader>

                    {hasDatabaseAdminAccess && (
                        <div className="mb-3 flex-shrink-0">
                            <Button
                                variant="primary"
                                title={t("manualBackup.createTitle")}
                                onClick={createManualBackup}
                            >
                                <Icon icon="backup" /> {t("manualBackup.create")}
                            </Button>
                        </div>
                    )}
                </div>

                <ManualBackup model={manualBackup} />

                <div className="flex-shrink-0">
                    <HrHeader
                        right={
                            canNavigateToServerWideTasks && (
                                <a
                                    className="btn btn-link btn-xs"
                                    title={t("periodicBackup.serverWideTasksTitle")}
                                    href={serverWideTasksUrl}
                                >
                                    {t("periodicBackup.serverWideTasksLink")}
                                </a>
                            )
                        }
                    >
                        <Icon icon="manage-ongoing-tasks" />
                        <span>
                            {t("periodicBackup.heading", { count: backups.length })}{" "}
                            {!hasPeriodicBackups && <LicenseRestrictedBadge licenseRequired="Professional +" />}
                        </span>
                    </HrHeader>
                    {hasDatabaseWriteAccess && (
                        <div className="mb-3">
                            <Button
                                variant="primary"
                                onClick={createNewPeriodicBackupTask}
                                title={t("periodicBackup.createTitle")}
                            >
                                <Icon icon="backup" /> {t("periodicBackup.create")}
                            </Button>
                        </div>
                    )}
                </div>

                <div className="flex-vertical">
                    <div className="scroll flex-grow">
                        <div key="backups">
                            {backups.length > 0 && (
                                <>
                                    {backups.map((x) => (
                                        <PeriodicBackupPanel
                                            sourceView="Backups"
                                            forceReload={reload}
                                            allowSelect={false}
                                            {...sharedPanelProps}
                                            key={taskKey(x.shared)}
                                            data={x}
                                        />
                                    ))}
                                </>
                            )}
                        </div>

                        {backups.length === 0 && <EmptySet>{t("periodicBackup.empty")}</EmptySet>}
                    </div>
                </div>
            </div>
        </div>
    );
}

const notImplemented = (): boolean => {
    console.error("Not implemented for backup page");
    return false;
};
