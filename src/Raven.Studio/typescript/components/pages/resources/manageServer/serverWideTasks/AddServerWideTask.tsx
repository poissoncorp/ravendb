import React from "react";
import appUrl from "common/appUrl";
import { AboutViewHeading } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { AddTaskCardList, TaskCardCategory } from "components/pages/database/tasks/shared/AddTaskCardList";
import { PerDatabaseOngoingTasksLink } from "./partials/PerDatabaseOngoingTasksLink";
import { ServerWideTasksInfoHub } from "./partials/ServerWideTasksInfoHub";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AddServerWideTask() {
    const t = useStudioTranslation("serverWideTasks");
    const hasServerWideBackups = useAppSelector(licenseSelectors.statusValue("HasServerWideBackups"));
    const hasServerWideExternalReplications = useAppSelector(
        licenseSelectors.statusValue("HasServerWideExternalReplications")
    );

    const serverWideTasks: TaskCardCategory[] = [
        {
            categoryName: t("taskType.replication"),
            categoryIcon: "external-replication",
            tasks: [
                {
                    title: t("add.replicationTitle"),
                    description: t("add.replicationDescription"),
                    iconName: "external-replication",
                    variant: "Replication",
                    target: "serverWideExternalReplication",
                    link: appUrl.forEditServerWideExternalReplication(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideExternalReplications,
                },
            ],
        },
        {
            categoryName: t("taskType.backup"),
            categoryIcon: "backup",
            tasks: [
                {
                    title: t("add.backupTitle"),
                    description: t("add.backupDescription"),
                    iconName: "periodic-backup",
                    variant: "Backup",
                    target: "serverWidePeriodicBackup",
                    link: appUrl.forEditServerWideBackup(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideBackups,
                },
            ],
        },
    ];

    return (
        <div className="content-margin">
            <div className="d-flex justify-content-between align-items-center mb-4">
                <AboutViewHeading
                    title={t("add.title")}
                    icon="server-wide-tasks"
                    iconAddon="plus"
                    backUrl={appUrl.forServerWideTasks()}
                    marginBottom={0}
                />
                <div className="d-flex align-items-center gap-3">
                    <PerDatabaseOngoingTasksLink />
                    <ServerWideTasksInfoHub />
                </div>
            </div>
            <AddTaskCardList categories={serverWideTasks} />
        </div>
    );
}
