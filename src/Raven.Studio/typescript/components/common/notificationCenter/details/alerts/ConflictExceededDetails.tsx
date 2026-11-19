import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type ActionDetails = Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails.ActionDetails;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<ActionDetails>[] {
    return [
        {
            header: t("conflictExceeded.id"),
            accessorKey: "Id",
            meta: { relativeWidth: 30 },
        },
        {
            header: t("conflictExceeded.exceededValue"),
            accessorKey: "Reason",
            meta: { relativeWidth: 30 },
        },
        {
            header: t("conflictExceeded.deletedRevisions"),
            accessorKey: "Deleted",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("fields.date"),
            accessorKey: "Time",
            ...dateColumnOptions,
        },
    ];
}

export default function ConflictExceededDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"ConflictRevisionsExceeded">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const documents = useMemo(() => [...(details.Details ?? [])].reverse(), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={documents}
                columns={columns}
                countKey="table.count.document"
            />
        </AlertDetailsModal>
    );
}
