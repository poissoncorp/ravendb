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

type QueueSinkErrorInfo = Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<QueueSinkErrorInfo>[] {
    return [
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            header: t("fields.error"),
            accessorKey: "Error",
            meta: { relativeWidth: 75 },
        },
    ];
}

export default function QueueSinkErrorDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"QueueSink_ConsumeError" | "QueueSink_ScriptError">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);
    const errors = useMemo(() => details.Errors ?? [], [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={errors}
                columns={columns}
                countKey="table.count.error"
            />
        </AlertDetailsModal>
    );
}
