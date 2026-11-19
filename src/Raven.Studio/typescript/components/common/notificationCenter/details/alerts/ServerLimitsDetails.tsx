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

type ServerLimitInfo = Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails.ServerLimitInfo;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<ServerLimitInfo>[] {
    return [
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            header: t("serverLimits.name"),
            accessorKey: "Name",
            meta: { relativeWidth: 25 },
        },
        {
            header: t("serverLimits.limit"),
            accessorKey: "Limit",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("serverLimits.value"),
            accessorKey: "Value",
            meta: { relativeWidth: 15 },
        },
        {
            header: t("serverLimits.max"),
            accessorKey: "Max",
            meta: { relativeWidth: 15 },
        },
    ];
}

export default function ServerLimitsDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"ServerLimits">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);
    const limits = useMemo(() => details.Limits ?? [], [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={limits}
                columns={columns}
                countKey="table.count.limit"
            />
        </AlertDetailsModal>
    );
}
