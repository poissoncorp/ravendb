import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

type WarningDetails =
    Raven.Server.NotificationCenter.Notifications.Details.MismatchedReferencesLoadWarning.WarningDetails;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<WarningDetails>[] {
    return [
        {
            header: t("mismatchedReferenceLoad.sourceId"),
            accessorKey: "SourceId",
            meta: { relativeWidth: 25 },
        },
        {
            header: t("fields.referenceId"),
            accessorKey: "ReferenceId",
            meta: { relativeWidth: 25 },
        },
        {
            header: t("mismatchedReferenceLoad.actualCollection"),
            accessorKey: "ActualCollection",
            meta: { relativeWidth: 25 },
        },
        {
            id: "MismatchedCollections",
            header: t("mismatchedReferenceLoad.mismatchedCollections"),
            accessorFn: (x) => x.MismatchedCollections.join(", "),
            meta: { relativeWidth: 25 },
        },
    ];
}

export default function MismatchedReferenceLoadDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"MismatchedReferenceLoad">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const warnings = useMemo(() => Object.values(details.Warnings ?? {}).flat(), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                countKey="table.count.reference"
            />
        </AlertDetailsModal>
    );
}
