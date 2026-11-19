import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellBytes, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type HugeDocumentInfo = Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentInfo;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<HugeDocumentInfo>[] {
    return [
        {
            header: t("hugeDocuments.documentId"),
            accessorKey: "Id",
            meta: { relativeWidth: 40 },
        },
        {
            header: t("hugeDocuments.documentSize"),
            accessorKey: "Size",
            cell: CellBytes,
            size: columnSizes.bytes,
            meta: {
                exportHeader: t("hugeDocuments.documentSizeExport"),
                filterPlaceholder: t("table.sizeFilterPlaceholder"),
            },
        },
        {
            header: t("hugeDocuments.lastAccess"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
    ];
}

export default function HugeDocumentsDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"HugeDocuments">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const documents = useMemo(() => Object.values(details.HugeDocuments ?? {}).reverse(), [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={documents}
                columns={columns}
                countKey="table.count.document"
            />
        </PerformanceHintDetailsModal>
    );
}
