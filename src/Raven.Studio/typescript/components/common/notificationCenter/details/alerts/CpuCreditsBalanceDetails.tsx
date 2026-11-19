import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

interface PausedIndex {
    indexName: string;
}

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<PausedIndex>[] {
    return [
        {
            header: t("fields.indexName"),
            accessorKey: "indexName",
            meta: { relativeWidth: 100 },
        },
    ];
}

export default function CpuCreditsBalanceDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Throttling_CpuCreditsBalance">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const pausedIndexes = useMemo(() => (details.IndexNames ?? []).map((indexName) => ({ indexName })), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <h4 className="mb-0">{t("cpuCreditsBalance.pausedIndexes")}</h4>
            <NotificationDetailsTable
                notification={notification}
                data={pausedIndexes}
                columns={columns}
                countKey="table.count.index"
            />
        </AlertDetailsModal>
    );
}
