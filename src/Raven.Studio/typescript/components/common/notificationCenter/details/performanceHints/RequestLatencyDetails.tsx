import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellWithFormattedValue, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type RequestLatencyInfo = Raven.Server.NotificationCenter.Notifications.Details.RequestLatencyInfo;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<RequestLatencyInfo>[] {
    return [
        {
            header: t("fields.action"),
            accessorKey: "Action",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            header: t("fields.duration"),
            accessorKey: "Duration",
            cell: CellDuration,
            size: columnSizes.duration,
            meta: {
                exportHeader: t("fields.durationMs"),
                filterPlaceholder: t("requestLatency.durationFilterPlaceholder"),
            },
        },
        {
            header: t("fields.query"),
            accessorKey: "Query",
            meta: { relativeWidth: 45 },
        },
    ];
}

export default function RequestLatencyDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"RequestLatency">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const requests = useMemo(() => {
        return Object.entries(details.RequestLatencies ?? {})
            .flatMap(([action, infos]) => infos.map((x) => ({ ...x, Action: action })))
            .reverse();
    }, [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={requests}
                columns={columns}
                countKey="table.count.request"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellDuration({ getValue }: Pick<CellContext<RequestLatencyInfo, unknown>, "getValue">) {
    const durationInMs = getValue() as number;
    return <CellWithFormattedValue value={durationInMs} formattedValue={genUtils.formatTimeSpan(durationInMs, true)} />;
}
