import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellWithFormattedValue, dateColumnOptions } from "../NotificationDetailsCells";

type SlowWriteInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowIoDetails.SlowWriteInfo;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<SlowWriteInfo>[] {
    return [
        {
            header: t("slowIo.path"),
            accessorKey: "Path",
            meta: { relativeWidth: 35 },
        },
        {
            header: t("fields.type"),
            accessorKey: "Type",
            meta: { relativeWidth: 15 },
        },
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            header: t("slowIo.speed"),
            accessorKey: "SpeedInMbPerSec",
            cell: CellSpeed,
            meta: { relativeWidth: 30, exportHeader: t("slowIo.speedExport") },
        },
    ];
}

export default function SlowIoDetails({ notification, actions }: NotificationDetailsProps<performanceHint<"SlowIO">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const writes = useMemo(() => Object.values(details.Writes ?? {}).reverse(), [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={writes}
                columns={columns}
                countKey="table.count.write"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellSpeed({ row }: Pick<CellContext<SlowWriteInfo, unknown>, "row">) {
    const t = useStudioTranslation("notificationCenter");
    const { DataWrittenInMb, DurationInSec, SpeedInMbPerSec } = row.original;
    const formattedSpeed = t("slowIo.speedValue", {
        dataWritten: DataWrittenInMb.toFixed(2),
        duration: DurationInSec.toFixed(2),
        speed: SpeedInMbPerSec.toFixed(2),
    });

    return <CellWithFormattedValue value={SpeedInMbPerSec} formattedValue={formattedSpeed} />;
}
