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
import { CellBytes, CellDetailsValue, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type ActionDetails = Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails.ActionDetails;

interface PagingItem extends ActionDetails {
    Action: string;
}

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<PagingItem>[] {
    return [
        {
            header: t("fields.action"),
            accessorKey: "Action",
            meta: { relativeWidth: 14 },
        },
        {
            header: t("paging.numberOfResults"),
            accessorKey: "NumberOfResults",
            meta: { relativeWidth: 14 },
        },
        {
            id: "PageSize",
            header: t("paging.pageSize"),
            accessorFn: (x) => (x.PageSize === genUtils.integerMaxValue ? t("paging.pageSizeNotSet") : x.PageSize),
            cell: CellPageSize,
            sortingFn: "alphanumeric",
            meta: { relativeWidth: 12 },
        },
        {
            header: t("paging.docsSize"),
            accessorKey: "TotalDocumentsSizeInBytes",
            cell: CellBytes,
            size: columnSizes.bytes,
            meta: {
                exportHeader: t("paging.docsSizeExport"),
                filterPlaceholder: t("table.sizeFilterPlaceholder"),
            },
        },
        {
            header: t("fields.date"),
            accessorKey: "Occurrence",
            ...dateColumnOptions,
        },
        {
            header: t("fields.durationMs"),
            accessorKey: "Duration",
            size: columnSizes.duration,
        },
        {
            header: t("paging.details"),
            accessorKey: "Details",
            meta: { relativeWidth: 29 },
        },
    ];
}

export default function PagingDetails({ notification, actions }: NotificationDetailsProps<performanceHint<"Paging">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const items = useMemo(
        () =>
            Object.entries(details.Actions ?? {})
                .flatMap(([action, actionDetails]) => actionDetails.map((x) => ({ ...x, Action: action })))
                .reverse(),
        [details]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={items}
                columns={columns}
                countKey="table.count.request"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellPageSize({ getValue }: Pick<CellContext<PagingItem, unknown>, "getValue">) {
    const t = useStudioTranslation("notificationCenter");
    const pageSizeNotSetText = t("paging.pageSizeNotSet");

    if (getValue() === pageSizeNotSetText) {
        return <span>{pageSizeNotSetText}</span>;
    }

    return <CellDetailsValue getValue={getValue} />;
}
