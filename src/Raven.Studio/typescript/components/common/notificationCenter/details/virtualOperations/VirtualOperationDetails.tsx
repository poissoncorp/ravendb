import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import groupedVirtualNotification from "common/notifications/models/groupedVirtualNotification";
import virtualBulkInsert from "common/notifications/models/virtualBulkInsert";
import virtualBulkInsertFailures from "common/notifications/models/virtualBulkInsertFailures";
import virtualDeleteByQuery from "common/notifications/models/virtualDeleteByQuery";
import virtualDeleteByQueryFailures from "common/notifications/models/virtualDeleteByQueryFailures";
import virtualUpdateByQuery from "common/notifications/models/virtualUpdateByQuery";
import virtualUpdateByQueryFailures from "common/notifications/models/virtualUpdateByQueryFailures";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { NotificationDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CloseDetailsFooter } from "../NotificationDetailsFooters";
import { columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type OperationItem = { id: string; operationId?: number };

interface VirtualOperationDetailsProps<T extends OperationItem>
    extends NotificationDetailsProps<groupedVirtualNotification<T>> {
    title: string;
    columns: ColumnDef<T>[];
}

function VirtualOperationDetails<T extends OperationItem>({
    notification,
    actions,
    title,
    columns,
}: VirtualOperationDetailsProps<T>) {
    const operations = useKnockoutValue(notification.operations);

    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            title={title}
            isMessageHidden
            size="xxl"
            footer={<CloseDetailsFooter actions={actions} />}
        >
            <NotificationDetailsTable
                notification={notification}
                data={operations}
                columns={columns}
                countKey="table.count.operation"
                exportTitle={title}
            />
        </NotificationDetailsModal>
    );
}

function dateColumn<T extends { date: string }>(t: StudioTranslate<"notificationCenter">): ColumnDef<T> {
    return {
        id: "date",
        header: t("fields.date"),
        accessorFn: (x) => x.date,
        ...dateColumnOptions,
    };
}

function durationColumn<T extends { duration: number }>(t: StudioTranslate<"notificationCenter">): ColumnDef<T> {
    return {
        id: "duration",
        header: t("fields.durationMs"),
        accessorFn: (x) => x.duration,
        size: columnSizes.duration,
    };
}

function getBulkInsertColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<virtualBulkOperationItem>[] {
    return [
        dateColumn(t),
        durationColumn(t),
        { header: t("virtualOperations.documents"), accessorKey: "documentsProcessed", meta: { relativeWidth: 17 } },
        {
            header: t("virtualOperations.attachments"),
            accessorKey: "attachmentsProcessed",
            meta: { relativeWidth: 17 },
        },
        { header: t("virtualOperations.counters"), accessorKey: "countersProcessed", meta: { relativeWidth: 17 } },
        { header: t("virtualOperations.timeSeries"), accessorKey: "timeSeriesProcessed", meta: { relativeWidth: 17 } },
    ];
}

function getBulkInsertFailureColumns(
    t: StudioTranslate<"notificationCenter">
): ColumnDef<virtualBulkOperationFailureItem>[] {
    return [
        dateColumn(t),
        durationColumn(t),
        { header: t("fields.errorMessage"), accessorKey: "errorMsg", meta: { relativeWidth: 25 } },
        { header: t("fields.error"), accessorKey: "error", meta: { relativeWidth: 43 } },
    ];
}

function getQueryBasedColumns(
    t: StudioTranslate<"notificationCenter">
): ColumnDef<queryBasedVirtualBulkOperationItem>[] {
    return [
        dateColumn(t),
        durationColumn(t),
        {
            header: t("virtualOperations.processedDocuments"),
            accessorKey: "totalItemsProcessed",
            meta: { relativeWidth: 15 },
        },
        {
            header: t("virtualOperations.collectionOrIndex"),
            accessorKey: "indexOrCollectionUsed",
            meta: { relativeWidth: 20 },
        },
        { header: t("fields.query"), accessorKey: "query", meta: { relativeWidth: 33 } },
    ];
}

function getQueryBasedFailureColumns(
    t: StudioTranslate<"notificationCenter">
): ColumnDef<queryBasedVirtualBulkOperationFailureItem>[] {
    return [
        dateColumn(t),
        durationColumn(t),
        { header: t("fields.query"), accessorKey: "query", meta: { relativeWidth: 25 } },
        { header: t("fields.errorMessage"), accessorKey: "errorMsg", meta: { relativeWidth: 18 } },
        { header: t("fields.error"), accessorKey: "error", meta: { relativeWidth: 25 } },
    ];
}

export function VirtualBulkInsertDetails(props: NotificationDetailsProps<virtualBulkInsert>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getBulkInsertColumns(t), [t]);

    return <VirtualOperationDetails {...props} title={t("virtualOperations.bulkInsertTitle")} columns={columns} />;
}

export function VirtualBulkInsertFailuresDetails(props: NotificationDetailsProps<virtualBulkInsertFailures>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getBulkInsertFailureColumns(t), [t]);

    return (
        <VirtualOperationDetails {...props} title={t("virtualOperations.bulkInsertFailuresTitle")} columns={columns} />
    );
}

export function VirtualUpdateByQueryDetails(props: NotificationDetailsProps<virtualUpdateByQuery>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getQueryBasedColumns(t), [t]);

    return <VirtualOperationDetails {...props} title={t("virtualOperations.updateByQueryTitle")} columns={columns} />;
}

export function VirtualUpdateByQueryFailuresDetails(props: NotificationDetailsProps<virtualUpdateByQueryFailures>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getQueryBasedFailureColumns(t), [t]);

    return (
        <VirtualOperationDetails
            {...props}
            title={t("virtualOperations.updateByQueryFailuresTitle")}
            columns={columns}
        />
    );
}

export function VirtualDeleteByQueryDetails(props: NotificationDetailsProps<virtualDeleteByQuery>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getQueryBasedColumns(t), [t]);

    return <VirtualOperationDetails {...props} title={t("virtualOperations.deleteByQueryTitle")} columns={columns} />;
}

export function VirtualDeleteByQueryFailuresDetails(props: NotificationDetailsProps<virtualDeleteByQueryFailures>) {
    const t = useStudioTranslation("notificationCenter");
    const columns = useMemo(() => getQueryBasedFailureColumns(t), [t]);

    return (
        <VirtualOperationDetails
            {...props}
            title={t("virtualOperations.deleteByQueryFailuresTitle")}
            columns={columns}
        />
    );
}
