import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { RemoteAttachmentErrorInfo } from "common/notifications/models/notificationDetailsByReason";
import genUtils from "common/generalUtils";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

function getDataColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<RemoteAttachmentErrorInfo>[] {
    return [
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            id: "Ids",
            header: t("remoteAttachmentError.ids"),
            accessorFn: (x) => x.Ids?.join(", "),
            meta: { relativeWidth: 15 },
        },
        {
            header: t("remoteAttachmentError.identifier"),
            accessorKey: "Identifier",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("fields.error"),
            accessorKey: "Error",
            meta: { relativeWidth: 50 },
        },
    ];
}

export default function RemoteAttachmentErrorDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Attachments_RemoteAttachmentErroredIdentifier">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);

    const errors = useMemo(() => [...(details.Errors ?? [])].reverse(), [details]);

    const [selectedError, setSelectedError] = usePreviewItem(errors, getErrorKey);

    const columns = useMemo(
        () => [previewColumn<RemoteAttachmentErrorInfo>(setSelectedError), ...getDataColumns(t)],
        [setSelectedError, t]
    );

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={errors}
                columns={columns}
                countKey="table.count.error"
                selectedItem={selectedError}
                onItemClick={setSelectedError}
            />
            {selectedError && (
                <NotificationDetailsPreview title={t("table.previewTitle")} item={selectedError}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label={t("remoteAttachmentError.identifier")}>
                            {selectedError.Identifier}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label={t("remoteAttachmentError.hash")}>
                            {selectedError.Hash}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label={t("remoteAttachmentError.documentIds")}>
                            {selectedError.Ids?.length ? selectedError.Ids.join(", ") : "-"}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label={t("fields.date")}>
                            {genUtils.formatUtcDateAsLocal(selectedError.Date)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Code
                        code={selectedError.Error}
                        language="plaintext"
                        whiteSpace="pre-wrap"
                        className="notification-details-code"
                    />
                </NotificationDetailsPreview>
            )}
        </AlertDetailsModal>
    );
}

function getErrorKey(error: RemoteAttachmentErrorInfo): string {
    return `${error.Date}|${error.Identifier}|${error.Hash}`;
}
