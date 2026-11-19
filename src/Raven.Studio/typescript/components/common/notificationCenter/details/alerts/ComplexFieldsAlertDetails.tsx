import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

interface ComplexFieldsItem {
    indexName: string;
    complexFields: string;
}

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<ComplexFieldsItem>[] {
    return [
        {
            header: t("fields.indexName"),
            accessorKey: "indexName",
            meta: { relativeWidth: 30 },
        },
        {
            header: t("complexFields.fields"),
            accessorKey: "complexFields",
            meta: { relativeWidth: 70 },
        },
    ];
}

export default function ComplexFieldsAlertDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Indexing_CoraxComplexItem">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const items = useMemo(
        () =>
            Object.entries(details.Fields ?? {}).map(([indexName, fields]) => ({
                indexName,
                complexFields: fields.join(", "),
            })),
        [details]
    );

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={items}
                columns={columns}
                countKey="table.count.index"
            />
        </AlertDetailsModal>
    );
}
