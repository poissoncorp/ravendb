import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import Alert from "react-bootstrap/Alert";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type WarningDetails = Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument.WarningDetails;

interface IndexingWarning extends WarningDetails {
    IndexName: string;
}

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<IndexingWarning>[] {
    return [
        {
            header: t("fields.indexName"),
            accessorKey: "IndexName",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("indexing.exceedingDocuments"),
            accessorKey: "NumberOfExceedingDocuments",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("indexing.sampleDocumentId"),
            accessorKey: "SampleDocumentId",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("indexing.outputsPerDocument"),
            accessorKey: "MaxNumberOutputsPerDocument",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("fields.date"),
            accessorKey: "LastWarningTime",
            ...dateColumnOptions,
        },
    ];
}

export default function IndexingDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"Indexing">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);

    const warnings = useMemo(
        () =>
            Object.entries(details.Warnings ?? {})
                .flatMap(([indexName, indexWarnings]) => indexWarnings.map((x) => ({ ...x, IndexName: indexName })))
                .reverse(),
        [details]
    );

    const suggestions = useMemo(() => [...new Set(warnings.map((x) => x.Suggestion))].filter(Boolean), [warnings]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                countKey="table.count.warning"
            />
            {suggestions.length > 0 && (
                <Alert variant="info" className="mb-0">
                    <strong>{t("indexing.suggestions", { count: suggestions.length })}</strong>
                    {suggestions.map((suggestion) => (
                        <div key={suggestion}>{suggestion}</div>
                    ))}
                </Alert>
            )}
        </PerformanceHintDetailsModal>
    );
}
