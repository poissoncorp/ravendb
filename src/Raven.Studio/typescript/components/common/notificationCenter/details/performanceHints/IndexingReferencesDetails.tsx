import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import Table from "react-bootstrap/Table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

type LoadedReference =
    Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning.LoadedReference;

interface IndexReferencesWarning {
    indexName: string;
    lastWarningTime: string;
    loadedReferences: LoadedReference[];
}

function getDataColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<IndexReferencesWarning>[] {
    return [
        {
            header: t("fields.indexName"),
            accessorKey: "indexName",
            meta: { relativeWidth: 60 },
        },
        {
            header: t("indexingReferences.lastWarningDate"),
            accessorKey: "lastWarningTime",
            ...dateColumnOptions,
        },
    ];
}

export default function IndexingReferencesDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"Indexing_References">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);

    const warnings = useMemo(
        () =>
            Object.entries(details.Warnings ?? {})
                .map(([indexName, warning]) => ({
                    indexName,
                    lastWarningTime: warning.LastWarningTime,
                    loadedReferences: Object.values(warning.Top10LoadedReferences ?? {}),
                }))
                .reverse(),
        [details]
    );

    const [selectedWarning, setSelectedWarning] = usePreviewItem(warnings, getWarningKey);

    const columns = useMemo(
        () => [previewColumn<IndexReferencesWarning>(setSelectedWarning), ...getDataColumns(t)],
        [setSelectedWarning, t]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                countKey="table.count.index"
                selectedItem={selectedWarning}
                onItemClick={setSelectedWarning}
            />
            {selectedWarning && (
                <NotificationDetailsPreview title={t("indexingReferences.previewTitle")} item={selectedWarning}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label={t("fields.indexName")}>
                            {selectedWarning.indexName}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label={t("indexingReferences.lastWarningTime")}>
                            {genUtils.formatUtcDateAsLocal(selectedWarning.lastWarningTime)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Table striped size="sm" className="mb-0">
                        <thead>
                            <tr>
                                <th>{t("fields.referenceId")}</th>
                                <th>{t("indexingReferences.numberOfLoads")}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {selectedWarning.loadedReferences.map((reference, index) => (
                                <tr key={index}>
                                    <td>{reference.ReferenceId}</td>
                                    <td>{reference.NumberOfLoads.toLocaleString()}</td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </NotificationDetailsPreview>
            )}
        </PerformanceHintDetailsModal>
    );
}

function getWarningKey(warning: IndexReferencesWarning): string {
    return warning.indexName;
}
