import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { columnSizes, dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

type SlowSqlStatementInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo;

function getDataColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<SlowSqlStatementInfo>[] {
    return [
        {
            header: t("fields.durationMs"),
            accessorKey: "Duration",
            size: columnSizes.duration,
        },
        {
            header: t("fields.date"),
            accessorKey: "Date",
            ...dateColumnOptions,
        },
        {
            header: t("slowSql.statement"),
            accessorKey: "Statement",
            meta: { relativeWidth: 65 },
        },
    ];
}

export default function SlowSqlDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"SqlEtl_SlowSql">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);

    const statements = useMemo(() => [...(details.Statements ?? [])].reverse(), [details]);

    const [selectedStatement, setSelectedStatement] = usePreviewItem(statements, getStatementKey);

    const columns = useMemo(
        () => [previewColumn<SlowSqlStatementInfo>(setSelectedStatement), ...getDataColumns(t)],
        [setSelectedStatement, t]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={statements}
                columns={columns}
                countKey="table.count.statement"
                selectedItem={selectedStatement}
                onItemClick={setSelectedStatement}
            />
            {selectedStatement && (
                <NotificationDetailsPreview title={t("table.previewTitle")} item={selectedStatement}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label={t("fields.duration")}>
                            {t("slowSql.durationValue", { duration: selectedStatement.Duration.toLocaleString() })}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label={t("fields.date")}>
                            {genUtils.formatUtcDateAsLocal(selectedStatement.Date)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Code
                        code={selectedStatement.Statement}
                        language="sql"
                        whiteSpace="pre-wrap"
                        className="notification-details-code"
                    />
                </NotificationDetailsPreview>
            )}
        </PerformanceHintDetailsModal>
    );
}

function getStatementKey(statement: SlowSqlStatementInfo): string {
    return `${statement.Date}|${statement.Statement}`;
}
