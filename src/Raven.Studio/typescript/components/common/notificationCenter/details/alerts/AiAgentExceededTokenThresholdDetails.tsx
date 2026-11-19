import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import Alert from "react-bootstrap/Alert";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import type { StudioTranslate } from "common/i18n/resources";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";

type ToolCallDetails =
    Raven.Server.NotificationCenter.Notifications.Details.ExceededTokenThresholdDetails.ToolCallDetails;

function getColumns(t: StudioTranslate<"notificationCenter">): ColumnDef<ToolCallDetails>[] {
    return [
        {
            header: t("aiAgentTokenThreshold.toolName"),
            accessorKey: "Name",
            meta: { relativeWidth: 20 },
        },
        {
            header: t("fields.type"),
            accessorKey: "Type",
            meta: { relativeWidth: 15 },
        },
        {
            header: t("aiAgentTokenThreshold.arguments"),
            accessorKey: "Arguments",
            meta: { relativeWidth: 25 },
        },
        {
            header: t("fields.query"),
            accessorKey: "Query",
            meta: { relativeWidth: 25 },
        },
        {
            header: t("aiAgentTokenThreshold.id"),
            accessorKey: "Id",
            meta: { relativeWidth: 15 },
        },
    ];
}

export default function AiAgentExceededTokenThresholdDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"AiAgent_ExceededTokenThreshold">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);
    const columns = useMemo(() => getColumns(t), [t]);
    const toolCalls = details.ToolCalls ?? [];

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsList>
                <NotificationDetailsListItem label={t("aiAgentTokenThreshold.agentName")}>
                    {details.AgentName}
                </NotificationDetailsListItem>
                <NotificationDetailsListItem label={t("aiAgentTokenThreshold.conversationId")}>
                    {details.ConversationId}
                </NotificationDetailsListItem>
                <NotificationDetailsListItem label={t("aiAgentTokenThreshold.tokenCount")}>
                    {details.TokenCount.toLocaleString()}
                </NotificationDetailsListItem>
                <NotificationDetailsListItem label={t("aiAgentTokenThreshold.tokenThreshold")}>
                    {details.TokenThreshold.toLocaleString()}
                </NotificationDetailsListItem>
            </NotificationDetailsList>
            {details.Recommendation && (
                <Alert variant="info" className="mb-0">
                    <strong>{t("aiAgentTokenThreshold.recommendation")}</strong>
                    <div>{details.Recommendation}</div>
                </Alert>
            )}
            {toolCalls.length > 0 && (
                <NotificationDetailsTable
                    notification={notification}
                    data={toolCalls}
                    columns={columns}
                    countKey="table.count.toolCall"
                />
            )}
        </AlertDetailsModal>
    );
}
