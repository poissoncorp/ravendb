import moment from "moment";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";

export default function EtlTaskHealthChangeDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Etl_HealthStatusChange">>) {
    const t = useStudioTranslation("notificationCenter");
    const details = useKnockoutValue(notification.details);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            {details.PreviousHealthStatus && (
                <NotificationDetailsList>
                    <NotificationDetailsListItem label={t("etlTaskHealthChange.previousHealthStatus")}>
                        {details.PreviousHealthStatus}
                    </NotificationDetailsListItem>
                    {details.PreviousHealthStatusChangeAt && (
                        <NotificationDetailsListItem label={t("etlTaskHealthChange.previousHealthStatusChangeAt")}>
                            {moment.utc(details.PreviousHealthStatusChangeAt).local().format("LLL")}
                        </NotificationDetailsListItem>
                    )}
                </NotificationDetailsList>
            )}
        </AlertDetailsModal>
    );
}
