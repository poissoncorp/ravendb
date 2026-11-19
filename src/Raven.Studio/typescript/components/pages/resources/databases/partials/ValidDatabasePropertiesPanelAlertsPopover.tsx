import React from "react";
import { ValidDatabasePropertiesPanelPopoverProps } from "./ValidDatabasePropertiesPanel";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function ValidDatabasePropertiesPanelAlertsPopover({
    isCurrentNodeRelevant,
    localNodeTag,
    localTotal,
    remoteTopLevelStates,
    openNotificationCenter,
    getServerNodeUrl,
}: ValidDatabasePropertiesPanelPopoverProps) {
    const t = useStudioTranslation("databases");

    return (
        <div className="p-3 notifications-popover">
            {isCurrentNodeRelevant && (
                <>
                    <strong className="d-block mb-1">{t("notificationsPopover.local")}</strong>
                    <div className="notifications-popover-grid">
                        <span>
                            <Icon icon="node" color="node" /> {localNodeTag}
                        </span>
                        <span>
                            <StudioTrans
                                ns="databases"
                                i18nKey="notificationsPopover.alertsCount"
                                options={{ count: localTotal }}
                                components={{ strong: <strong /> }}
                            />
                        </span>
                        {localTotal > 0 && (
                            <Button
                                type="button"
                                size="xs"
                                variant="warning"
                                className="rounded-pill"
                                onClick={openNotificationCenter}
                            >
                                <Icon icon="alert" />
                                {t("notificationsPopover.seeAlerts", { count: localTotal })}
                            </Button>
                        )}
                    </div>
                </>
            )}

            {remoteTopLevelStates.some((x) => !!x.alerts) && (
                <>
                    {isCurrentNodeRelevant && <hr className="my-2" />}
                    <strong className="d-block mb-1">{t("notificationsPopover.remote")}</strong>

                    {remoteTopLevelStates.map((x) => {
                        if (!x.alerts) {
                            return null;
                        }

                        return (
                            <div key={x.nodeTag} className="notifications-popover-grid mb-1">
                                <span>
                                    <Icon icon="node" color="node" /> {x.nodeTag}
                                </span>
                                <span>
                                    <StudioTrans
                                        ns="databases"
                                        i18nKey="notificationsPopover.alertsCount"
                                        options={{ count: x.alerts }}
                                        components={{ strong: <strong /> }}
                                    />
                                </span>
                                <a href={getServerNodeUrl(x.nodeTag)} className="no-decor" target="_blank">
                                    <Button type="button" size="xs" variant="node" className="rounded-pill">
                                        <Icon icon="newtab" />
                                        {t("notificationsPopover.openNode")}
                                    </Button>
                                </a>
                            </div>
                        );
                    })}
                </>
            )}
        </div>
    );
}
