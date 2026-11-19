import React, { useCallback } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { Icon } from "components/common/Icon";
import { OngoingInternalReplicationNodeInfo } from "components/models/tasks";
import useBoolean from "hooks/useBoolean";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { InternalReplicationTaskDistribution } from "components/pages/database/tasks/ongoingTasks/partials/InternalReplicationTaskDistribution";

type InternalReplicationPanelProps = {
    data: OngoingInternalReplicationNodeInfo[];
    onToggleDetails?: (newState: boolean) => void;
};

export function InternalReplicationPanel(props: InternalReplicationPanelProps) {
    const { data, onToggleDetails } = props;
    const t = useStudioTranslation("ongoingTasks");

    const { value: detailsVisible, toggle: toggleDetailsVisible } = useBoolean(false);

    const toggleDetails = useCallback(() => {
        toggleDetailsVisible();
        onToggleDetails?.(!detailsVisible);
    }, [onToggleDetails, toggleDetailsVisible, detailsVisible]);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName>{t("taskType.internalReplication")}</RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <div className="actions">
                        <ButtonGroup>
                            <Button variant="secondary" onClick={toggleDetails} title={t("taskDetails.toggleTitle")}>
                                <Icon icon={detailsVisible ? "fold" : "unfold"} margin="m-0" />
                            </Button>
                        </ButtonGroup>
                    </div>
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse in={detailsVisible}>
                <div>
                    <InternalReplicationTaskDistribution data={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
