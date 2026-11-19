import React from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { DestinationUrlItem, OngoingTaskResponsibleNode } from "../../shared/shared";
import {
    ExternalReplicationTaskDistribution,
    getVisibleNodes,
    ReplicationTaskSummaryProgress,
} from "../partials/ExternalReplicationTaskDistribution";
import {
    getSinkConnectionStatus,
    getSinkConnectionStatusLabels,
    getSinkError,
} from "../partials/useReplicationHubSinks";
import { ErrorModal } from "../partials/ErrorModal";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface ReplicationHubConnectedSinkPanelProps {
    data: OngoingTaskReplicationHubInfo;
    isExpanded: boolean;
    toggleExpanded: () => void;
}

export function ReplicationHubConnectedSinkPanel(props: ReplicationHubConnectedSinkPanelProps) {
    const { data, isExpanded, toggleExpanded } = props;
    const t = useStudioTranslation("ongoingTasks");

    const visibleNodes = getVisibleNodes(data);
    const connectionStatus = getSinkConnectionStatus(data);
    const error = getSinkError(data);

    return (
        <RichPanel className="well">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName
                        size="sm"
                        title={t("connectedSink.nameTitle", { database: data.shared.destinationDatabase })}
                    >
                        {data.shared.destinationDatabase}
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="pull-replication-agent" />
                        {t("taskType.replicationSink")}
                    </span>
                    <OngoingTaskResponsibleNode task={data} />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <RichPanelDetailItem>
                    <Button
                        variant="secondary"
                        onClick={toggleExpanded}
                        title={t("connectedSink.toggleDetailsTitle", {
                            context: isExpanded ? "collapse" : "expand",
                            database: data.shared.destinationDatabase,
                        })}
                        className="btn-toggle-panel rounded-pill"
                    >
                        <Icon icon={isExpanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </RichPanelDetailItem>
                <DestinationUrlItem
                    destinationUrl={data.shared.destinationUrl}
                    label={t("connectedSink.actualSinkUrl")}
                />
                {connectionStatus && (
                    <RichPanelDetailItem label={t("connectedSink.replicationStatus")}>
                        {getSinkConnectionStatusLabels(t)[connectionStatus]}
                    </RichPanelDetailItem>
                )}
                {error && <SinkErrorItem error={error} />}
                {visibleNodes.length > 0 && (
                    <RichPanelDetailItem>
                        <ReplicationTaskSummaryProgress task={data} nodesInfo={visibleNodes} />
                    </RichPanelDetailItem>
                )}
            </RichPanelDetails>
            <Collapse in={isExpanded} mountOnEnter>
                <div>
                    <ExternalReplicationTaskDistribution task={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}

function SinkErrorItem({ error }: { error: string }) {
    const t = useStudioTranslation("ongoingTasks");
    const { value: isErrorModalOpen, toggle: toggleErrorModal } = useBoolean(false);

    return (
        <RichPanelDetailItem>
            <Button
                variant="link"
                onClick={toggleErrorModal}
                className="d-flex gap-1 align-items-center p-0"
                title={t("connectedSink.errorDetailsTitle")}
            >
                <Icon icon="warning" color="danger" margin="m-0" />
                <span className="text-danger">{t("error")}</span>
            </Button>
            {isErrorModalOpen && <ErrorModal toggleErrorModal={toggleErrorModal} error={error} />}
        </RichPanelDetailItem>
    );
}
