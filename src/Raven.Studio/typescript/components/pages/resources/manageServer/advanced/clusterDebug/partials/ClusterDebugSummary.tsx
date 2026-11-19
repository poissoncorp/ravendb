import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import "./ClusterDebugSummary.scss";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import endpoints from "endpoints";
import { LazyLoad } from "components/common/LazyLoad";
import assertUnreachable from "components/utils/assertUnreachable";
import { ClusterDebugNodeInfo } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import genUtils from "common/generalUtils";
import moment from "moment";
import notificationCenter from "common/notifications/notificationCenter";
import messagePublisher from "common/messagePublisher";
import useDialog from "components/common/Dialog";
import Code from "components/common/Code";
import ClusterSnapshotInstallation from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterSnapshotInstallation";
import SizeGetter from "components/common/SizeGetter";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import ProgressBar from "react-bootstrap/ProgressBar";
import { EmptySet } from "components/common/EmptySet";
import { nodeAwareLoadableData } from "components/models/common";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface ClusterDebugSummaryProps {
    nodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[];
}

function jsonUrl(serverUrl: string) {
    return serverUrl + endpoints.global.rachisAdmin.adminClusterLog;
}

export default function ClusterDebugSummary(props: ClusterDebugSummaryProps) {
    const { nodes } = props;
    const t = useStudioTranslation("clusterDebug");

    const localNode = useAppSelector(clusterSelectors.localNode);
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const dialog = useDialog();
    const hasAnyCriticalError = nodes.some((x) => !!x.data?.criticalError);

    const openInstallationDetails = async (nodeTag: string) => {
        const nodeData = nodes.find((x) => x.nodeTag === nodeTag);

        if (nodeData.data.installingSnapshot) {
            await dialog({
                title: t("summary.snapshotDialogTitle", { nodeTag }),
                modalSize: "lg",
                message: (
                    <SizeGetter
                        render={(size) => (
                            <ClusterSnapshotInstallation
                                availableWidth={size.width}
                                messages={nodeData.data.installationLog}
                            />
                        )}
                    />
                ),
            });
        }
    };

    const showConnectionDetails = async (connection: Raven.Server.Rachis.RaftDebugView.PeerConnection) => {
        const jsonString = JSON.stringify(connection, null, 4);
        await dialog({
            title: t("summary.connectionDialogTitle"),
            message: <Code code={jsonString} language="json" />,
            modalSize: "lg",
        });
    };

    const openCriticalError = (nodeTag: string) => {
        if (nodeTag !== localNode.nodeTag) {
            messagePublisher.reportInfo(t("summary.goToNode", { nodeTag }));
            return;
        }

        const nodeData = nodes.find((x) => x.nodeTag === nodeTag);

        const alertId = nodeData.data.criticalError.Id;
        const criticalErrorAlert = notificationCenter.instance.globalNotifications().find((x) => x.id === alertId);
        if (!criticalErrorAlert) {
            messagePublisher.reportError(t("summary.criticalErrorNotFound"));
        }

        notificationCenter.instance.openDetails(criticalErrorAlert);
    };

    return (
        <React.Fragment key="summary">
            <Table bordered responsive className="mb-1 rounded-1 overflow-hidden cluster-debug-summary">
                <thead>
                    <tr>
                        <th></th>
                        {nodes.map((node) => {
                            const nodeInfo = allNodes.find((x) => x.nodeTag === node.nodeTag);
                            return (
                                <th key={node.nodeTag}>
                                    <div className="d-flex gap-1 align-items-center">
                                        <span>
                                            <Icon
                                                icon={node.data?.role === "Leader" ? "node-leader" : "cluster-member"}
                                                color="node"
                                            />
                                            <span className="text-nowrap">{t("node", { nodeTag: node.nodeTag })}</span>
                                        </span>
                                        {localNode?.nodeTag === node.nodeTag && (
                                            <Badge bg="node" pill>
                                                {t("currentNode")}
                                            </Badge>
                                        )}
                                        <FlexGrow />
                                        <a
                                            target="_blank"
                                            href={jsonUrl(nodeInfo?.serverUrl)}
                                            title={t("summary.rawJsonTitle")}
                                            className="no-decor"
                                        >
                                            <Icon icon="json" margin="m-0" />
                                        </a>
                                    </div>
                                </th>
                            );
                        })}
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <th>
                            {t("summary.rolePhase")}
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <ul>
                                            <li>
                                                <StudioTrans
                                                    ns="clusterDebug"
                                                    i18nKey="summary.rolePhaseInfo.role"
                                                    components={{ br: <br />, i: <i /> }}
                                                />
                                            </li>
                                            <li>
                                                <StudioTrans
                                                    ns="clusterDebug"
                                                    i18nKey="summary.rolePhaseInfo.phase"
                                                    components={{ br: <br />, i: <i /> }}
                                                />
                                            </li>
                                        </ul>
                                    </>
                                }
                            >
                                <Icon id="ReplicationInfo" icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            if (node.status === "failure") {
                                return (
                                    <td
                                        rowSpan={hasAnyCriticalError ? 9 : 8}
                                        className="align-content-center"
                                        key={node.nodeTag}
                                    >
                                        <EmptySet color="danger" icon="warning">
                                            <span>{t("summary.unableToConnect")}</span>
                                            <br />
                                            <small className="text-muted">
                                                <StudioTrans
                                                    ns="clusterDebug"
                                                    i18nKey="summary.connectionIssue"
                                                    components={{
                                                        icon: <Icon icon="node" addon="warning" margin="ms-1 me-1" />,
                                                    }}
                                                    options={{ nodeTag: node.nodeTag }}
                                                />
                                            </small>
                                        </EmptySet>
                                    </td>
                                );
                            }
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => {
                                        if (node.data.installingSnapshot) {
                                            return (
                                                <div className="d-flex">
                                                    {t("summary.installingSnapshot", { role: node.data.role })}{" "}
                                                    <small className="ms-1">
                                                        <span className="global-spinner spinner-sm"></span>
                                                    </small>
                                                    <FlexGrow />
                                                    <Button
                                                        variant="secondary"
                                                        size="xs"
                                                        onClick={() => openInstallationDetails(node.nodeTag)}
                                                    >
                                                        <Icon icon="preview" />
                                                        {t("summary.viewDetails")}
                                                    </Button>
                                                </div>
                                            );
                                        } else {
                                            return <>{node.data.role}</>;
                                        }
                                    }}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.progress")}
                            <PopoverWithHoverWrapper message={t("summary.progressInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            inline={false}
                                            targetStyle={{ width: "100%" }}
                                            message={
                                                <>
                                                    <StudioTrans
                                                        ns="clusterDebug"
                                                        i18nKey="summary.firstEntryIndex"
                                                        components={{ strong: <strong /> }}
                                                        options={{ index: node.data.firstEntryIndex.toLocaleString() }}
                                                    />
                                                    <br />
                                                    <StudioTrans
                                                        ns="clusterDebug"
                                                        i18nKey="summary.lastCommitIndexDetail"
                                                        components={{ strong: <strong /> }}
                                                        options={{ index: node.data.commitIndex.toLocaleString() }}
                                                    />
                                                    <br />
                                                    <StudioTrans
                                                        ns="clusterDebug"
                                                        i18nKey="summary.lastLogEntryIndex"
                                                        components={{ strong: <strong /> }}
                                                        options={{
                                                            index: node.data.lastLogEntryIndex.toLocaleString(),
                                                        }}
                                                    />
                                                    <br />
                                                </>
                                            }
                                        >
                                            <ProgressBar
                                                variant={node.data.progress === 100 ? "success" : "progress"}
                                                striped={node.data.progress < 100}
                                                now={node.data.progress}
                                                animated={node.data.progress < 100}
                                                label={`${node.data.progress}%`}
                                            />
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.queueSize")}
                            <PopoverWithHoverWrapper message={t("summary.queueSizeInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => <>{node.data.queueSize.toLocaleString()}</>}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.lastCommitIndex")}
                            <PopoverWithHoverWrapper message={t("summary.lastCommitIndexInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender
                                    node={node}
                                    key={node.nodeTag}
                                    tdClassName={classNames(node.data?.chocked && "bg-faded-warning text-warning")}
                                >
                                    {() => (
                                        <>
                                            {node.data.commitIndex.toLocaleString()}
                                            {node.data.chocked && (
                                                <PopoverWithHoverWrapper
                                                    message={
                                                        <>
                                                            <span className="text-warning">
                                                                <Icon icon="warning" />
                                                                {t("summary.warning")}
                                                            </span>
                                                            <span> {t("summary.noCommits")}</span>
                                                        </>
                                                    }
                                                >
                                                    <Icon icon="warning" margin="ms-1" />
                                                </PopoverWithHoverWrapper>
                                            )}
                                        </>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.lastCommittedDate")}{" "}
                            <PopoverWithHoverWrapper message={t("summary.lastCommittedDateInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            const lastCommitedAsAgo = node.data?.lastCommitedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastCommitedTime), true)
                                : null;
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            message={lastCommitedAsAgo ? <> {node.data.lastCommitedTime}</> : null}
                                        >
                                            <div>{lastCommitedAsAgo ?? t("summary.notAvailable")}</div>
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.lastAppendDate")}
                            <PopoverWithHoverWrapper message={t("summary.lastAppendDateInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            const lastAppendedAsAgo = node.data?.lastAppendedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastAppendedTime), true)
                                : null;
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            message={lastAppendedAsAgo ? node.data.lastAppendedTime : null}
                                        >
                                            <div id={"last-appended-" + node.nodeTag}>
                                                {lastAppendedAsAgo ?? t("summary.notAvailable")}
                                            </div>
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.localVersion")}
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <ul>
                                            <li>{t("summary.localVersionInfo.commandId")}</li>
                                            <li>{t("summary.localVersionInfo.highestVersion")}</li>
                                        </ul>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => (
                            <ConditionalRender node={node} key={node.nodeTag}>
                                {() => <>{node.data.localVersion}</>}
                            </ConditionalRender>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            {t("summary.connection")}
                            <PopoverWithHoverWrapper message={t("summary.connectionInfo")}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <div>
                                            {node.data.connections.map((connection) => (
                                                <Button
                                                    title={t("summary.connectionDetailsTitle")}
                                                    size="xs"
                                                    key={connection.Destination}
                                                    onClick={() => showConnectionDetails(connection)}
                                                    variant={connection.Connected ? "success" : "danger"}
                                                    className="me-1 rounded-pill"
                                                >
                                                    <Icon icon={connection.Connected ? "connected" : "disconnected"} />
                                                    {connection.Destination}
                                                </Button>
                                            ))}
                                        </div>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    {hasAnyCriticalError && (
                        <tr>
                            <th>{t("summary.criticalError")}</th>
                            {nodes.map((node) => {
                                return (
                                    <ConditionalRender node={node} key={node.nodeTag}>
                                        {() => (
                                            <div>
                                                {node.data.criticalError ? (
                                                    <Button
                                                        size="xs"
                                                        variant="danger"
                                                        onClick={() => openCriticalError(node.nodeTag)}
                                                    >
                                                        <Icon icon="preview" />
                                                        {t("summary.viewError")}
                                                    </Button>
                                                ) : (
                                                    <>-</>
                                                )}
                                            </div>
                                        )}
                                    </ConditionalRender>
                                );
                            })}
                        </tr>
                    )}
                </tbody>
            </Table>
        </React.Fragment>
    );
}

interface ConditionalRenderProps {
    node: nodeAwareLoadableData<ClusterDebugNodeInfo>;
    children: () => React.ReactNode;
    tdClassName?: string;
}

function ConditionalRender(props: ConditionalRenderProps) {
    const { node, children, tdClassName } = props;
    const status = node.status;
    switch (status) {
        case "loading":
            return (
                <td className={tdClassName}>
                    <LazyLoad active={true}>
                        <div>&nbsp;</div>
                    </LazyLoad>
                </td>
            );
        case "idle":
        case "failure":
            return null;
        case "success":
            return <td className={classNames("align-content-center", tdClassName)}>{children()}</td>;
        default:
            assertUnreachable(status);
    }
}
