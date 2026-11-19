import React from "react";
import StatTile from "./StatTile";
import NodeTagPill from "./NodeTagPill";
import { formatUpTime, osIcon } from "./analyzerUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type DebugPackageAnalysisSummary = Raven.Server.Documents.Handlers.Debugging.DebugPackage.DebugPackageAnalysisSummary;

interface NodeOverviewProps {
    summary: DebugPackageAnalysisSummary;
    nodeTag: string;
}

export default function NodeOverview({ summary, nodeTag }: NodeOverviewProps) {
    const t = useStudioTranslation("debugPackageAnalyzer");
    const node = summary.SummaryPerNode?.[nodeTag]?.ClusterNodeInfo;

    if (!node) {
        return null;
    }

    return (
        <div className="node-overview">
            <div className="panel-bg-1 rounded">
                <div className="p-4 vstack gap-3">
                    <h3 className="m-0">{t("nodeOverview.title")}</h3>
                    <div className="vstack gap-3">
                        <div>
                            <div className="hstack gap-1 align-items-center mb-1">
                                <StudioTrans
                                    ns="debugPackageAnalyzer"
                                    i18nKey="nodeOverview.nodeHeading"
                                    components={{ pill: <NodeTagPill tag={nodeTag} /> }}
                                />
                            </div>
                            <div className="overview-stats gap-2">
                                <StatTile
                                    label={t("nodeOverview.nodeStatus")}
                                    icon="check"
                                    iconColor="success"
                                    value={t("nodeOverview.online")}
                                />
                                <StatTile
                                    label={t("nodeOverview.state")}
                                    icon={node.NodeState === "Leader" ? "node-leader" : "cluster-member"}
                                    iconColor="node"
                                    value={node.NodeState}
                                />
                                <StatTile
                                    label={t("nodeOverview.type")}
                                    icon={
                                        node.NodeType === "Promotable"
                                            ? "cluster-promotable"
                                            : node.NodeType === "Member"
                                              ? "cluster-member"
                                              : "node"
                                    }
                                    iconColor="node"
                                    value={node.NodeType}
                                />
                                <StatTile
                                    label={t("nodeOverview.os")}
                                    icon={node.OsName ? osIcon(node.OsType) : "server"}
                                    value={node.OsName || "-"}
                                />
                                <StatTile
                                    label={t("nodeOverview.serverVersion")}
                                    icon="server"
                                    value={node.ServerVersion}
                                />
                                <StatTile
                                    label={t("nodeOverview.uptime")}
                                    icon="clock"
                                    value={formatUpTime(node.UpTime)}
                                />
                                <StatTile
                                    label="URL"
                                    icon="link"
                                    value={
                                        <PopoverWithHoverWrapper message={node.NodeUrl} placement="top">
                                            <a
                                                href={node.NodeUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                                className="node-url-link"
                                            >
                                                {node.NodeUrl}
                                            </a>
                                        </PopoverWithHoverWrapper>
                                    }
                                />
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
