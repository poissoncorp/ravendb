import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import { NodeSet, NodeSetItem, NodeSetLabel } from "components/common/NodeSet";
import React from "react";
import DatabaseUtils from "components/utils/DatabaseUtils";
import DatabaseTopologyNodeSetItem from "./DatabaseTopologyNodeSetItem";
import { locationAwareLoadableData } from "components/models/common";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseTopologyProps {
    db: DatabaseSharedInfo;
    localInfos: locationAwareLoadableData<DatabaseLocalInfo>[];
    togglePanelCollapsed: () => void;
}

export function DatabaseTopology(props: DatabaseTopologyProps) {
    const { db, localInfos, togglePanelCollapsed } = props;
    const t = useStudioTranslation("databases");

    if (db.isSharded) {
        return (
            <div>
                <NodeSet
                    color="orchestrator"
                    className="m-1 cursor-pointer"
                    onClick={togglePanelCollapsed}
                    title={t("properties.expandDetailsTitle")}
                >
                    <NodeSetLabel color="orchestrator" icon="orchestrator">
                        {t("topology.orchestrators")}
                    </NodeSetLabel>
                    {db.nodes.map((node) => (
                        <DatabaseTopologyNodeSetItem
                            key={node.tag}
                            node={node}
                            localInfos={localInfos}
                            isDisabled={db.isDisabled}
                        />
                    ))}
                </NodeSet>

                {db.shards.map((shard) => {
                    const shardNumber = DatabaseUtils.shardNumber(shard.name);
                    return (
                        <React.Fragment key={shard.name}>
                            <NodeSet
                                color="shard"
                                className="m-1 cursor-pointer"
                                onClick={togglePanelCollapsed}
                                title={t("properties.expandDetailsTitle")}
                            >
                                <NodeSetLabel color="shard" icon="shard">
                                    #{shardNumber}
                                </NodeSetLabel>
                                {shard.nodes.map((node) => (
                                    <DatabaseTopologyNodeSetItem
                                        key={node.tag}
                                        node={node}
                                        localInfos={localInfos}
                                        shardNumber={shardNumber}
                                        isDisabled={db.isDisabled}
                                    />
                                ))}
                                {shard.deletionInProgress.map((node) => {
                                    return (
                                        <NodeSetItem
                                            key={"deletion-" + node}
                                            icon="trash"
                                            color="warning"
                                            title={t("topology.deletionInProgress")}
                                            extraIconClassName="pulse"
                                        >
                                            {node}
                                        </NodeSetItem>
                                    );
                                })}
                            </NodeSet>
                        </React.Fragment>
                    );
                })}
            </div>
        );
    } else {
        return (
            <div>
                <NodeSet
                    className="m-1 cursor-pointer"
                    onClick={togglePanelCollapsed}
                    title={t("properties.expandDetailsTitle")}
                >
                    <NodeSetLabel icon="database">{t("topology.nodes")}</NodeSetLabel>
                    {db.nodes.map((node) => (
                        <DatabaseTopologyNodeSetItem
                            key={node.tag}
                            node={node}
                            localInfos={localInfos}
                            isDisabled={db.isDisabled}
                        />
                    ))}
                    {db.deletionInProgress.map((node) => {
                        return (
                            <NodeSetItem
                                key={"deletion-" + node}
                                icon="trash"
                                color="warning"
                                title={t("topology.deletionInProgress")}
                                extraIconClassName="pulse"
                            >
                                {node}
                            </NodeSetItem>
                        );
                    })}
                </NodeSet>
            </div>
        );
    }
}
