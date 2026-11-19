import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React, { useState } from "react";
import { DatabaseSharedInfo, NodeInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import genUtils from "common/generalUtils";
import { SizeOnDisk } from "components/pages/resources/databases/partials/SizeOnDisk";
import { DatabaseLoadError } from "components/pages/resources/databases/partials/DatabaseLoadError";
import { Icon } from "components/common/Icon";
import { selectDatabaseState } from "components/pages/resources/databases/store/databasesViewSelectors";
import { DatabaseNodeSetItem } from "components/pages/resources/databases/partials/DatabaseNodeSetItem";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface DatabaseDistributionProps {
    db: DatabaseSharedInfo;
}

export function DatabaseDistribution(props: DatabaseDistributionProps) {
    const { db } = props;
    const t = useStudioTranslation("databases");
    const isSharded = db.isSharded;
    const [hoveredShardNumber, setHoveredShardNumber] = useState<number | null>(null);
    const dbState = useAppSelector(selectDatabaseState(db.name));

    return (
        <LocationDistribution>
            <DistributionLegend>
                <div className="top"></div>
                {isSharded && (
                    <div className="node">
                        <Icon icon="node" /> {t("distribution.node")}
                    </div>
                )}
                <div>
                    <Icon icon="list" /> {t("distribution.documents")}
                </div>
                <div>
                    <Icon icon="ongoing-tasks" addon="close" /> {t("distribution.taskErrors")}
                </div>
                <div>
                    <Icon icon="warning" /> {t("distribution.indexingErrors")}
                </div>
                <div>
                    <Icon icon="indexing" /> {t("distribution.indexingStatus")}
                </div>
                <div>
                    <Icon icon="alerts" /> {t("distribution.alerts")}
                </div>
                <div>
                    <Icon icon="info" /> {t("distribution.performanceHints")}
                </div>
                <div>
                    <Icon icon="storage" /> {t("distribution.sizeOnDisk")}
                </div>
                <div>
                    <Icon icon="recent" /> {t("distribution.uptime")}
                </div>
            </DistributionLegend>

            {dbState.map((localState) => {
                const shardNumber = localState.location.shardNumber;

                const shard = (
                    <div className="top shard">
                        {shardNumber != null && (
                            <>
                                <Icon icon="shard" />
                                {shardNumber}
                            </>
                        )}
                    </div>
                );

                const nodesToUse = getNodes(db, shardNumber);

                const node = nodesToUse.find((x) => x.tag === localState.location.nodeTag);

                const uptime = localState.data ? DatabaseUtils.formatUptime(localState.data.upTime) : "";
                const isInactive =
                    localState.status === "idle" ||
                    localState.status === "loading" ||
                    uptime === "Offline" ||
                    db.isDisabled;

                return (
                    <DistributionItem
                        key={genUtils.formatLocation(localState.location)}
                        loading={localState.status === "idle" || localState.status === "loading"}
                        className={classNames("distribution-item pb-2", {
                            [`shard-${shardNumber}`]: isSharded && shardNumber != null,
                            hovered: isSharded ? shardNumber === hoveredShardNumber : false,
                        })}
                        onMouseEnter={() => setHoveredShardNumber(shardNumber)}
                        onMouseLeave={() => setHoveredShardNumber(null)}
                    >
                        {isSharded && shard}
                        <div className={classNames("node", { top: !isSharded })}>
                            <DatabaseNodeSetItem node={node} isInactive={isInactive} />
                        </div>
                        <div className="entries">
                            {localState.data?.loadError ? (
                                <DatabaseLoadError error={localState.data.loadError} />
                            ) : (
                                localState.data?.documentsCount.toLocaleString()
                            )}
                        </div>
                        <div className="entries">{localState.data?.tasksErrors?.toLocaleString()}</div>
                        <div className="entries">{localState.data?.indexingErrors?.toLocaleString()}</div>
                        <div className="entries">
                            {localState.data?.indexingStatus &&
                                t("distribution.indexingStatusValue", { context: localState.data.indexingStatus })}
                        </div>
                        <div className="entries">{localState.data?.alerts?.toLocaleString()}</div>
                        <div className="entries">{localState.data?.performanceHints?.toLocaleString()}</div>
                        <div className="entries">
                            <SizeOnDisk info={localState.data} />
                        </div>
                        <div className="entries">{uptime === "Offline" ? t("state.offline") : uptime}</div>
                    </DistributionItem>
                );
            })}
        </LocationDistribution>
    );
}

function getNodes(db: DatabaseSharedInfo, shardNumber: number): NodeInfo[] {
    if (db.isSharded) {
        return db.shards.find((x) => DatabaseUtils.shardNumber(x.name) === shardNumber)?.nodes ?? [];
    }

    return db.nodes;
}
