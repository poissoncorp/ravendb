import { IndexNodeInfo, IndexSharedInfo } from "components/models/indexes";
import React, { useState } from "react";
import classNames from "classnames";
import { IndexProgressTooltip } from "./IndexProgressTooltip";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";
import {
    DistributionItem,
    DistributionLegend,
    DistributionSummary,
    LocationDistribution,
} from "components/common/LocationDistribution";
import assertUnreachable from "../../../../utils/assertUnreachable";
import { ProgressCircle } from "components/common/ProgressCircle";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import IndexDistributionStatusChecker from "./IndexDistributionStatusChecker";
import moment = require("moment");
import genUtils = require("common/generalUtils");
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useRavenLink } from "components/hooks/useRavenLink";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

interface IndexDistributionProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
    openFaulty: (location: databaseLocationSpecifier) => void;
}

interface ItemWithTooltipProps {
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
    openFaulty: (location: databaseLocationSpecifier) => void;
    nodeInfo: IndexNodeInfo;
    sharded: boolean;
}

function getFormattedTime(date: Date, t: StudioTranslate<"indexesList">): string {
    return date ? t("distribution.timeAgo", { duration: genUtils.formatDurationByDate(moment(date)) }) : "-";
}

function ItemWithTooltip(props: ItemWithTooltipProps) {
    const { nodeInfo, sharded, openFaulty, showStaleReason, globalIndexingStatus, index } = props;
    const t = useStudioTranslation("indexesList");
    const entriesCount = nodeInfo.details?.faulty
        ? t("distribution.notAvailable")
        : (nodeInfo.details?.entriesCount ?? "");

    const shard = (
        <div className="top shard">
            {nodeInfo.location.shardNumber != null && (
                <>
                    <Icon icon="shard" />
                    {nodeInfo.location.shardNumber}
                </>
            )}
        </div>
    );

    const [node, setNode] = useState<HTMLDivElement>();

    return (
        <div ref={setNode}>
            <DistributionItem loading={nodeInfo.status === "loading" || nodeInfo.status === "idle"}>
                {sharded && shard}
                <div className={classNames("node", { top: !sharded })}>
                    {!sharded && <Icon icon="node" />}

                    {nodeInfo.location.nodeTag}
                </div>
                <div className="entries">{entriesCount.toLocaleString()}</div>
                <div className="errors">{nodeInfo.details?.errorCount.toLocaleString() ?? ""}</div>
                <div className="text-center">{getFormattedTime(nodeInfo.details?.lastIndexingTime, t)}</div>
                <div className="text-center">{getFormattedTime(nodeInfo.details?.lastQueryingTime, t)}</div>

                <IndexProgress nodeInfo={nodeInfo} />

                {nodeInfo.details?.faulty && (
                    <div className="text-center">
                        <Button
                            variant="danger"
                            className="px-1 py-0 my-1"
                            size="xs"
                            onClick={() => openFaulty(nodeInfo.location)}
                        >
                            {t("actions.openFaulty")}
                        </Button>
                    </div>
                )}
            </DistributionItem>
            {node && (
                <IndexProgressTooltip
                    target={node}
                    nodeInfo={nodeInfo}
                    index={index}
                    globalIndexingStatus={globalIndexingStatus}
                    showStaleReason={showStaleReason}
                />
            )}
        </div>
    );
}

export function IndexDistribution(props: IndexDistributionProps) {
    const { index, globalIndexingStatus, showStaleReason, openFaulty } = props;
    const t = useStudioTranslation("indexesList");

    const totalErrors = index.nodesInfo
        .filter((x) => x.status === "success")
        .reduce((prev, current) => prev + current.details.errorCount, 0);
    const estimatedEntries = IndexUtils.estimateEntriesCount(index)?.toLocaleString() ?? "-";

    const sharded = IndexUtils.isSharded(index);

    const earliestCreatedTimestamp = IndexUtils.getEarliestCreatedTimestamp(index);
    const formattedEarliestCreatedTimestamp = earliestCreatedTimestamp
        ? t("distribution.timeAgo", { duration: genUtils.formatDurationByDate(moment(earliestCreatedTimestamp)) })
        : null;

    const items = (
        <>
            {[...index.nodesInfo]
                .sort((l, r) => l.location.shardNumber - r.location.shardNumber)
                .map((nodeInfo) => {
                    const key = indexNodeInfoKey(nodeInfo);
                    return (
                        <ItemWithTooltip
                            key={key}
                            nodeInfo={nodeInfo}
                            sharded={sharded}
                            index={index}
                            globalIndexingStatus={globalIndexingStatus}
                            showStaleReason={showStaleReason}
                            openFaulty={openFaulty}
                        />
                    );
                })}
        </>
    );

    return (
        <div>
            <LocationDistribution>
                <DistributionLegend>
                    <div className="top"></div>
                    {sharded && (
                        <div className="node">
                            <Icon icon="node" /> {t("distribution.legend.node")}
                        </div>
                    )}
                    <div>
                        <Icon icon="list" /> {t("distribution.legend.entries")}
                    </div>
                    <div>
                        <Icon icon="warning" /> {t("distribution.legend.errors")}
                    </div>
                    <div>
                        <Icon icon="index-history" /> {t("distribution.legend.indexed")}
                    </div>
                    <div>
                        <Icon icon="queries" /> {t("distribution.legend.queried")}
                    </div>
                    <div>
                        <Icon icon="changes" /> {t("distribution.legend.state")}
                    </div>
                </DistributionLegend>
                <DistributionSummary>
                    <div className="top">{t("distribution.total")}</div>
                    {sharded && <div> </div>}
                    <div>{estimatedEntries}</div>
                    <div>{totalErrors}</div>
                    <div></div>
                </DistributionSummary>
                {items}
            </LocationDistribution>
            {formattedEarliestCreatedTimestamp && (
                <div className="small">
                    <span className="text-muted">{t("distribution.created")}</span>{" "}
                    <strong>{formattedEarliestCreatedTimestamp}</strong>
                </div>
            )}
        </div>
    );
}

interface IndexProgressProps {
    nodeInfo: IndexNodeInfo;
}

function iconForState(status: Raven.Client.Documents.Indexes.IndexRunningStatus): IconName {
    switch (status) {
        case "Disabled":
            return "stop";
        case "Paused":
            return "pause";
        case "Pending":
            return "waiting";
        case "Running":
            return "check";
        default:
            assertUnreachable(status);
    }
}

interface JoinedIndexProgressProps {
    index: IndexSharedInfo;
    onClick: () => void;
}

function calculateOverallProgress(index: IndexSharedInfo) {
    const allProgresses = index.nodesInfo.filter((x) => x.status === "success" && x.progress);
    if (!allProgresses.length) {
        return 0;
    }

    const processed = allProgresses.reduce((p, c) => p + c.progress.global.processed, 0);
    const total = allProgresses.reduce((p, c) => p + c.progress.global.total, 0);

    if (total === 0) {
        return 1;
    }

    return processed / total;
}

function getState(statusChecker: IndexDistributionStatusChecker) {
    if (statusChecker.everyFailure() || statusChecker.someFaulty() || statusChecker.someErrors()) {
        return "failed";
    }
    if (statusChecker.someFailure()) {
        return "warning";
    }
    if (
        statusChecker.someDisabled() ||
        statusChecker.somePaused() ||
        statusChecker.somePending() ||
        statusChecker.someStale()
    ) {
        return "running";
    }
    return "success";
}

function getIcon(statusChecker: IndexDistributionStatusChecker): IconName {
    if (statusChecker.everyFailure() || statusChecker.someFaulty() || statusChecker.someErrors()) {
        return "cancel";
    }
    if (statusChecker.someFailure()) {
        return "warning";
    }
    if (statusChecker.someDisabled()) {
        return iconForState("Disabled");
    }
    if (statusChecker.somePaused()) {
        return iconForState("Paused");
    }
    if (statusChecker.somePending()) {
        return iconForState("Pending");
    }
    if (statusChecker.everyIdle()) {
        return "check";
    }
    if (statusChecker.someStale()) {
        return null;
    }
    return "check";
}

function getStateTextKey(statusChecker: IndexDistributionStatusChecker) {
    if (statusChecker.everyFailure()) {
        return "loadErrors";
    }
    if (statusChecker.someFailure()) {
        return "someLoadErrors";
    }
    if (statusChecker.everyFaulty()) {
        return "faulty";
    }
    if (statusChecker.someFaulty()) {
        return "someFaulty";
    }
    if (statusChecker.everyErrors()) {
        return "errors";
    }
    if (statusChecker.someErrors()) {
        return "someErrored";
    }
    if (statusChecker.everyDisabled()) {
        return "disabled";
    }
    if (statusChecker.someDisabled()) {
        return "someDisabled";
    }
    if (statusChecker.everyPaused()) {
        return "paused";
    }
    if (statusChecker.somePaused()) {
        return "somePaused";
    }
    if (statusChecker.everyPending()) {
        return "pending";
    }
    if (statusChecker.somePending()) {
        return "somePending";
    }
    if (statusChecker.someStale()) {
        return "running";
    }
    if (statusChecker.everyIdle()) {
        return "idle";
    }
    return "upToDate";
}

export function JoinedIndexProgress(props: JoinedIndexProgressProps) {
    const { index, onClick } = props;
    const t = useStudioTranslation("indexesList");

    const overallProgress = calculateOverallProgress(index);

    const statusChecker = new IndexDistributionStatusChecker(index);
    const stateTextKey = getStateTextKey(statusChecker);

    return (
        <ProgressCircle
            inline
            state={getState(statusChecker)}
            icon={getIcon(statusChecker)}
            progress={stateTextKey === "running" ? overallProgress : null}
            onClick={onClick}
            descClassName="d-flex align-items-center"
        >
            {t(`distribution.state.${stateTextKey}`)}
            {statusChecker.everyIdle() && <IdleIndexInfoIcon />}
        </ProgressCircle>
    );
}

export function IndexProgress(props: IndexProgressProps) {
    const { nodeInfo } = props;
    const t = useStudioTranslation("indexesList");

    if (nodeInfo.status === "failure") {
        return (
            <ProgressCircle state="failed" icon="cancel">
                {t("distribution.progress.loadError")}
            </ProgressCircle>
        );
    }

    if (!nodeInfo.details) {
        return null;
    }

    if (nodeInfo.details.faulty) {
        return (
            <ProgressCircle state="failed" icon="cancel">
                {t("distribution.progress.faulty")}
            </ProgressCircle>
        );
    }

    if (nodeInfo.details.state === "Error") {
        return (
            <ProgressCircle state="failed" icon="cancel">
                {t("distribution.progress.error")}
            </ProgressCircle>
        );
    }

    if (nodeInfo.details.state === "Idle") {
        return (
            <ProgressCircle state="success" icon="check" descClassName="d-flex align-items-center">
                {t("distribution.progress.idle")}
                <IdleIndexInfoIcon />
            </ProgressCircle>
        );
    }

    const icon = iconForState(nodeInfo.details.status);

    if (nodeInfo.details.stale) {
        const progress = nodeInfo.progress
            ? nodeInfo.progress.global.total
                ? nodeInfo.progress.global.processed / nodeInfo.progress.global.total
                : 1
            : null;

        return (
            <ProgressCircle
                state="running"
                icon={nodeInfo.details.status === "Running" ? null : icon}
                progress={progress}
            >
                {getStatusText(nodeInfo.details.status, t)}
            </ProgressCircle>
        );
    }

    if (
        nodeInfo.details.status === "Paused" ||
        nodeInfo.details.status === "Disabled" ||
        nodeInfo.details.status === "Pending"
    ) {
        return (
            <ProgressCircle state="running" icon={icon}>
                {getStatusText(nodeInfo.details.status, t)}
            </ProgressCircle>
        );
    }

    return (
        <ProgressCircle state="success" icon={icon}>
            {t("distribution.progress.upToDate")}
        </ProgressCircle>
    );
}

function getStatusText(status: IndexRunningStatus, t: StudioTranslate<"indexesList">): string {
    switch (status) {
        case "Disabled":
            return t("indexStatus.disabled");
        case "Paused":
            return t("indexStatus.paused");
        case "Pending":
            return t("indexStatus.pending");
        case "Running":
            return t("indexStatus.running");
        default:
            assertUnreachable(status);
    }
}

function IdleIndexInfoIcon() {
    const t = useStudioTranslation("indexesList");
    const docsLink = useRavenLink({ hash: "2VWBSO" });

    return (
        <PopoverWithHoverWrapper
            message={
                <>
                    <StudioTrans ns="indexesList" i18nKey="distribution.idleInfo" components={{ br: <br /> }} />
                    <br />
                    <br />
                    <a href={docsLink} target="_blank">
                        {t("distribution.idleDocsLink")}
                    </a>
                </>
            }
            wrapperClassName="d-flex"
        >
            <Icon icon="info" color="info" margin="ms-1" className="fs-4" />
        </PopoverWithHoverWrapper>
    );
}

const indexNodeInfoKey = (nodeInfo: IndexNodeInfo) => nodeInfo.location.shardNumber + "__" + nodeInfo.location.nodeTag;
