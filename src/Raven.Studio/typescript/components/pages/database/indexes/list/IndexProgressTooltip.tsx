import { IndexingProgress, IndexNodeInfo, IndexNodeInfoDetails, IndexSharedInfo } from "components/models/indexes";

import moment = require("moment");

import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import classNames from "classnames";
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;
import IndexUtils from "../../../../utils/IndexUtils";
import genUtils from "common/generalUtils";
import { withPreventDefault } from "components/utils/common";
import { StatePill } from "components/common/StatePill";
import {
    LocationSpecificDetails,
    LocationSpecificDetailsItem,
    LocationSpecificDetailsItemsContainer,
} from "components/common/LocationSpecificDetails";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

interface IndexProgressTooltipProps {
    target: HTMLElement;
    nodeInfo: IndexNodeInfo;
    index: IndexSharedInfo;
    globalIndexingStatus: IndexRunningStatus;
    showStaleReason: (location: databaseLocationSpecifier) => void;
}

export function IndexProgressTooltip(props: IndexProgressTooltipProps) {
    const { target, nodeInfo, index, globalIndexingStatus, showStaleReason } = props;
    const t = useStudioTranslation("indexesList");

    if (nodeInfo.status === "failure") {
        return (
            <PopoverWithHover target={target} placement="top">
                <div className="flex-horizontal p-3 word-break">
                    <div>
                        <div className="d-flex justify-content-between flex-wrap">
                            <span className="text-danger">
                                <Icon icon="warning" /> {t("tooltip.loadError")}
                            </span>
                            <Button
                                className="rounded-pill"
                                variant="primary"
                                size="xs"
                                onClick={() =>
                                    copyToClipboard.copy(
                                        nodeInfo.loadError.responseJSON.Message,
                                        t("tooltip.errorCopied")
                                    )
                                }
                            >
                                <Icon icon="copy" /> <span>{t("tooltip.copyToClipboard")}</span>
                            </Button>
                        </div>
                        <div className="error-message text-danger mt-3">{nodeInfo.loadError.responseJSON.Message}</div>
                    </div>
                </div>
            </PopoverWithHover>
        );
    }

    if (!nodeInfo.details) {
        return null;
    }

    return (
        <PopoverWithHover
            target={target}
            placement="top"
            style={{ width: index.sourceType === "TimeSeries" ? "460px" : undefined }}
        >
            <LocationSpecificDetails location={nodeInfo.location}>
                <LocationSpecificDetailsItemsContainer>
                    <LocationSpecificDetailsItem>
                        <StatePill bg={pillColor(index, nodeInfo.details, globalIndexingStatus)}>
                            {pillText(index, nodeInfo.details, globalIndexingStatus, t)}
                        </StatePill>
                    </LocationSpecificDetailsItem>
                    <LocationSpecificDetailsItem>
                        <Icon icon="list" />{" "}
                        {t("tooltip.entries", {
                            count: nodeInfo.details.entriesCount,
                            entries: nodeInfo.details.entriesCount.toLocaleString(),
                        })}
                    </LocationSpecificDetailsItem>
                    <LocationSpecificDetailsItem
                        className={classNames("errors", {
                            "text-danger": nodeInfo.details.errorCount > 0,
                        })}
                    >
                        <Icon icon="warning" margin="me-sm-1" />{" "}
                        {t("tooltip.errors", {
                            count: nodeInfo.details.errorCount,
                            errors: nodeInfo.details.errorCount.toLocaleString(),
                        })}
                    </LocationSpecificDetailsItem>
                    {nodeInfo.details.stale ? (
                        <LocationSpecificDetailsItem className="status updating">
                            <div>
                                <div>
                                    <Icon icon="waiting" margin="me-xl-1" />{" "}
                                    {formatTimeLeftToProcess(nodeInfo.progress?.global, nodeInfo.details, t)}
                                </div>
                                <div className="mt-2">
                                    <a
                                        href="#"
                                        className="mt-2"
                                        onClick={withPreventDefault(() => showStaleReason(nodeInfo.location))}
                                    >
                                        {t("tooltip.showStalenessReason")}
                                    </a>
                                </div>
                            </div>
                        </LocationSpecificDetailsItem>
                    ) : (
                        <LocationSpecificDetailsItem className="status">
                            <Icon icon="check" margin="me-sm-1" /> {t("tooltip.upToDate")}
                        </LocationSpecificDetailsItem>
                    )}
                </LocationSpecificDetailsItemsContainer>

                {nodeInfo.progress &&
                    nodeInfo.progress.collections.map((collection) => {
                        return (
                            <NamedProgress name={collection.name} key={collection.name}>
                                <NamedProgressItem progress={collection.documents}>
                                    {t("tooltip.progress.documents")}
                                </NamedProgressItem>
                                <NamedProgressItem progress={collection.tombstones}>
                                    {t("tooltip.progress.tombstones")}
                                </NamedProgressItem>
                                {index.sourceType === "TimeSeries" && (
                                    <NamedProgressItem progress={collection.deletedTimeSeries}>
                                        {t("tooltip.progress.deletedTimeSeries")}
                                    </NamedProgressItem>
                                )}
                            </NamedProgress>
                        );
                    })}
            </LocationSpecificDetails>
        </PopoverWithHover>
    );
}

function pillColor(
    index: IndexSharedInfo,
    details: IndexNodeInfoDetails,
    globalIndexingStatus: IndexRunningStatus
): string {
    if (details.faulty) {
        return "danger";
    }

    if (IndexUtils.isErrorState(details)) {
        return "danger";
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return "warning";
    }

    if (IndexUtils.isErrorState(details)) {
        return "danger";
    }

    return "success";
}

function pillText(
    index: IndexSharedInfo,
    details: IndexNodeInfoDetails,
    globalIndexingStatus: IndexRunningStatus,
    t: StudioTranslate<"indexesList">
) {
    if (details.faulty) {
        return t("tooltip.pill.faulty");
    }

    if (IndexUtils.isErrorState(details)) {
        return t("tooltip.pill.error");
    }

    if (IndexUtils.isPausedState(details, globalIndexingStatus)) {
        return t("tooltip.pill.paused");
    }

    if (IndexUtils.isDisabledState(details, globalIndexingStatus)) {
        return t("tooltip.pill.disabled");
    }

    if (IndexUtils.isIdleState(details, globalIndexingStatus)) {
        return t("tooltip.pill.idle");
    }

    return t("tooltip.pill.normal");
}

function isCompleted(progress: Progress, stale: boolean) {
    return progress.processed === progress.total && !stale;
}

function isDisabled(status: IndexRunningStatus) {
    return status === "Disabled" || status === "Paused";
}

function formatTimeLeftToProcess(
    progress: IndexingProgress,
    nodeDetails: IndexNodeInfoDetails,
    t: StudioTranslate<"indexesList">
) {
    if (!progress) {
        return t("tooltip.timeLeft.updating");
    }

    const { total, processed, processedPerSecond } = progress;

    if (isDisabled(nodeDetails.status)) {
        return t("tooltip.timeLeft.overallProgress");
    }

    if (isCompleted(progress, nodeDetails.stale)) {
        return t("tooltip.timeLeft.completed");
    }

    const leftToProcess = total - processed;
    if (leftToProcess === 0 || processedPerSecond === 0) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails, t);
    }

    const timeLeftInSec = leftToProcess / processedPerSecond;
    if (timeLeftInSec <= 0) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails, t);
    }

    const formattedDuration = genUtils.formatDuration(moment.duration(timeLeftInSec * 1000), true, 2, true);
    if (!formattedDuration) {
        return formatDefaultTimeLeftMessage(progress, nodeDetails, t);
    }

    if (leftToProcess !== 0 && processedPerSecond !== 0) {
        return t("tooltip.timeLeft.estimatedWithRate", {
            duration: formattedDuration,
            rate: (processedPerSecond | 0).toLocaleString(),
        });
    }

    return t("tooltip.timeLeft.estimated", { duration: formattedDuration });
}

function formatDefaultTimeLeftMessage(
    progress: Progress,
    details: IndexNodeInfoDetails,
    t: StudioTranslate<"indexesList">
) {
    const { total, processed } = progress;
    const { stale } = details;

    if (total === processed && stale) {
        return t("tooltip.timeLeft.finalizing");
    }

    return isDisabled(details.status)
        ? t("tooltip.timeLeft.indexIs", { context: details.status === "Paused" ? "paused" : "disabled" })
        : t("tooltip.timeLeft.overallProgress");
}
