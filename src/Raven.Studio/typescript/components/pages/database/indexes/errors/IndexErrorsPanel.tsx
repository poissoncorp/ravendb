import React, { useRef } from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
    RichPanelStatus,
} from "components/common/RichPanel";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import moment from "moment/moment";
import genUtils from "common/generalUtils";
import { useIndexErrorsPanel } from "components/pages/database/indexes/errors/hooks/useIndexErrorsPanel";
import { IndexErrorsPanelTable } from "components/pages/database/indexes/errors/IndexErrorsPanelTable";
import { ErrorInfoItem } from "components/pages/database/indexes/errors/hooks/useIndexErrors";
import { Table } from "@tanstack/react-table";
import { useResizeObserver } from "hooks/useResizeObserver";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AsyncStateStatus, UseAsyncReturn } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import useBoolean from "hooks/useBoolean";
import useUniqueId from "hooks/useUniqueId";
import "./IndexErrorsPanelTooltip.scss";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import DeleteIndexErrorsModal from "components/pages/database/indexes/errors/DeleteIndexErrorsModal";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface IndexErrorsPanelProps {
    errorItem: ErrorInfoItem;
    asyncFetchAllErrorCount: UseAsyncReturn<ErrorInfoItem[]>;
    table: Table<IndexErrorPerDocument>;
}

export function IndexErrorsPanel(props: IndexErrorsPanelProps) {
    const { table, asyncFetchAllErrorCount, errorItem } = props;
    const t = useStudioTranslation("indexErrors");
    const hasDatabaseWriteAccess = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    const {
        selectedErrors,
        isDeleteModalOpen,
        openDeleteModal,
        closeDeleteModal,
        asyncClearSelectedIndexErrors,
        hasErrors,
        mappedIndexErrors,
        newestDate,
        panelCollapsed,
        togglePanelCollapsed,
        asyncFetchErrorDetails,
    } = useIndexErrorsPanel(props);

    const isLoading = asyncFetchErrorDetails.loading || asyncFetchAllErrorCount.loading;

    if (asyncFetchErrorDetails.status === "error" || props.asyncFetchAllErrorCount.status === "error") {
        return (
            <RichPanel className="flex-row with-status">
                <RichPanelStatus className="bg-striped-danger">{t("panel.status.error")}</RichPanelStatus>
                <div className="flex-grow-1" style={{ width: 0 }}>
                    <RichPanelHeader>
                        <RichPanelInfo>
                            <RichPanelName className="d-flex gap-3">
                                <span className="d-flex align-items-center justify-content-center gap-1">
                                    <Icon icon="node" color="node" margin="m-0" /> {errorItem.location.nodeTag}
                                </span>
                                {errorItem.location.shardNumber != null && (
                                    <span className="d-flex align-items-center justify-content-center gap-1">
                                        <Icon icon="shard" color="shard" margin="m-0" /> #
                                        {errorItem.location.shardNumber}
                                    </span>
                                )}
                            </RichPanelName>
                        </RichPanelInfo>
                    </RichPanelHeader>
                    <RichPanelDetails className="pb-0">
                        <RichPanelDetailItem>
                            <Button
                                variant="secondary"
                                onClick={togglePanelCollapsed}
                                title={panelCollapsed ? t("panel.expandDetails") : t("panel.collapseDetails")}
                                className="btn-toggle-panel rounded-pill"
                            >
                                <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                            </Button>
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                    <Collapse unmountOnExit mountOnEnter in={!panelCollapsed}>
                        <RichPanelDetails>
                            <div className="w-100">
                                {(asyncFetchErrorDetails.error as unknown as JQueryXHR).responseJSON.Message}
                            </div>
                        </RichPanelDetails>
                    </Collapse>
                </div>
            </RichPanel>
        );
    }

    return (
        <RichPanel className="flex-row with-status">
            <IndexErrorsPanelStatus
                isLoading={isLoading}
                hasErrors={hasErrors}
                status={asyncFetchErrorDetails.status}
            />
            <div className="flex-grow-1" style={{ width: 0 }}>
                <RichPanelHeader className="py-3">
                    <RichPanelInfo>
                        <RichPanelName className="d-flex gap-3">
                            <span className="d-flex align-items-center justify-content-center gap-1">
                                <Icon icon="node" color="node" margin="m-0" /> {errorItem.location.nodeTag}
                            </span>
                            {errorItem.location.shardNumber != null && (
                                <span className="d-flex align-items-center justify-content-center gap-1">
                                    <Icon icon="shard" color="shard" margin="m-0" /> #{errorItem.location.shardNumber}
                                </span>
                            )}
                        </RichPanelName>
                    </RichPanelInfo>
                    {!isLoading && hasErrors && hasDatabaseWriteAccess && (
                        <RichPanelActions>
                            <Button
                                disabled={isLoading}
                                variant="warning"
                                onClick={openDeleteModal}
                                title={t("panel.deleteErrorsTitle")}
                            >
                                <Icon icon="trash" />
                                {t("panel.deleteErrors")}
                            </Button>
                        </RichPanelActions>
                    )}
                </RichPanelHeader>
                <IndexErrorsPanelDetailsStatus
                    hasErrors={hasErrors}
                    errorItem={errorItem}
                    mappedIndexErrors={mappedIndexErrors}
                    isLoading={isLoading}
                    asyncFetchErrorDetails={asyncFetchErrorDetails}
                    newestDate={newestDate}
                    table={table}
                />
            </div>
            {isDeleteModalOpen && (
                <DeleteIndexErrorsModal
                    location={errorItem.location}
                    selectedIndexNames={selectedErrors}
                    isDeleting={asyncClearSelectedIndexErrors.loading}
                    onConfirm={asyncClearSelectedIndexErrors.execute}
                    onClose={closeDeleteModal}
                />
            )}
        </RichPanel>
    );
}

interface IndexErrorsPanelStatusProps {
    isLoading: boolean;
    hasErrors: boolean;
    status: AsyncStateStatus;
}

function IndexErrorsPanelStatus({ isLoading, hasErrors, status }: IndexErrorsPanelStatusProps) {
    const t = useStudioTranslation("indexErrors");

    if (isLoading) {
        return (
            <RichPanelStatus color="secondary" data-testid="loader">
                {t("panel.status.loading")}
            </RichPanelStatus>
        );
    }

    if (hasErrors) {
        return <RichPanelStatus color="danger">{t("panel.status.errors")}</RichPanelStatus>;
    }

    if (status === "success") {
        return <RichPanelStatus color="success">{t("panel.status.ok")}</RichPanelStatus>;
    }

    return null;
}

interface IndexErrorsPanelDetailsStatusProps {
    isLoading: boolean;
    errorItem: ErrorInfoItem;
    newestDate?: Date;
    mappedIndexErrors: IndexErrorPerDocument[];
    table: Table<IndexErrorPerDocument>;
    asyncFetchErrorDetails: UseAsyncReturn<Raven.Client.Documents.Indexes.IndexErrors[]>;
    hasErrors: boolean;
}

function IndexErrorsPanelDetailsStatus({
    isLoading,
    errorItem,
    newestDate,
    asyncFetchErrorDetails,
    mappedIndexErrors,
    hasErrors,
    table,
}: IndexErrorsPanelDetailsStatusProps) {
    const t = useStudioTranslation("indexErrors");
    const { value: panelCollapsed, toggle: togglePanelCollapsed } = useBoolean(false);
    const ref = useRef<HTMLDivElement>(null);
    const { width } = useResizeObserver({ ref });
    const mostRecentDateId = useUniqueId("most-recent-date");

    if (hasErrors || isLoading) {
        return (
            <>
                <RichPanelDetails>
                    <LazyLoad active={isLoading} className="d-flex">
                        <RichPanelDetailItem>
                            <Button
                                variant="secondary"
                                onClick={togglePanelCollapsed}
                                title={panelCollapsed ? t("panel.expandDetails") : t("panel.collapseDetails")}
                                className="btn-toggle-panel rounded-pill"
                            >
                                <Icon icon={panelCollapsed ? "unfold" : "fold"} margin="m-0" />
                            </Button>
                        </RichPanelDetailItem>
                        <RichPanelDetailItem>
                            <span className="text-danger">
                                <Icon icon="warning" />
                                {t("panel.totalCount")}
                            </span>
                            <div className="value">{t("panel.errorsCount", { count: errorItem.totalErrorCount })}</div>
                        </RichPanelDetailItem>
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    <div className="index-errors-details-tooltip__container">
                                        <b>{t("panel.utc")}</b>
                                        <time className="index-errors-details-tooltip__date">
                                            {moment.utc(newestDate).toISOString()}
                                        </time>
                                    </div>
                                    <div className="index-errors-details-tooltip__container">
                                        <b>{t("panel.relative")}</b>
                                        <time>{genUtils.formatDurationByDate(moment.utc(newestDate), true)}</time>
                                    </div>
                                </>
                            }
                        >
                            <RichPanelDetailItem id={mostRecentDateId}>
                                <span>
                                    <Icon icon="clock" />
                                    {t("panel.mostRecent")}
                                </span>
                                <div className="value">
                                    {newestDate ? moment(newestDate).format(genUtils.dateFormat) : ""}
                                </div>
                            </RichPanelDetailItem>
                        </PopoverWithHoverWrapper>
                    </LazyLoad>
                </RichPanelDetails>
                {!isLoading && (
                    <div ref={ref}>
                        <Collapse in={!panelCollapsed} mountOnEnter unmountOnExit>
                            <RichPanelDetails>
                                <div className="w-100">
                                    <IndexErrorsPanelTable
                                        status={asyncFetchErrorDetails.status}
                                        refresh={asyncFetchErrorDetails.execute}
                                        indexErrors={mappedIndexErrors}
                                        isLoading={asyncFetchErrorDetails.loading}
                                        width={width}
                                        table={table}
                                    />
                                </div>
                            </RichPanelDetails>
                        </Collapse>
                    </div>
                )}
            </>
        );
    }

    return null;
}
