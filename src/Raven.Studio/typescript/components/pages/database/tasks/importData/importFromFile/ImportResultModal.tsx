import React, { JSX, UIEvent, useEffect, useRef } from "react";
import classNames from "classnames";
import "./ImportResultModal.scss";
import Alert from "react-bootstrap/Alert";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Table from "react-bootstrap/Table";
import Spinner from "react-bootstrap/Spinner";
import Modal from "components/common/Modal";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import genUtils from "common/generalUtils";
import { ThemeColor } from "components/models/common";
import moment from "moment";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
type Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
type OperationStatus = Raven.Client.Documents.Operations.OperationStatus;
type OperationExceptionResult = Raven.Client.Documents.Operations.OperationExceptionResult;

export type ImportProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase &
    Pick<SmugglerResult, "Messages"> &
    Partial<Pick<Raven.Client.Documents.Smuggler.ShardedSmugglerProgress, "ShardNumber">>;

interface ImportResultRow {
    name: string;
    isNested: boolean;
    counts: Counts | null;
    parent?: Counts | null;
}

interface ImportResultModalProps {
    progress: ImportProgress | null;
    error?: OperationExceptionResult | null;
    status: OperationStatus;
    startTime: Date;
    endTime: Date | null;
    onClose: () => void;
}

export default function ImportResultModal({
    progress,
    error,
    status,
    startTime,
    endTime,
    onClose,
}: ImportResultModalProps) {
    const t = useStudioTranslation("importFromFile");
    const rows = buildRows(progress, t);
    const duration = genUtils.formatAsTimeSpan(moment(endTime ?? undefined).diff(moment(startTime)));

    const { value: isDetailsVisible, setTrue: showDetails, toggle: toggleDetails } = useBoolean(false);
    const { value: isLogPinnedToBottom, setValue: setIsLogPinnedToBottom } = useBoolean(true);
    const logRef = useRef<HTMLDivElement>(null);
    const messages = [...(progress?.Messages ?? []), ...[error?.Message, error?.Error].filter(Boolean)];

    useEffect(() => {
        if (status === "Faulted") {
            showDetails();
        }
    }, [status, showDetails]);

    useEffect(() => {
        if (isLogPinnedToBottom) {
            scrollToBottom(logRef.current);
        }
    }, [messages.length, isLogPinnedToBottom]);

    const handleLogScroll = (e: UIEvent<HTMLDivElement>) => {
        const { scrollHeight, scrollTop, clientHeight } = e.currentTarget;
        setIsLogPinnedToBottom(scrollHeight - scrollTop - clientHeight < 8);
    };

    return (
        <Modal size="lg" show onHide={onClose} className="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-0">
                <h3 className="mb-0">
                    <Icon icon="import-database" color="primary" /> {t("result.title")}
                </h3>
            </Modal.Header>
            <Modal.Body>
                <div className="import-result-row">
                    <span className="text-muted">{t("result.date")}</span>
                    <span>{moment(startTime).format("YYYY MMMM Do, h:mm A")}</span>
                </div>
                <div className="import-result-row">
                    <span className="text-muted">{t("result.duration")}</span>
                    <span>{duration}</span>
                </div>
                <div className="import-result-row">
                    <span className="text-muted">{t("result.status")}</span>
                    <OperationStatusBadge status={status} />
                </div>
                {status === "InProgress" && progress?.ShardNumber != null && (
                    <Alert variant="info" className="mt-3 mb-0">
                        <Icon icon="info" />
                        {t("result.shardProgressInfo", { shardNumber: progress.ShardNumber })}
                    </Alert>
                )}
                {rows.length > 0 && (
                    <Table className="import-result-table mt-3 mb-0">
                        <thead>
                            <tr>
                                <th></th>
                                <th>{t("result.columns.status")}</th>
                                <th>{t("result.columns.read")}</th>
                                <th>{t("result.columns.skipped")}</th>
                                <th>{t("result.columns.errors")}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((row, index) => (
                                <ImportResultTableRow key={`${row.name}-${index}`} row={row} operationStatus={status} />
                            ))}
                        </tbody>
                    </Table>
                )}
                <Collapse in={isDetailsVisible} onEntered={() => scrollToBottom(logRef.current)}>
                    <div>
                        <Badge bg="info" className="mt-3">
                            {t("result.utcBadge")}
                        </Badge>
                        <div ref={logRef} role="log" className="import-result-log mt-2" onScroll={handleLogScroll}>
                            <Code code={messages.join("\n")} language="plaintext" whiteSpace="pre-wrap" />
                        </div>
                    </div>
                </Collapse>
            </Modal.Body>
            <Modal.Footer>
                <Button onClick={onClose} variant="link" className="link-muted">
                    {t("close")}
                </Button>
                <Button variant="secondary" onClick={toggleDetails}>
                    <Icon icon="preview" /> {isDetailsVisible ? t("result.hideDetails") : t("result.showDetails")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

function OperationStatusBadge({ status }: { status: OperationStatus }) {
    const t = useStudioTranslation("importFromFile");

    switch (status) {
        case "Completed":
            return (
                <Badge bg="success">
                    <Icon icon="check" /> {t("result.operationStatus.completed")}
                </Badge>
            );
        case "Faulted":
            return (
                <Badge bg="danger">
                    <Icon icon="cancel" /> {t("result.operationStatus.failed")}
                </Badge>
            );
        case "Canceled":
            return <Badge bg="warning">{t("result.operationStatus.canceled")}</Badge>;
        case "InProgress":
            return (
                <Badge bg="info" className="d-inline-flex align-items-center">
                    <Spinner className="me-1" style={{ width: "0.85em", height: "0.85em", borderWidth: "0.15em" }} />{" "}
                    {t("result.operationStatus.inProgress")}
                </Badge>
            );
        default:
            return null;
    }
}

function ImportResultTableRow({ row, operationStatus }: { row: ImportResultRow; operationStatus: OperationStatus }) {
    const t = useStudioTranslation("importFromFile");
    const rowStatus = getRowStatus(row, operationStatus, t);
    return (
        <tr className={classNames({ "import-result-nested": row.isNested })}>
            <td className={classNames("import-result-name", { "ps-4": row.isNested })}>
                {row.isNested && <Icon icon="arrow-corner-down-right" color="secondary" margin="me-1" />}
                {row.name}
            </td>
            <td>
                <span className={rowStatus.color && `text-${rowStatus.color}`}>
                    {rowStatus.icon} {rowStatus.label}
                </span>
            </td>
            <td>{row.counts.ReadCount.toLocaleString()}</td>
            <td>{getSkippedCount(row.counts)}</td>
            <td className={row.counts.ErroredCount > 0 ? "text-danger" : undefined}>
                {row.counts.ErroredCount > 0 ? row.counts.ErroredCount.toLocaleString() : "-"}
            </td>
        </tr>
    );
}

function buildRows(progress: ImportProgress | null, t: StudioTranslate<"importFromFile">): ImportResultRow[] {
    if (!progress) {
        return [];
    }
    return [
        { name: t("result.rows.databaseRecord"), isNested: false, counts: progress.DatabaseRecord },
        { name: t("result.rows.documents"), isNested: false, counts: progress.Documents },
        {
            name: t("result.rows.attachments"),
            isNested: true,
            counts: progress.Documents?.Attachments,
            parent: progress.Documents,
        },
        { name: t("result.rows.counters"), isNested: true, counts: progress.Counters },
        { name: t("result.rows.timeSeries"), isNested: true, counts: progress.TimeSeries },
        { name: t("result.rows.tombstones"), isNested: true, counts: progress.Tombstones },
        { name: t("result.rows.revisions"), isNested: false, counts: progress.RevisionDocuments },
        {
            name: t("result.rows.attachments"),
            isNested: true,
            counts: progress.RevisionDocuments?.Attachments,
            parent: progress.RevisionDocuments,
        },
        { name: t("result.rows.conflicts"), isNested: false, counts: progress.Conflicts },
        { name: t("result.rows.indexes"), isNested: false, counts: progress.Indexes },
        { name: t("result.rows.identities"), isNested: false, counts: progress.Identities },
        { name: t("result.rows.compareExchange"), isNested: false, counts: progress.CompareExchange },
        {
            name: t("result.rows.compareExchangeTombstones"),
            isNested: true,
            counts: progress.CompareExchangeTombstones,
        },
        { name: t("result.rows.subscriptions"), isNested: false, counts: progress.Subscriptions },
        { name: t("result.rows.timeSeriesDeletedRanges"), isNested: false, counts: progress.TimeSeriesDeletedRanges },
    ].filter((row) => row.counts != null);
}

function getRowStatus(
    { counts, parent }: ImportResultRow,
    operationStatus: OperationStatus,
    t: StudioTranslate<"importFromFile">
): { label: string; icon: JSX.Element; color?: ThemeColor } {
    if (counts.Skipped) {
        return { label: t("result.rowStatus.skipped"), icon: <Icon icon="skip" color="warning" />, color: "warning" };
    }
    if (counts.Processed) {
        return counts.ErroredCount > 0
            ? {
                  label: t("result.rowStatus.processedWithErrors"),
                  icon: <Icon icon="warning" color="warning" />,
                  color: "warning",
              }
            : { label: t("result.rowStatus.processed"), icon: <Icon icon="check" color="success" />, color: "success" };
    }
    if (operationStatus === "InProgress") {
        const isStarted = !!(counts.StartTime ?? parent?.StartTime);
        return isStarted
            ? { label: t("result.rowStatus.processing"), icon: <Spinner size="sm" className="me-2" /> }
            : { label: t("result.rowStatus.pending"), icon: <Icon icon="waiting" /> };
    }
    return { label: t("result.rowStatus.notProcessed"), icon: <Icon icon="cancel" color="danger" />, color: "danger" };
}

function getSkippedCount(counts: Counts): string {
    return "SkippedCount" in counts ? counts.SkippedCount.toLocaleString() : "-";
}

function scrollToBottom(element: HTMLElement | null) {
    if (element) {
        element.scrollTop = element.scrollHeight;
    }
}
