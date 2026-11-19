import React, { useEffect, useRef, useState } from "react";
import moment from "moment";
import { useAsyncCallback } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import TimeSeriesRangePicker, { TimeSeriesRangeState } from "./TimeSeriesRangePicker";
import { FilterTimezone, wallOf, FULL_FORMAT } from "./timeSeriesRangeUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

const COUNT_DEBOUNCE_MS = 300;

export interface TimeSeriesRangeCount {
    count: number;
    // Whether count is the true total, or only a lower bound (the range has more than we fetched).
    exact: boolean;
}

type TimeSeriesRange = filterTimeSeriesDates<moment.Moment | null>;

interface DeleteTimeSeriesRangeModalProps {
    timeSeriesName: string;
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    timezone?: FilterTimezone;
    resolveCount: (range: TimeSeriesRange) => Promise<TimeSeriesRangeCount>;
    onDelete: (range: TimeSeriesRange) => Promise<void>;
    close: () => void;
}

function rangeSignature(range: TimeSeriesRange): string {
    const stamp = (d: moment.Moment | null) => (d ? String(d.valueOf()) : "∅");
    return `${stamp(range.startDate)}|${stamp(range.endDate)}`;
}

function describeRange(range: TimeSeriesRange, timezone: FilterTimezone, t: StudioTranslate<"editTimeSeries">): string {
    const fmt = (d: moment.Moment) => wallOf(d, timezone).format(FULL_FORMAT);
    const { startDate, endDate } = range;

    if (startDate && endDate) {
        return t("deleteRangeModal.deleting.between", { context: timezone, start: fmt(startDate), end: fmt(endDate) });
    }
    if (endDate) {
        return t("deleteRangeModal.deleting.upTo", { context: timezone, end: fmt(endDate) });
    }
    if (startDate) {
        return t("deleteRangeModal.deleting.from", { context: timezone, start: fmt(startDate) });
    }
    return t("deleteRangeModal.deleting.all");
}

interface CountState {
    loading: boolean;
    result: TimeSeriesRangeCount | null;
    // Set when the count request failed for a reason other than an empty range (e.g. 500/network).
    error?: boolean;
}

export default function DeleteTimeSeriesRangeModal(props: DeleteTimeSeriesRangeModalProps) {
    const { timeSeriesName, startDate, endDate, timezone, resolveCount, onDelete, close } = props;
    const t = useStudioTranslation("editTimeSeries");
    const tCommon = useStudioTranslation("common");

    const [range, setRange] = useState<TimeSeriesRangeState>(() => ({
        range: { startDate, endDate },
        canApply: true,
        timezone: timezone ?? "local",
    }));
    const [countState, setCountState] = useState<CountState>({ loading: true, result: null });

    const rangeRef = useRef(range.range);
    rangeRef.current = range.range;
    // Monotonic request id so a slow in-flight count can't overwrite a newer one.
    const reqIdRef = useRef(0);

    const rangeSig = rangeSignature(range.range);

    useEffect(() => {
        if (!range.canApply) {
            setCountState({ loading: false, result: null });
            return;
        }
        setCountState((prev) => ({ loading: true, result: prev.result }));
        const reqId = ++reqIdRef.current;
        const handle = setTimeout(() => {
            resolveCount(rangeRef.current)
                .then((res) => {
                    if (reqIdRef.current === reqId) {
                        setCountState({ loading: false, result: res });
                    }
                })
                .catch(() => {
                    if (reqIdRef.current === reqId) {
                        setCountState({ loading: false, result: null, error: true });
                    }
                });
        }, COUNT_DEBOUNCE_MS);
        return () => clearTimeout(handle);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [rangeSig, range.canApply]);

    const { loading, result, error } = countState;
    const known = !loading && result != null;
    const countFailed = !loading && !!error;
    const count = result?.count ?? 0;
    const isEmptyRange = known && count === 0;
    const showExactCount = known && result.exact && count > 0;
    const formattedCount = count.toLocaleString();

    const deleteLabel = showExactCount
        ? t("deleteRangeModal.deleteCount", { count, formattedCount })
        : t("deleteRangeModal.deleteButton");
    const deleteDisabled = !range.canApply || loading || isEmptyRange;

    const asyncDelete = useAsyncCallback(async () => {
        await onDelete(range.range);
        close();
    });

    const handleDelete = () => {
        if (deleteDisabled || asyncDelete.loading) {
            return;
        }
        // Failure is surfaced by the delete command itself; keep the dialog open to retry.
        asyncDelete.execute().catch(() => {});
    };

    return (
        <Modal show size="lg" onHide={close} contentClassName="modal-border bulge-danger">
            <Modal.Header onCloseClick={close} className="vstack gap-1 align-items-start">
                <div className="hstack gap-2 align-items-center">
                    <Icon icon="trash" color="danger" margin="m-0" className="fs-3" />
                    <h3 className="mb-0">{t("deleteRangeModal.title")}</h3>
                </div>
                <div className="text-muted">
                    <StudioTrans
                        ns="editTimeSeries"
                        i18nKey="deleteRangeModal.description"
                        components={{ strong: <strong /> }}
                        options={{ name: timeSeriesName }}
                    />
                </div>
            </Modal.Header>
            <Modal.Body>
                <TimeSeriesRangePicker
                    startDate={startDate}
                    endDate={endDate}
                    initialTimezone={timezone}
                    onChange={setRange}
                />

                {range.canApply && loading && (
                    <div className="text-muted mt-3 hstack gap-2 align-items-center">
                        <Spinner size="sm" />
                        <span>{t("deleteRangeModal.checkingCount")}</span>
                    </div>
                )}

                {range.canApply && countFailed && (
                    <RichAlert variant="warning" className="mt-3">
                        {t("deleteRangeModal.countFailed")}
                    </RichAlert>
                )}

                {range.canApply && !loading && isEmptyRange && (
                    <RichAlert variant="info" className="mt-3">
                        {t("deleteRangeModal.emptyRange")}
                    </RichAlert>
                )}

                {range.canApply && !loading && !error && !isEmptyRange && (
                    <RichAlert variant="danger" className="mt-3">
                        <div>
                            {showExactCount ? (
                                <StudioTrans
                                    ns="editTimeSeries"
                                    i18nKey="deleteRangeModal.willBeDeleted"
                                    components={{ strong: <strong /> }}
                                    options={{ count, formattedCount }}
                                />
                            ) : (
                                t("deleteRangeModal.tooManyToCount")
                            )}
                        </div>
                        <div>
                            {describeRange(range.range, range.timezone, t)}{" "}
                            {!showExactCount && <>{t("deleteRangeModal.narrowRange")} </>}
                            {t("deleteRangeModal.cannotBeUndone")}
                        </div>
                    </RichAlert>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={close} disabled={asyncDelete.loading}>
                    {tCommon("cancel")}
                </Button>
                <ConditionalPopover
                    conditions={[
                        { isActive: !range.canApply, message: t("deleteRangeModal.completeRange") },
                        { isActive: range.canApply && isEmptyRange, message: t("deleteRangeModal.noEntries") },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="danger"
                        className="rounded-pill"
                        icon="trash"
                        isSpinning={asyncDelete.loading}
                        disabled={deleteDisabled}
                        onClick={handleDelete}
                    >
                        {asyncDelete.loading ? t("deleteRangeModal.deletingProgress") : deleteLabel}
                    </ButtonWithSpinner>
                </ConditionalPopover>
            </Modal.Footer>
        </Modal>
    );
}
