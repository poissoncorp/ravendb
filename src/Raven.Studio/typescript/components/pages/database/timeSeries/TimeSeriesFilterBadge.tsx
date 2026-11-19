import React from "react";
import moment from "moment";
import { FilterTimezone, wallOf } from "./timeSeriesRangeUtils";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";
import "./TimeSeriesFilterBadge.scss";

// Deliberately compact (no seconds/ms) for a glanceable chip, but same ISO family as the grid.
const BADGE_FORMAT = "YYYY-MM-DD HH:mm";

interface TimeSeriesFilterBadgeProps {
    startDate: moment.Moment | null;
    endDate: moment.Moment | null;
    // Zone the grid is displaying timestamps in. The bounds are absolute instants, so
    // without this the badge always renders browser-local and can contradict a UTC grid.
    timezone?: FilterTimezone;
    onEdit: () => void;
    onClear: () => void;
}

function formatFilter(
    startDate: moment.Moment | null,
    endDate: moment.Moment | null,
    timezone: FilterTimezone,
    t: StudioTranslate<"editTimeSeries">
): string | null {
    const fmt = (d: moment.Moment) => wallOf(d, timezone).format(BADGE_FORMAT);

    if (startDate && endDate) {
        return t("filterBadge.between", { context: timezone, start: fmt(startDate), end: fmt(endDate) });
    }
    if (endDate) {
        return t("filterBadge.before", { context: timezone, end: fmt(endDate) });
    }
    if (startDate) {
        return t("filterBadge.after", { context: timezone, start: fmt(startDate) });
    }
    return null;
}

export default function TimeSeriesFilterBadge({
    startDate,
    endDate,
    timezone = "local",
    onEdit,
    onClear,
}: TimeSeriesFilterBadgeProps) {
    const t = useStudioTranslation("editTimeSeries");
    const text = formatFilter(startDate, endDate, timezone, t);

    if (!text) {
        return null;
    }

    // A bootstrap-3 split button (must stay outside a .bs5 scope so the legacy .btn styling
    // resolves): left segment opens the filter dialog, right segment clears it.
    return (
        <div className="btn-group time-series-filter-badge">
            <button type="button" className="btn" title={t("filterBadge.editTitle")} onClick={onEdit}>
                <i className="icon-calendar" />
                <span className="margin-left margin-left-xs">{text}</span>
            </button>
            <button
                type="button"
                className="btn time-series-filter-badge__clear"
                title={t("filterBadge.clearTitle")}
                onClick={onClear}
            >
                <i className="icon-cancel" />
            </button>
        </div>
    );
}
