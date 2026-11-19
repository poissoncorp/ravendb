import moment from "moment";
import genUtils from "common/generalUtils";
import type { StudioTranslate } from "common/i18n/resources";

export type FilterTimezone = "local" | "utc";

export const FULL_FORMAT = genUtils.timeSeriesFullDateFormat;

export function wallOf(instant: moment.Moment, tz: FilterTimezone): moment.Moment {
    return tz === "utc" ? instant.clone().utc() : instant.clone().local();
}

export function zoneLabel(tz: FilterTimezone, t: StudioTranslate<"editTimeSeries">): string {
    return t("zone", { context: tz });
}
