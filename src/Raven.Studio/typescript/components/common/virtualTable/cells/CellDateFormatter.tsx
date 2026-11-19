import { CellContext } from "@tanstack/react-table";
import CellValue, { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import React, { useMemo } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import moment from "moment";
import genUtils from "common/generalUtils";
import copyToClipboard from "common/copyToClipboard";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

type CellContextSubset<TData, TValue> = Pick<CellContext<TData, TValue>, "cell" | "getValue">;

export interface DateFormatterCellProps<TData, TValue> extends CellContextSubset<TData, TValue> {
    cellClassName?: string;
    displayFormat?: string;
    showTooltip?: boolean;
}

export function DateFormatterCell<TData, TValue>({
    getValue,
    cellClassName,
    displayFormat = genUtils.dateFormat,
    showTooltip = true,
}: DateFormatterCellProps<TData, TValue>) {
    const rawValue = getValue();

    const dateValue = useMemo(() => {
        if (rawValue instanceof Date) {
            return rawValue;
        }
        if (typeof rawValue === "string" || typeof rawValue === "number") {
            const parsed = new Date(rawValue);
            return isNaN(parsed.getTime()) ? null : parsed;
        }
        return null;
    }, [rawValue]);

    const formattedDate = useMemo(() => {
        if (!dateValue) {
            return "";
        }
        return displayFormat ? moment(dateValue).format(displayFormat) : String(rawValue);
    }, [dateValue, displayFormat, rawValue]);

    if (!dateValue) {
        return <CellValueWrapper className={cellClassName} getValue={getValue} />;
    }

    if (!showTooltip) {
        return <CellValueWrapper className={cellClassName} getValue={getValue} />;
    }

    return (
        <PopoverWithHoverWrapper
            message={<DateTooltip date={dateValue} copyValue={typeof rawValue === "string" ? rawValue : null} />}
        >
            <CellValue value={formattedDate} className={cellClassName} />
        </PopoverWithHoverWrapper>
    );
}

function DateTooltip({ date, copyValue }: { date: Date; copyValue: string | null }) {
    const t = useStudioTranslation("virtualTable");
    const utcDate = moment.utc(date).toISOString();

    const handleCopyToClipboard = () => {
        copyToClipboard.copy(copyValue ?? utcDate, t("cells.dateCopied"));
    };

    return (
        <>
            <div className="index-errors-details-tooltip__container">
                <b>{t("cells.utcLabel")} </b>
                <time className="index-errors-details-tooltip__date">{utcDate}</time>
            </div>
            <div className="index-errors-details-tooltip__container">
                <b>{t("cells.relativeLabel")} </b>
                <time>{genUtils.formatDurationByDate(moment.utc(date), true)}</time>
            </div>
            <div className="mt-3">
                <span className="small-label">{t("cells.actions")}</span>
                <div className="d-flex gap-2">
                    <Button onClick={handleCopyToClipboard} size="sm" title={t("cells.copyToClipboard")}>
                        <Icon icon="copy-to-clipboard" margin="m-0" />
                    </Button>
                </div>
            </div>
        </>
    );
}

export default DateFormatterCell;
