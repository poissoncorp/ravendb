import React from "react";
import Button from "react-bootstrap/Button";
import ProgressBar from "react-bootstrap/ProgressBar";
import classNames from "classnames";
import genUtils from "common/generalUtils";
import { ThemeColor } from "components/models/common";
import { FileUploadItem, FileUploadItemStatus } from "components/common/FileUploadList";
import { useStudioTranslation } from "hooks/useStudioTranslation";

interface FileUploadSummaryProps {
    items: FileUploadItem[];
    onClearAll: () => void;
    heading?: string;
}

const terminalStatuses: FileUploadItemStatus[] = ["uploaded", "failed", "skipped", "cancelled"];

const rollupOrder = [
    { status: "uploaded", labelKey: "fileUpload.uploadedCount", color: "success" },
    { status: "failed", labelKey: "fileUpload.failedCount", color: "danger" },
    { status: "skipped", labelKey: "fileUpload.skippedCount", color: "warning" },
    { status: "cancelled", labelKey: "fileUpload.cancelledCount", color: "warning" },
] as const satisfies readonly { status: FileUploadItemStatus; labelKey: string; color: ThemeColor }[];

export default function FileUploadSummary({ items, onClearAll, heading }: FileUploadSummaryProps) {
    const t = useStudioTranslation("commonComponents");

    if (items.length === 0) {
        return null;
    }

    const isUploading = items.some((x) => x.status === "uploading");
    const isFinished = !isUploading && items.every((x) => terminalStatuses.includes(x.status));

    const totalBytes = items.reduce((sum, x) => sum + x.file.size, 0);
    const loadedBytes = items.reduce((sum, x) => {
        if (x.status === "uploaded") {
            return sum + x.file.size;
        }
        return x.status === "uploading" ? sum + (x.loaded ?? 0) : sum;
    }, 0);

    const position = items.filter((x) => terminalStatuses.includes(x.status)).length + 1;
    const percentage = totalBytes ? Math.floor((loadedBytes * 100) / totalBytes) : 0;

    return (
        <div className="file-upload-summary">
            <div className="file-upload-summary__line">
                <span className="file-upload-summary__heading">{heading ?? t("fileUpload.heading")}</span>
                {isUploading && (
                    <>
                        <span className="text-muted">
                            {t("fileUpload.uploading", { position, total: items.length })}
                        </span>
                        <span className="text-muted ms-auto">
                            {t("fileUpload.bytesProgress", {
                                loaded: genUtils.formatBytesToSize(loadedBytes),
                                total: genUtils.formatBytesToSize(totalBytes),
                            })}
                        </span>
                    </>
                )}
                {isFinished && <StatusRollup items={items} />}
                {!isUploading && !isFinished && (
                    <>
                        <span className="text-muted">
                            {t("fileUpload.filesSummary", {
                                count: items.length,
                                formattedCount: items.length.toLocaleString(),
                                size: genUtils.formatBytesToSize(totalBytes),
                            })}
                        </span>
                        <Button variant="link" size="xs" className="p-0 ms-auto" onClick={onClearAll}>
                            {t("fileUpload.clearAll")}
                        </Button>
                    </>
                )}
            </div>
            {isUploading && <ProgressBar now={percentage} className="file-upload-summary__bar" />}
        </div>
    );
}

function StatusRollup({ items }: { items: FileUploadItem[] }) {
    const t = useStudioTranslation("commonComponents");
    const counts = rollupOrder
        .map((entry) => ({ ...entry, count: items.filter((x) => x.status === entry.status).length }))
        .filter((entry) => entry.count > 0);

    return (
        <span className="d-flex align-items-center gap-3 text-muted ms-auto">
            {counts.map((entry) => (
                <span key={entry.status} className="d-flex align-items-center gap-1">
                    <span className={classNames("file-upload-summary__dot", `bg-${entry.color}`)} />
                    {t(entry.labelKey, { count: entry.count })}
                </span>
            ))}
        </span>
    );
}
