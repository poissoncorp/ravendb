import "./FileUploadList.scss";
import React, { useEffect, useRef, useState } from "react";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { Icon } from "components/common/Icon";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";
import genUtils from "common/generalUtils";
import useBoolean from "components/hooks/useBoolean";
import useFilePreviews, { getFilePreviewKey } from "components/hooks/useFilePreviews";
import { getFileBadgeLabel, getFileTypeColor } from "components/common/fileTypeBadge";
import { useStudioTranslation } from "hooks/useStudioTranslation";

const rowDurationSeconds = 0.18;

export type FileUploadItemStatus = "selected" | attachmentUploadStatus;

export interface FileUploadItem {
    file: File;
    status: FileUploadItemStatus;
    loaded?: number;
    total?: number;
}

interface FileUploadListProps {
    items: FileUploadItem[];
    onRemove: (file: File) => void;
    onCancel: (file: File) => void;
    isUploading?: boolean;
    collapsedCount?: number;
}

export default function FileUploadList({
    items,
    onRemove,
    onCancel,
    isUploading,
    collapsedCount = 3,
}: FileUploadListProps) {
    const t = useStudioTranslation("commonComponents");
    const { value: isExpanded, toggle } = useBoolean(false);
    const previews = useFilePreviews(items.map((x) => x.file));
    const prefersReducedMotion = useReducedMotion();
    const itemsRef = useRef<HTMLDivElement>(null);
    const [collapsedHeight, setCollapsedHeight] = useState<number>();

    useEffect(() => {
        const el = itemsRef.current;

        if (!el || isExpanded) {
            return;
        }

        const observer = new ResizeObserver(() => {
            const height = el.getBoundingClientRect().height;
            setCollapsedHeight((current) => (current === height ? current : height));
        });

        observer.observe(el);
        return () => observer.disconnect();
    }, [isExpanded]);

    if (items.length === 0) {
        return null;
    }

    const isCollapsible = items.length > collapsedCount;
    const isCapped = isCollapsible && !isExpanded;

    const isHidden = (index: number) => isCapped && index >= collapsedCount;

    const rowTransition = prefersReducedMotion
        ? { duration: 0 }
        : { duration: rowDurationSeconds, ease: "easeOut" as const };

    return (
        <div className="file-upload-list">
            <div className="file-upload-list__card">
                <div
                    ref={itemsRef}
                    className={classNames("file-upload-list__items", { capped: isCapped, expanded: isExpanded })}
                    style={isExpanded && collapsedHeight ? { maxHeight: collapsedHeight } : undefined}
                >
                    <AnimatePresence initial={false}>
                        {items.map((item, index) => (
                            <motion.div
                                key={item.file.name}
                                className={classNames("file-upload-list__row", { hidden: isHidden(index) })}
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: "auto" }}
                                exit={{ opacity: 0, height: 0 }}
                                transition={rowTransition}
                            >
                                <FileUploadRow
                                    item={item}
                                    preview={previews[getFilePreviewKey(item.file)]}
                                    onRemove={onRemove}
                                    onCancel={onCancel}
                                    isUploading={isUploading}
                                />
                            </motion.div>
                        ))}
                    </AnimatePresence>
                    {isCapped && (
                        <div className="file-upload-list__fade">
                            <Button
                                variant="secondary"
                                size="sm"
                                className="file-upload-list__toggle rounded-pill"
                                onClick={toggle}
                            >
                                <Icon icon="chevron-down" />
                                {t("fileUpload.showAll", { count: items.length })}
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            {isCollapsible && isExpanded && (
                <div className="file-upload-list__collapse">
                    <Button variant="link" size="xs" className="p-0" onClick={toggle}>
                        {t("showLess")}
                    </Button>
                </div>
            )}
        </div>
    );
}

interface FileUploadRowProps {
    item: FileUploadItem;
    preview?: string;
    onRemove: (file: File) => void;
    onCancel: (file: File) => void;
    isUploading?: boolean;
}

function FileUploadRow({ item, preview, onRemove, onCancel, isUploading }: FileUploadRowProps) {
    const t = useStudioTranslation("commonComponents");
    const { file, status, loaded, total } = item;
    const percentage = status === "uploading" && total ? Math.floor((loaded * 100) / total) : null;

    return (
        <div className={classNames("file-upload-item", `file-upload-item--${status}`)}>
            <FileTypeBadge file={file} status={status} preview={preview} />
            <div className="file-upload-item__details">
                <div className="file-upload-item__name text-truncate" title={file.name}>
                    {file.name}
                </div>
                <FileUploadStatus item={item} />
            </div>
            {status === "selected" && !isUploading && (
                <Button
                    variant="link"
                    size="sm"
                    className="file-upload-item__action"
                    aria-label={t("fileUpload.remove", { name: file.name })}
                    onClick={() => onRemove(file)}
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            )}
            {status === "uploading" && (
                <Button
                    variant="link"
                    size="sm"
                    className="file-upload-item__action"
                    aria-label={t("fileUpload.cancelUpload", { name: file.name })}
                    onClick={() => onCancel(file)}
                >
                    <Icon icon="cancel" margin="m-0" />
                </Button>
            )}
            {percentage !== null && (
                <div
                    className="file-upload-item__progress"
                    style={{ width: `${percentage}%` }}
                    role="progressbar"
                    aria-label={t("fileUpload.uploadProgress", { name: file.name })}
                    aria-valuenow={percentage}
                    aria-valuemin={0}
                    aria-valuemax={100}
                />
            )}
        </div>
    );
}

interface FileTypeBadgeProps {
    file: File;
    status: FileUploadItemStatus;
    preview?: string;
}

function FileTypeBadge({ file, status, preview }: FileTypeBadgeProps) {
    if (status === "uploaded") {
        return (
            <div className="file-upload-item__badge file-upload-item__badge--success">
                <Icon icon="check" margin="m-0" />
            </div>
        );
    }

    if (preview) {
        return <img className="file-upload-item__thumbnail" src={preview} alt="" />;
    }

    return (
        <div
            className={classNames("file-upload-item__badge", `file-upload-item__badge--${getFileTypeColor(file.name)}`)}
        >
            {getFileBadgeLabel(file.name)}
        </div>
    );
}

function FileUploadStatus({ item }: { item: FileUploadItem }) {
    const t = useStudioTranslation("commonComponents");
    const { file, status, loaded, total } = item;
    const size = genUtils.formatBytesToSize(file.size);

    switch (status) {
        case "uploading":
            return (
                <small className="text-muted d-flex align-items-center gap-1">
                    <Spinner size="sm" />
                    {t("fileUpload.bytesProgress", {
                        loaded: genUtils.formatBytesToSize(loaded),
                        total: genUtils.formatBytesToSize(total),
                    })}
                </small>
            );
        case "uploaded":
            return <StatusLine text={t("fileUpload.status.uploaded")} size={size} />;
        case "failed":
            return <StatusLine icon="danger" color="danger" text={t("fileUpload.status.failed")} size={size} />;
        case "cancelled":
            return <StatusLine icon="cancel" color="warning" text={t("fileUpload.status.cancelled")} size={size} />;
        case "skipped":
            return <StatusLine icon="warning" color="warning" text={t("fileUpload.status.skipped")} size={size} />;
        default:
            return <small className="text-muted">{size}</small>;
    }
}

interface StatusLineProps {
    icon?: IconName;
    color?: ThemeColor;
    text: string;
    size: string;
}

function StatusLine({ icon, color, text, size }: StatusLineProps) {
    return (
        <small className="text-muted d-flex align-items-center gap-1">
            {icon && <Icon icon={icon} color={color} margin="m-0" />}
            {text}
            <span>&bull;</span>
            {size}
        </small>
    );
}
