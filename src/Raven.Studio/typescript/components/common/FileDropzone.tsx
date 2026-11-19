import "./FileDropzone.scss";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import React, { useState, useRef, useEffect, DragEvent, ChangeEvent } from "react";
import genUtils from "common/generalUtils";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

const dropPulseMs = 320;

interface FileDropzoneProps {
    onChange: (files: File[]) => void;
    maxFiles?: number;
    validExtensions?: string[];
    initialFiles?: File[];
    className?: string;
    showSelectedFiles?: boolean;
    disabled?: boolean;
    displayedExtensions?: string[];
}

export default function FileDropzone({
    onChange,
    validExtensions = [],
    maxFiles = Infinity,
    initialFiles = [],
    className,
    showSelectedFiles = true,
    disabled = false,
    displayedExtensions,
}: FileDropzoneProps) {
    const t = useStudioTranslation("commonComponents");
    const fileInputRef = useRef<HTMLInputElement>(null);

    const [isDragging, setIsDragging] = useState(false);
    const [isDropped, setIsDropped] = useState(false);
    const dragDepthRef = useRef(0);
    const dropPulseTimerRef = useRef<number | undefined>(undefined);

    const [files, setFiles] = useState<File[]>(initialFiles);
    const [error, setError] = useState<string>();

    useEffect(() => () => window.clearTimeout(dropPulseTimerRef.current), []);

    const endDragging = () => {
        dragDepthRef.current = 0;
        setIsDragging(false);
    };

    const handleDragEnter = () => {
        dragDepthRef.current += 1;
        setIsDragging(true);
    };

    const handleDragLeave = () => {
        dragDepthRef.current = Math.max(dragDepthRef.current - 1, 0);

        if (dragDepthRef.current === 0) {
            setIsDragging(false);
        }
    };

    const handleDrop = (e: DragEvent<HTMLDivElement>) => {
        endDragging();

        const files = Array.from(e.dataTransfer.files || []);

        if (files.length > 0) {
            setIsDropped(true);
            window.clearTimeout(dropPulseTimerRef.current);
            dropPulseTimerRef.current = window.setTimeout(() => setIsDropped(false), dropPulseMs);
        }

        handleFilesChange(files);
    };

    const handleFileInput = (e: ChangeEvent<HTMLInputElement>) => {
        const files = Array.from(e.target.files || []);
        handleFilesChange(files);
    };

    const handleFilesChange = (files: File[]) => {
        if (files.length > maxFiles) {
            setError(t("fileDropzone.tooManyFiles", { count: maxFiles }));
            handleSetFiles([]);
            return;
        }

        const hasUnsupportedFile =
            validExtensions.length > 0 &&
            files.some((file) => !validExtensions.includes(genUtils.getFileExtension(file.name)));

        if (hasUnsupportedFile) {
            setError(t("fileDropzone.unsupportedFiles", { extensions: formatExtensions(validExtensions) }));
            handleSetFiles([]);
            return;
        }

        setError(null);
        handleSetFiles(files);
    };

    const handleSetFiles = (files: File[]) => {
        setFiles(files);
        onChange(files);
    };

    const openFileDialog = () => {
        fileInputRef.current?.click();
    };

    return (
        <div className={className}>
            <div className={classNames("file-dropzone", { isDragging, isDropped, disabled })}>
                <input
                    data-testid="file-input"
                    type="file"
                    ref={fileInputRef}
                    onChange={handleFileInput}
                    multiple
                    accept={validExtensions.map((ext) => `.${ext}`).join(",")}
                    style={{ display: "none" }}
                />

                {!disabled && (
                    <div
                        className="drop-area"
                        onDragEnter={withPrevent(handleDragEnter)}
                        onDragLeave={withPrevent(handleDragLeave)}
                        onDrop={withPrevent(handleDrop)}
                        onClick={openFileDialog}
                        onDragOver={withPrevent(() => {
                            // empty by design (prevents opening file in a new tab)
                        })}
                    />
                )}

                <DropzoneBody
                    files={showSelectedFiles ? files : []}
                    error={error}
                    maxFiles={maxFiles}
                    isDragging={isDragging}
                />
                <ValidExtensionsList validExtensions={displayedExtensions ?? validExtensions} />
            </div>
        </div>
    );
}

interface DropzoneBodyProps {
    files: File[];
    error: string;
    maxFiles: number;
    isDragging: boolean;
}

function DropzoneBody({ files, error, maxFiles, isDragging }: DropzoneBodyProps) {
    const t = useStudioTranslation("commonComponents");
    const filesContext = maxFiles === 1 ? "single" : "multiple";

    if (isDragging) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="file-import" className="fs-2" margin="m-0" />
                <span className="text-emphasis">{t("fileDropzone.dropHere", { context: filesContext })}</span>
            </div>
        );
    }

    if (error) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="warning" color="danger" className="fs-2" margin="m-0" />
                <div>
                    <p className="m-0 text-danger">{error}</p>
                    <Button variant="link">{t("fileDropzone.tryAgain")}</Button>
                </div>
            </div>
        );
    }

    if (files.length === 0) {
        return (
            <div className="d-flex gap-3 flex-vertical">
                <Icon icon="file-import" className="fs-2" margin="m-0" />
                <div>
                    <span className="text-muted">
                        <StudioTrans
                            ns="commonComponents"
                            i18nKey="fileDropzone.dropOrBrowse"
                            components={{ link: <span className="link" /> }}
                            options={{ context: filesContext }}
                        />
                    </span>
                </div>
            </div>
        );
    }

    const maxVisibleFiles = 2;

    return (
        <div className="d-flex gap-3 flex-vertical">
            <Icon icon="document" className="fs-2" margin="m-0" />
            <div>
                <div className="vstack gap-1 overflow-hidden" style={{ maxHeight: "100px" }}>
                    {files.slice(0, maxVisibleFiles).map((file) => (
                        <p key={file.name} className="m-0">
                            {file.name} ({genUtils.formatBytesToSize(file.size)})
                        </p>
                    ))}
                    {files.length > maxVisibleFiles && (
                        <p className="m-0">
                            <span className="text-muted">
                                {t("fileDropzone.andMore", { count: files.length - maxVisibleFiles })}
                            </span>
                        </p>
                    )}
                </div>

                <Button variant="link">{t("fileDropzone.changeFile")}</Button>
            </div>
        </div>
    );
}

interface ValidExtensionsListProps {
    validExtensions: string[];
}

function ValidExtensionsList({ validExtensions }: ValidExtensionsListProps) {
    if (validExtensions.length === 0) {
        return null;
    }

    return (
        <Badge bg="secondary" className="file-dropzone__constraints">
            {formatExtensions(validExtensions)}
        </Badge>
    );
}

function formatExtensions(validExtensions: string[]): string {
    return validExtensions.map((ext) => `.${ext}`).join(", ");
}

function withPrevent(fn: (...args: any[]) => void): React.DragEventHandler<HTMLDivElement> {
    return (e: React.DragEvent<HTMLDivElement>) => {
        e.preventDefault();
        e.stopPropagation();
        fn(e);
    };
}
