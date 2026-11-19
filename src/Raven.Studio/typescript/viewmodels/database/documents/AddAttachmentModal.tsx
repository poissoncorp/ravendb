import Modal from "components/common/Modal";
import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { Icon } from "components/common/Icon";
import FileUploadPanel from "components/common/FileUploadPanel";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import useAttachmentUpload from "components/hooks/useAttachmentUpload";
import messagePublisher from "common/messagePublisher";
import document from "models/database/documents/document";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";

type AddAttachmentModalProps = {
    document: KnockoutObservable<document>;
    onUploaded: () => void;
    onClose: () => void;
};

export default function AddAttachmentModal({ document, onUploaded, onClose }: AddAttachmentModalProps) {
    const t = useStudioTranslation("editDocument");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
    const { uploadItems, batchProgress, upload, abortCurrent } = useAttachmentUpload(
        document,
        databaseName,
        onUploaded
    );

    const asyncUpload = useAsyncCallback(async () => {
        if (await upload(selectedFiles)) {
            onClose();
        }
    });

    const addFiles = (files: File[]) => {
        if (files.some((file) => !file.name.trim())) {
            messagePublisher.reportError(t("attachmentUpload.loadFileFailed"));
            return;
        }

        setSelectedFiles((current) => [
            ...current.filter((x) => !files.some((file) => file.name === x.name)),
            ...files,
        ]);
    };

    const removeFile = (file: File) => setSelectedFiles((current) => current.filter((x) => x !== file));

    const attachmentsCount = selectedFiles.length || 1;

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-primary">
            <Modal.Header className="pb-0" onCloseClick={onClose}>
                <h3>
                    <Icon icon="attachment" color="primary" />
                    {t("addAttachment.title", { count: attachmentsCount })}
                </h3>
            </Modal.Header>
            <Modal.Body>
                <FileUploadPanel
                    items={uploadItems(selectedFiles)}
                    onChange={addFiles}
                    onRemove={removeFile}
                    onCancel={abortCurrent}
                    onClearAll={() => setSelectedFiles([])}
                />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="text-muted" onClick={onClose}>
                    {t("close")}
                </Button>
                <ButtonWithSpinner
                    className="rounded-pill"
                    variant="primary"
                    isSpinning={asyncUpload.loading}
                    onClick={asyncUpload.execute}
                    disabled={selectedFiles.length === 0}
                >
                    {batchProgress
                        ? t("attachmentUpload.uploading", {
                              position: batchProgress.position,
                              total: batchProgress.count,
                          })
                        : t("addAttachment.upload", { count: attachmentsCount })}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
