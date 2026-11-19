import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import React from "react";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { useAsync } from "react-async-hook";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import notificationCenter from "common/notifications/notificationCenter";
import genUtils from "common/generalUtils";
import studioSettings from "common/settings/studioSettings";
import { Switch } from "components/common/Checkbox";
import useBoolean from "hooks/useBoolean";
import { useIsMounted } from "components/hooks/useIsMounted";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DeleteDocumentsModalProps {
    close: () => void;
    collectionName: string;
    collectionDocumentCount: number;
    isAllDocuments: boolean;
    excludedIds: string[];
    selectedCount: number;
    onDeleteStarted: () => void;
    onDeleteCompleted: () => void;
    onCollectionDeletionStarted: (collectionName: string) => void;
    onCollectionDeletionFailed: (collectionName: string) => void;
}

export default function DeleteDocumentsModal({
    close,
    collectionName,
    collectionDocumentCount,
    isAllDocuments,
    excludedIds,
    selectedCount,
    onDeleteStarted,
    onDeleteCompleted,
    onCollectionDeletionStarted,
    onCollectionDeletionFailed,
}: DeleteDocumentsModalProps) {
    const t = useStudioTranslation("documentsList");
    const commonT = useStudioTranslation("common");
    // Note: wrapped in function to avoid type error (JQueryPromise<globalSettings>)
    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteDocuments.getValue() ?? true;

    const isSelectedAll = selectedCount === collectionDocumentCount;
    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const deleteCollection = useDeleteCollection({
        close,
        collectionName,
        collectionDocumentCount,
        isAllDocuments,
        excludedIds,
        selectedCount,
        onDeleteStarted,
        onDeleteCompleted,
        onCollectionDeletionStarted,
        onCollectionDeletionFailed,
    });

    const onConfirm = () => {
        if (!isConfirmed) {
            return;
        }
        deleteCollection.execute();
    };

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError(t("deleteModal.loadSettingsFailed"));
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteDocuments.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4" onCloseClick={close}>
                <div className="text-center">
                    <Icon icon="trash" color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    {t("deleteModal.title", { context: isSelectedAll ? "all" : "selected" })}
                </div>
            </Modal.Header>
            <Modal.Body>
                <CollectionsInfo
                    collectionName={collectionName}
                    isAllDocuments={isAllDocuments}
                    selectedCount={selectedCount}
                    isSelectedAll={isSelectedAll}
                />
                {isRequireTypedConfirm && (
                    <Form.Group>
                        <Form.Label className="fw-bold">{t("deleteModal.typeToConfirm")}</Form.Label>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </Form.Group>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch selected={isRequireTypedConfirm} toggleSelection={toggleIsRequireTypedConfirm} color="primary">
                    {t("deleteModal.requireTypedConfirmation")}
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={close} className="link-muted">
                        {commonT("cancel")}
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={deleteCollection.loading}
                        variant="danger"
                        onClick={onConfirm}
                        className="rounded-pill"
                        disabled={!isConfirmed}
                    >
                        {commonT("delete")}
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

function useDeleteCollection({
    close,
    collectionName,
    isAllDocuments,
    excludedIds,
    selectedCount: documentCount,
    onDeleteStarted,
    onDeleteCompleted,
    onCollectionDeletionStarted,
    onCollectionDeletionFailed,
}: DeleteDocumentsModalProps) {
    const t = useStudioTranslation("documentsList");
    const { databasesService } = useServices();
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { value: isLoading, setValue: setIsLoading } = useBoolean(false);
    const isMounted = useIsMounted();

    const execute = () => {
        setIsLoading(true);
        const collectionNameForApi = isAllDocuments ? "@all_docs" : collectionName;
        const isCollectionRemoval = excludedIds.length === 0 && !isAllDocuments;

        // This is JQueryPromise. Use 'done' to prevent wrong order of execution in event loop
        databasesService
            .deleteCollection(collectionNameForApi, dbName, excludedIds)
            .done((result) => {
                const operationId = result.OperationId;
                onDeleteStarted();

                if (isCollectionRemoval) {
                    onCollectionDeletionStarted(collectionName);
                }

                notificationCenter.instance.openDetailsForOperationById(dbName, operationId);

                notificationCenter.instance
                    .monitorOperation(dbName, operationId)
                    .done(() => {
                        if (excludedIds.length === 0) {
                            messagePublisher.reportSuccess(t("deleteModal.collectionDeleted", { collectionName }));
                        } else {
                            messagePublisher.reportSuccess(
                                t("deleteModal.documentsDeleted", {
                                    count: documentCount,
                                    formattedCount: documentCount.toLocaleString(),
                                    collectionName,
                                })
                            );
                        }
                    })
                    .fail(() => {
                        if (isCollectionRemoval) {
                            onCollectionDeletionFailed(collectionName);
                        }
                    })
                    .always(() => {
                        onDeleteCompleted();
                    });

                close();
            })
            .always(() => {
                if (isMounted()) {
                    setIsLoading(false);
                }
            });
    };

    return {
        execute,
        loading: isLoading,
    };
}

interface CollectionsInfoProps {
    collectionName: string;
    isAllDocuments: boolean;
    selectedCount: number;
    isSelectedAll: boolean;
}

function CollectionsInfo({ collectionName, isAllDocuments, selectedCount, isSelectedAll }: CollectionsInfoProps) {
    const allCollectionsContext = isSelectedAll ? "allFromAllCollections" : "selectedFromAllCollections";
    const collectionContext = isSelectedAll ? "allFromCollection" : "selectedFromCollection";

    return (
        <p>
            <StudioTrans
                ns="documentsList"
                i18nKey="deleteModal.info"
                components={{ b: isAllDocuments ? <b className="text-uppercase" /> : <b /> }}
                options={{
                    context: isAllDocuments ? allCollectionsContext : collectionContext,
                    count: selectedCount,
                    formattedCount: genUtils.formatNumberToStringFixed(selectedCount, 0),
                    collectionName,
                }}
            />
        </p>
    );
}
