import React from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Switch } from "components/common/Checkbox";
import { FormGroup, FormLabel } from "components/common/Form";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import studioSettings from "common/settings/studioSettings";
import messagePublisher from "common/messagePublisher";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DeleteIndexErrorsModalProps {
    location: databaseLocationSpecifier;
    selectedIndexNames: string[];
    isDeleting: boolean;
    onConfirm: () => void;
    onClose: () => void;
}

export default function DeleteIndexErrorsModal({
    location,
    selectedIndexNames,
    isDeleting,
    onConfirm,
    onClose,
}: DeleteIndexErrorsModalProps) {
    const t = useStudioTranslation("indexErrors");
    const commonT = useStudioTranslation("common");
    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteIndexErrors.getValue() ?? true;

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError(t("deleteModal.loadSettingsFailed"));
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteIndexErrors.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-0">
                <h3>
                    <Icon icon="trash" color="danger" />
                    <span>
                        <StudioTrans
                            ns="indexErrors"
                            i18nKey="deleteModal.title"
                            components={{
                                nodeIcon: <Icon icon="node" color="node" margin="m-0" />,
                                shardIcon: <Icon icon="shard" color="shard" margin="m-0" />,
                                strong: <strong />,
                            }}
                            options={{
                                nodeTag: location.nodeTag,
                                shardNumber: location.shardNumber,
                                context: location.shardNumber != null ? "shard" : undefined,
                            }}
                        />
                    </span>
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <SelectedIndexesInfo selectedIndexNames={selectedIndexNames} />
                <RichAlert variant="info" className="mt-3">
                    <StudioTrans ns="indexErrors" i18nKey="deleteModal.info" components={{ b: <b /> }} />
                </RichAlert>
                {isRequireTypedConfirm && (
                    <FormGroup className="mt-3">
                        <FormLabel className="fw-bold">{t("deleteModal.typeToConfirm")}</FormLabel>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </FormGroup>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch
                    selected={isRequireTypedConfirm}
                    toggleSelection={toggleIsRequireTypedConfirm}
                    disabled={!asyncGlobalSettings.result}
                    color="primary"
                >
                    {t("deleteModal.requireTypedConfirmation")}
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={onClose} className="link-muted">
                        {commonT("cancel")}
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={isDeleting}
                        variant="danger"
                        onClick={onConfirm}
                        className="rounded-pill"
                        disabled={!isConfirmed || isDeleting}
                    >
                        {commonT("delete")}
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

interface SelectedIndexesInfoProps {
    selectedIndexNames: string[];
}

function SelectedIndexesInfo({ selectedIndexNames }: SelectedIndexesInfoProps) {
    if (selectedIndexNames.length === 0) {
        return (
            <p>
                <StudioTrans
                    ns="indexErrors"
                    i18nKey="deleteModal.allIndexes"
                    components={{ strong: <strong />, br: <br />, b: <b /> }}
                />
            </p>
        );
    }

    return (
        <div>
            <StudioTrans
                ns="indexErrors"
                i18nKey="deleteModal.selectedIndexes"
                components={{ b: <b /> }}
                options={{ count: selectedIndexNames.length }}
            />
            <ul>
                {selectedIndexNames.map((indexName) => (
                    <li key={indexName} title={indexName} className="text-truncate">
                        <b>{indexName}</b>
                    </li>
                ))}
            </ul>
        </div>
    );
}
