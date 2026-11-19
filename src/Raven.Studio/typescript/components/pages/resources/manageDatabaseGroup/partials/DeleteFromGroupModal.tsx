import React, { ReactNode } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useAsync, useAsyncCallback } from "react-async-hook";
import appUrl from "common/appUrl";
import messagePublisher from "common/messagePublisher";
import studioSettings from "common/settings/studioSettings";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Switch } from "components/common/Checkbox";
import { FormGroup, FormLabel } from "components/common/Form";
import { ThemeColor } from "components/models/common";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import { useServices } from "hooks/useServices";
import IconName from "typings/server/icons";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export interface NodeDeletion {
    nodeTag: string;
    isHardDelete: boolean;
}

interface DeleteFromGroupModalProps {
    db: DatabaseSharedInfo;
    deletion: NodeDeletion;
    onHardDeleteInstead: () => void;
    onClose: () => void;
}

export function DeleteFromGroupModal({ db, deletion, onHardDeleteInstead, onClose }: DeleteFromGroupModalProps) {
    const t = useStudioTranslation("manageDatabaseGroup");
    const commonT = useStudioTranslation("common");
    const { nodeTag, isHardDelete } = deletion;
    const { databasesService } = useServices();

    const isLastNode = !DatabaseUtils.isSharded(db.name) && db.nodes.length === 1;
    const canRequireTypedConfirm = !isHardDelete || isLastNode;

    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        canRequireTypedConfirm &&
        (asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteFromGroup.getValue() ?? true);

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError(t("deleteModal.loadSettingsError"));
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteFromGroup.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    const asyncDelete = useAsyncCallback(
        () => databasesService.deleteDatabaseFromNode(db.name, [nodeTag], isHardDelete),
        { onSuccess: onClose }
    );

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4 pb-0" onCloseClick={onClose}>
                <div className="text-center">
                    <Icon
                        icon={isHardDelete || isLastNode ? "trash" : "disconnected"}
                        color="danger"
                        className="fs-1"
                        margin="m-0"
                    />
                </div>
                <div className="text-center lead">
                    <DeleteFromGroupTitle db={db} deletion={deletion} isLastNode={isLastNode} />
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-3">
                <div className="vstack gap-3 border border-secondary rounded p-3">
                    <ul className="list-unstyled vstack gap-2 mb-0">
                        <DeleteFromGroupConsequences
                            db={db}
                            deletion={deletion}
                            isLastNode={isLastNode}
                            onHardDeleteInstead={onHardDeleteInstead}
                        />
                    </ul>
                    <hr className="m-0" />
                    <CopyDataHint databaseName={DatabaseUtils.shardGroupKey(db.name)} onNavigate={onClose} />
                </div>
                {isRequireTypedConfirm && (
                    <FormGroup marginClass="mb-0">
                        <FormLabel className="fw-bold">{t("deleteModal.typedConfirmLabel")}</FormLabel>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </FormGroup>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                {canRequireTypedConfirm && (
                    <Switch
                        selected={isRequireTypedConfirm}
                        toggleSelection={toggleIsRequireTypedConfirm}
                        disabled={!asyncGlobalSettings.result}
                        color="primary"
                    >
                        {t("deleteModal.requireTypedConfirm")}
                    </Switch>
                )}
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={onClose} className="link-muted">
                        {commonT("cancel")}
                    </Button>
                    <ButtonWithSpinner
                        variant="danger"
                        className="rounded-pill"
                        onClick={() => asyncDelete.execute()}
                        isSpinning={asyncDelete.loading}
                        disabled={!isConfirmed}
                    >
                        {t("deleteModal.deleteButton", { context: isHardDelete ? "hard" : "soft" })}
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

interface DeleteFromGroupTitleProps {
    db: DatabaseSharedInfo;
    deletion: NodeDeletion;
    isLastNode: boolean;
}

function DeleteFromGroupTitle({ db, deletion, isLastNode }: DeleteFromGroupTitleProps) {
    if (isLastNode) {
        return (
            <StudioTrans
                ns="manageDatabaseGroup"
                i18nKey="deleteModal.lastNodeTitle"
                components={{ strong: <strong /> }}
                options={{ databaseName: db.name }}
            />
        );
    }

    const context = deletion.isHardDelete ? "hard" : "soft";

    if (DatabaseUtils.isSharded(db.name)) {
        return (
            <StudioTrans
                ns="manageDatabaseGroup"
                i18nKey="deleteModal.shardTitle"
                components={{ strong: <strong /> }}
                options={{ shardNumber: DatabaseUtils.shardNumber(db.name), nodeTag: deletion.nodeTag, context }}
            />
        );
    }

    return (
        <StudioTrans
            ns="manageDatabaseGroup"
            i18nKey="deleteModal.databaseTitle"
            components={{ strong: <strong /> }}
            options={{ databaseName: db.name, nodeTag: deletion.nodeTag, context }}
        />
    );
}

interface DeleteFromGroupConsequencesProps {
    db: DatabaseSharedInfo;
    deletion: NodeDeletion;
    isLastNode: boolean;
    onHardDeleteInstead: () => void;
}

function DeleteFromGroupConsequences({
    db,
    deletion,
    isLastNode,
    onHardDeleteInstead,
}: DeleteFromGroupConsequencesProps) {
    const t = useStudioTranslation("manageDatabaseGroup");
    const { nodeTag, isHardDelete } = deletion;

    if (isLastNode) {
        return (
            <>
                <Consequence icon="warning" iconColor="danger">
                    <span>
                        <StudioTrans
                            ns="manageDatabaseGroup"
                            i18nKey="deleteModal.lastNodeDeletesDatabase"
                            components={{ strong: <strong /> }}
                            options={{ nodeTag, databaseName: db.name }}
                        />
                    </span>
                </Consequence>
                {isHardDelete ? (
                    <Consequence icon="trash" iconColor="danger">
                        <strong>{t("deleteModal.lastNodeFilesDeleted", { nodeTag })}</strong>
                    </Consequence>
                ) : (
                    <Consequence icon="storage">
                        <span>{t("deleteModal.lastNodeFilesKept", { nodeTag })}</span>
                    </Consequence>
                )}
            </>
        );
    }

    return (
        <>
            <Consequence icon="disconnected">
                <span>
                    <StudioTrans
                        ns="manageDatabaseGroup"
                        i18nKey="deleteModal.leavesGroup"
                        components={{ strong: <strong /> }}
                        options={{ nodeTag }}
                    />
                </span>
            </Consequence>
            {isHardDelete ? (
                <Consequence icon="trash">
                    <span>{t("deleteModal.filesDeleted", { nodeTag })}</span>
                </Consequence>
            ) : (
                <>
                    <Consequence icon="storage">
                        <span>{t("deleteModal.filesKept", { nodeTag })}</span>
                    </Consequence>
                    <Consequence icon="warning" iconColor="danger">
                        <div className="vstack gap-2 align-items-start">
                            <span>
                                <StudioTrans
                                    ns="manageDatabaseGroup"
                                    i18nKey="deleteModal.tombstonesWarning"
                                    components={{ strong: <strong /> }}
                                    options={{ nodeTag }}
                                />
                            </span>
                            <Button
                                variant="secondary"
                                size="sm"
                                className="rounded-pill"
                                onClick={onHardDeleteInstead}
                            >
                                {t("deleteModal.useHardDeleteInstead")}
                                <Icon icon="arrow-thin-right" margin="ms-1" />
                            </Button>
                        </div>
                    </Consequence>
                </>
            )}
        </>
    );
}

interface ConsequenceProps {
    icon: IconName;
    iconColor?: ThemeColor;
    children: ReactNode;
}

function Consequence({ icon, iconColor, children }: ConsequenceProps) {
    return (
        <li className="hstack gap-2 align-items-start">
            <span className="d-flex align-items-center" style={{ height: "1lh" }}>
                <Icon icon={icon} color={iconColor} margin="m-0" />
            </span>
            {children}
        </li>
    );
}

interface CopyDataHintProps {
    databaseName: string;
    onNavigate: () => void;
}

function CopyDataHint({ databaseName, onNavigate }: CopyDataHintProps) {
    return (
        <p className="small text-muted mb-0">
            <StudioTrans
                ns="manageDatabaseGroup"
                i18nKey="deleteModal.copyDataHint"
                components={{
                    backup: <a href={appUrl.forBackups(databaseName)} onClick={onNavigate} />,
                    export: <a href={appUrl.forExportDatabase(databaseName)} onClick={onNavigate} />,
                }}
            />
        </p>
    );
}
