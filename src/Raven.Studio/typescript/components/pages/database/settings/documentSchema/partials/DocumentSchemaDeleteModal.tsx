import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import React from "react";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DocumentSchemaDeleteModalProps {
    collectionName?: string;
    selectedCollectionNames?: string[];
    onHide: () => void;
}

function getNamesToDelete(selectedCollectionNames?: string[], collectionName?: string): string[] {
    if (selectedCollectionNames && selectedCollectionNames.length > 0) {
        return selectedCollectionNames;
    }

    if (collectionName) {
        return [collectionName];
    }

    return [];
}

export default function DocumentSchemaDeleteModal({
    collectionName,
    selectedCollectionNames,
    onHide,
}: DocumentSchemaDeleteModalProps) {
    const t = useStudioTranslation("documentSchema");
    const { databasesService } = useServices();
    const dispatch = useAppDispatch();
    const validatorsAll = useAppSelector(documentSchemaSelectors.allValidators);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncSaveValidators = useAsyncCallback(async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    });

    const namesToDelete = getNamesToDelete(selectedCollectionNames, collectionName);

    const isBulk = namesToDelete.length > 1;

    const onDelete = async () => {
        const next = validatorsAll.filter((v) => !namesToDelete.includes(v.Name));
        await asyncSaveValidators.execute(next);
        if (isBulk) {
            dispatch(documentSchemaActions.selectedValidatorsDeleted());
        } else if (namesToDelete.length === 1) {
            dispatch(documentSchemaActions.validatorDeleted(namesToDelete[0]));
        }
        onHide();
    };

    return (
        <Modal show centered contentClassName="modal-border bulge-danger">
            <Modal.Header className="vstack gap-3 pb-0" onCloseClick={onHide}>
                <div className="text-center">
                    <Icon icon="warning" color="danger" margin="m-0" className="fs-1" />
                </div>
                <div className="text-center lead">
                    {isBulk ? (
                        <StudioTrans
                            ns="documentSchema"
                            i18nKey="deleteModal.titleBulk"
                            components={{ b: <b /> }}
                            options={{ count: namesToDelete.length }}
                        />
                    ) : (
                        <StudioTrans
                            ns="documentSchema"
                            i18nKey="deleteModal.title"
                            components={{ b: <b /> }}
                            options={{ name: namesToDelete[0] }}
                        />
                    )}
                </div>
            </Modal.Header>
            <Modal.Body className="pb-0 vstack gap-3">
                <div className="text-center">
                    {isBulk ? (
                        <>
                            {t("deleteModal.messageBulk")}
                            <br /> <p className="mt-3 mb-0">{t("deleteModal.confirmQuestion")}</p>
                        </>
                    ) : (
                        <>
                            {t("deleteModal.message", { name: namesToDelete[0] })} <br />{" "}
                            <p className="mt-3 mb-0">{t("deleteModal.confirmQuestion")}</p>
                        </>
                    )}
                </div>
            </Modal.Body>
            <Modal.Footer className="mt-4">
                <Button className="link-muted" variant="link" onClick={onHide} type="button">
                    {t("deleteModal.close")}
                </Button>
                <ButtonWithSpinner
                    className="rounded-pill"
                    variant="danger"
                    icon="trash"
                    isSpinning={asyncSaveValidators.loading}
                    onClick={onDelete}
                    title={isBulk ? t("deleteModal.deleteSchemas") : t("deleteModal.deleteSchema")}
                >
                    {isBulk ? t("deleteModal.deleteSchemas") : t("deleteModal.deleteSchema")}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
