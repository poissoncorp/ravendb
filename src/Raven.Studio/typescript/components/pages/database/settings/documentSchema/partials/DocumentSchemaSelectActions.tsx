import React, { useState } from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import { useAppSelector } from "components/store";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "hooks/useEventsCollector";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import useBoolean from "hooks/useBoolean";
import DocumentSchemaDeleteModal from "components/pages/database/settings/documentSchema/partials/DocumentSchemaDeleteModal";
import DocumentSchemaOperationConfirm, {
    DocumentSchemaOperationConfirmType,
} from "components/pages/database/settings/documentSchema/partials/DocumentSchemaOperationConfirm";
import { useServices } from "hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import useConfirm from "components/common/ConfirmDialog";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "hooks/useAppUrls";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { ValidationSchemaViewSheetPanel } from "components/pages/database/settings/documentSchema/partials/ValidationSchemaViewSheetPanel";
import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export interface OperationConfirm {
    type: DocumentSchemaOperationConfirmType;
    onConfirm: () => void;
    validators: DocumentSchemaValidatorConfig[];
}

export default function DocumentSchemaSelectActions() {
    const t = useStudioTranslation("documentSchema");
    const commonT = useStudioTranslation("common");
    const { forCurrentDatabase: urls } = useAppUrls();
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);
    const { value: isTogglingStatus, setTrue: setTogglingStatus, setFalse: unsetTogglingStatus } = useBoolean(false);
    const {
        value: isTogglingGlobalStatus,
        setTrue: setIsTogglingGlobalStatusTrue,
        setFalse: setIsTogglingGlobalStatusFalse,
    } = useBoolean(false);
    const confirm = useConfirm();
    const { open } = useViewSheet();
    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const allValidators = useAppSelector(documentSchemaSelectors.allValidators);
    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const selectedCollectionNames = useAppSelector(documentSchemaSelectors.selectedCollectionNames);
    const isGlobalDisabled = useAppSelector(documentSchemaSelectors.isGlobalDisabled);
    const selectionState = genUtils.getSelectionState(allCollectionNames, selectedCollectionNames);

    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

    const handleOpenSheet = () => {
        const validators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));
        open({
            component: <ValidationSchemaViewSheetPanel validators={validators} />,
        });
        dispatch(documentSchemaActions.selectedCollectionNamesCleared());
    };

    const toggleAll = () => {
        reportEvent("document-schema", "toggle-select-all");
        dispatch(documentSchemaActions.allSelectedCollectionNamesToggled());
    };

    const handleBulkStatusToggle = async (disabled: boolean) => {
        try {
            setTogglingStatus();
            reportEvent("document-schema", disabled ? "bulk-disable" : "bulk-enable");

            const selectedValidators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));
            const updatedValidators: DocumentSchemaValidatorConfig[] = selectedValidators.map((validator) => ({
                ...validator,
                Disabled: disabled,
            }));

            const allUpdatedValidators = allValidators.map((validator) => {
                const updatedValidator = updatedValidators.find((uv) => uv.Name === validator.Name);
                return updatedValidator || validator;
            });

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allUpdatedValidators, isGlobalDisabled)
            );

            updatedValidators.forEach((validator) => {
                dispatch(documentSchemaActions.validatorEdited({ originalName: validator.Name, validator }));
            });

            dispatch(documentSchemaActions.validatorsSaved());
        } finally {
            unsetTogglingStatus();
        }
    };

    const handleStatusOperation = (type: DocumentSchemaOperationConfirmType) => {
        const selectedValidators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));

        setOperationConfirm({
            type,
            onConfirm: () => handleBulkStatusToggle(type === "disable"),
            validators: selectedValidators,
        });
    };

    const handleGlobalStatusOperation = async (disabled: boolean) => {
        const isConfirmed = await confirm({
            title: t("globalConfirm.title", { context: disabled ? "disable" : "enable" }),
            message: (
                <p>
                    <StudioTrans
                        ns="documentSchema"
                        i18nKey="globalConfirm.message"
                        components={{ b: <b /> }}
                        options={{ context: disabled ? "disable" : "enable" }}
                    />
                </p>
            ),
            icon: disabled ? "stop" : "play",
            confirmText: disabled ? t("actions.disable") : t("actions.enable"),
            actionColor: disabled ? "danger" : "success",
        });

        if (!isConfirmed) {
            return;
        }

        try {
            setIsTogglingGlobalStatusTrue();
            reportEvent("document-schema", disabled ? "global-disable" : "global-enable");

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allValidators, disabled)
            );

            dispatch(documentSchemaActions.isGlobalDisabledToggled(disabled));
            dispatch(documentSchemaActions.validatorsSaved());
        } finally {
            setIsTogglingGlobalStatusFalse();
        }
    };

    return (
        <>
            <div
                className={classNames(
                    "position-relative d-flex w-100 align-items-center gap-2 mb-3",
                    allCollectionNames.length !== 0 ? "justify-content-between" : "justify-content-end"
                )}
            >
                {allCollectionNames.length !== 0 && (
                    <div>
                        <Checkbox
                            selected={selectionState === "AllSelected"}
                            indeterminate={selectionState === "SomeSelected"}
                            toggleSelection={toggleAll}
                            color="primary"
                            disabled={!hasSchemaValidation}
                            title={t("selectActions.selectAllTitle")}
                            size="lg"
                        >
                            <span className="small-label">{t("selectActions.selectAll")}</span>
                        </Checkbox>
                        <SelectionActions active={selectionState !== "Empty"}>
                            <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                                <div className="lead text-nowrap">
                                    <StudioTrans
                                        ns="documentSchema"
                                        i18nKey="selectActions.selected"
                                        components={{ strong: <strong className="text-emphasis me-1" /> }}
                                        options={{ count: selectedCollectionNames.length }}
                                    />
                                </div>
                                <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                                    <Button onClick={handleOpenSheet} className="rounded-pill flex-grow-0">
                                        <Icon icon="rocket" />
                                        {t("selectActions.runTest")}
                                    </Button>
                                    <Dropdown>
                                        <Dropdown.Toggle
                                            variant="secondary"
                                            disabled={selectionState === "Empty" || isTogglingStatus}
                                            title={t("selectActions.setStateTitle")}
                                            className="rounded-pill"
                                        >
                                            {isTogglingStatus ? <Spinner size="sm" /> : <Icon icon="play" />}{" "}
                                            {t("selectActions.setState")}
                                        </Dropdown.Toggle>
                                        <Dropdown.Menu>
                                            <Dropdown.Item
                                                title={t("actions.enable")}
                                                onClick={() => handleStatusOperation("enable")}
                                            >
                                                <Icon icon="play" color="success" />
                                                <span>{t("actions.enable")}</span>
                                            </Dropdown.Item>
                                            <Dropdown.Item
                                                title={t("actions.disable")}
                                                onClick={() => handleStatusOperation("disable")}
                                            >
                                                <Icon icon="stop" color="danger" />
                                                <span>{t("actions.disable")}</span>
                                            </Dropdown.Item>
                                        </Dropdown.Menu>
                                    </Dropdown>

                                    <Button
                                        variant="danger"
                                        onClick={toggleDeleteModal}
                                        className="rounded-pill flex-grow-0"
                                    >
                                        <Icon icon="trash" /> {commonT("delete")}
                                    </Button>
                                </ButtonGroup>
                                <Button
                                    onClick={() => dispatch(documentSchemaActions.allSelectedCollectionNamesToggled())}
                                    variant="link"
                                >
                                    {commonT("cancel")}
                                </Button>
                            </div>
                        </SelectionActions>
                    </div>
                )}
                <div className="d-flex gap-2 align-items-center">
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasSchemaValidation,
                            message: <FeatureNotAvailableInYourLicensePopoverBody />,
                        }}
                    >
                        <a
                            className={classNames("btn btn-secondary rounded-pill", {
                                disabled: !hasSchemaValidation,
                            })}
                            href={urls.documentSchemaPlayground()}
                            title={t("selectActions.playgroundTitle")}
                        >
                            <Icon icon="rocket" />
                            {t("selectActions.playground")}
                        </a>
                    </ConditionalPopover>
                    {allCollectionNames.length !== 0 && (
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasSchemaValidation,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <ButtonWithSpinner
                                variant={isGlobalDisabled ? "success" : "secondary"}
                                onClick={() => handleGlobalStatusOperation(!isGlobalDisabled)}
                                isSpinning={isTogglingGlobalStatus}
                                disabled={!hasSchemaValidation}
                                className="rounded-pill"
                            >
                                <Icon icon={isGlobalDisabled ? "play" : "stop"} />
                                {t("selectActions.globalToggle", { context: isGlobalDisabled ? "enable" : "disable" })}
                            </ButtonWithSpinner>
                        </ConditionalPopover>
                    )}
                </div>
            </div>
            {isDeleteModalOpen && (
                <DocumentSchemaDeleteModal
                    selectedCollectionNames={selectedCollectionNames}
                    onHide={toggleDeleteModal}
                />
            )}
            {operationConfirm && (
                <DocumentSchemaOperationConfirm {...operationConfirm} toggle={() => setOperationConfirm(null)} />
            )}
        </>
    );
}
