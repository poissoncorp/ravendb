import React from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";

import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { EditConflictResolutionSyntaxModal } from "components/pages/database/settings/conflictResolution/EditConflictResolutionSyntaxModal";
import { useAppDispatch, useAppSelector } from "components/store";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import useBoolean from "hooks/useBoolean";
import useUniqueId from "components/hooks/useUniqueId";
import genUtils from "common/generalUtils";
import {
    conflictResolutionActions,
    ConflictResolutionCollectionConfig,
    conflictResolutionSelectors,
} from "./store/conflictResolutionSlice";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { FormAceEditor, FormLabel, FormSelectCreatable } from "components/common/Form";
import { yupResolver } from "@hookform/resolvers/yup";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTranslate } from "common/i18n/resources";

interface ConflictResolutionConfigPanelProps {
    initialConfig: ConflictResolutionCollectionConfig;
}

export default function ConflictResolutionConfigPanel({ initialConfig }: ConflictResolutionConfigPanelProps) {
    const dispatch = useAppDispatch();
    const t = useStudioTranslation("conflictResolution");
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );
    const usedCollectionNames = useAppSelector(conflictResolutionSelectors.usedCollectionNames);
    const collectionOptions = allCollectionNames
        .filter((x) => !usedCollectionNames.includes(x))
        .map((x) => ({ label: x, value: x }));

    const { control, handleSubmit, reset } = useForm<FormData>({
        defaultValues: {
            collectionName: initialConfig.name,
            script: initialConfig.script,
        },
        resolver: yupResolver(getSchema(initialConfig.name, usedCollectionNames, t)),
    });

    const formValues = useWatch({ control });

    const { value: isSyntaxModalOpen, toggle: toggleIsSyntaxModalOpen } = useBoolean(false);

    const scriptPanelId = useUniqueId("scriptPanel");
    const unsavedChangesId = useUniqueId("unsavedChanges");
    const configId = initialConfig.id;

    const save: SubmitHandler<FormData> = (formData) => {
        dispatch(
            conflictResolutionActions.editSaved({
                id: configId,
                newConfig: {
                    name: formData.collectionName,
                    script: formData.script,
                },
            })
        );
    };

    return (
        <RichPanel className="flex-row" id={scriptPanelId}>
            <div className="flex-grow-1">
                <RichPanelHeader>
                    <RichPanelInfo>
                        <RichPanelName>
                            {formValues.collectionName || t("script.collectionNameFallback")}
                            {(initialConfig.isEdited || initialConfig.isNewUnsaved) && (
                                <OverlayTrigger
                                    overlay={<Tooltip id={unsavedChangesId}>{t("script.notSaved")}</Tooltip>}
                                >
                                    <span id={unsavedChangesId} className="text-warning d-inline-block">
                                        *
                                    </span>
                                </OverlayTrigger>
                            )}
                        </RichPanelName>
                    </RichPanelInfo>
                    <Form onSubmit={handleSubmit(save)}>
                        <RichPanelActions>
                            <PanelActions
                                reset={reset}
                                isInEditMode={initialConfig.isInEditMode}
                                isNewUnsaved={initialConfig.isNewUnsaved}
                                configId={configId}
                            />
                        </RichPanelActions>
                    </Form>
                </RichPanelHeader>
                <Collapse in={!initialConfig.isInEditMode}>
                    <div>
                        <RichPanelDetails>
                            <RichPanelDetailItem
                                label={
                                    <>
                                        <Icon icon="clock" />
                                        {t("script.lastModified")}
                                    </>
                                }
                            >
                                {initialConfig.lastModifiedTime
                                    ? genUtils.formatUtcDateAsLocal(initialConfig.lastModifiedTime)
                                    : t("script.newMark")}
                            </RichPanelDetailItem>
                        </RichPanelDetails>
                    </div>
                </Collapse>
                <Collapse in={initialConfig.isInEditMode}>
                    <div>
                        <RichPanelDetails className="vstack gap-3 p-3">
                            {!initialConfig.name && (
                                <InputGroup className="vstack mb-1">
                                    <FormLabel>{t("script.collectionLabel")}</FormLabel>
                                    <FormSelectCreatable
                                        control={control}
                                        name="collectionName"
                                        placeholder={t("script.collectionPlaceholder")}
                                        options={collectionOptions}
                                        isClearable={false}
                                        maxMenuHeight={300}
                                        isDisabled={!hasDatabaseAdminAccess}
                                    />
                                </InputGroup>
                            )}
                            <InputGroup className="vstack">
                                <FormLabel className="d-flex flex-wrap justify-content-between">
                                    {t("script.scriptLabel")}
                                    <Button
                                        variant="link"
                                        size="xs"
                                        onClick={toggleIsSyntaxModalOpen}
                                        className="p-0 align-self-end"
                                    >
                                        {t("script.syntaxButton")}
                                        <Icon icon="help" margin="ms-1" />
                                    </Button>
                                </FormLabel>
                                {isSyntaxModalOpen && (
                                    <EditConflictResolutionSyntaxModal toggle={toggleIsSyntaxModalOpen} />
                                )}
                                <FormAceEditor
                                    control={control}
                                    name="script"
                                    mode="javascript"
                                    height="400px"
                                    readOnly={!hasDatabaseAdminAccess}
                                />
                            </InputGroup>
                        </RichPanelDetails>
                    </div>
                </Collapse>
            </div>
        </RichPanel>
    );
}

function PanelActions({
    isInEditMode,
    isNewUnsaved,
    configId,
    reset,
}: {
    isInEditMode: boolean;
    isNewUnsaved: boolean;
    configId: string;
    reset: () => void;
}) {
    const dispatch = useAppDispatch();
    const t = useStudioTranslation("conflictResolution");
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const submitContext = isNewUnsaved ? "add" : "update";

    const discard = () => {
        dispatch(conflictResolutionActions.editDiscarded(configId));
        reset();
    };

    if (hasDatabaseAdminAccess) {
        if (isInEditMode) {
            return (
                <React.Fragment key="actions-in-edit">
                    <Button type="submit" variant="success" title={t("script.submitTitle", { context: submitContext })}>
                        <Icon icon="tick" margin="m-0" /> {t("script.submit", { context: submitContext })}
                    </Button>
                    <Button type="button" variant="secondary" title={t("script.discardTitle")} onClick={discard}>
                        <Icon icon="cancel" margin="m-0" /> {t("script.discard")}
                    </Button>
                </React.Fragment>
            );
        } else {
            return (
                <React.Fragment key="actions-not-in-edit">
                    <Button
                        type="button"
                        variant="secondary"
                        title={t("script.editTitle")}
                        onClick={() => dispatch(conflictResolutionActions.edited(configId))}
                    >
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button
                        type="button"
                        variant="danger"
                        title={t("script.deleteTitle")}
                        onClick={() => dispatch(conflictResolutionActions.deleted(configId))}
                    >
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </React.Fragment>
            );
        }
    }

    if (isInEditMode) {
        return (
            <Button
                type="button"
                variant="secondary"
                title={t("script.hideTitle")}
                onClick={() => dispatch(conflictResolutionActions.editDiscarded(configId))}
            >
                <Icon icon="preview-off" margin="m-0" />
            </Button>
        );
    } else {
        return (
            <Button
                type="button"
                variant="secondary"
                title={t("script.showTitle")}
                onClick={() => dispatch(conflictResolutionActions.edited(configId))}
            >
                <Icon icon="preview" margin="m-0" />
            </Button>
        );
    }
}

function getSchema(initialName: string, usedCollectionNames: string[], t: StudioTranslate<"conflictResolution">) {
    return yup.object({
        collectionName: yup
            .string()
            .required()
            .notOneOf(
                usedCollectionNames.filter((x) => x !== initialName),
                t("script.nameUsed")
            ),
        script: yup.string().required(),
    });
}

type FormData = yup.InferType<ReturnType<typeof getSchema>>;
