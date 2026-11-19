import React from "react";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "components/common/Modal";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import { Switch } from "components/common/Checkbox";
import { FormGroup, FormLabel } from "components/common/Form";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsync, useAsyncCallback } from "react-async-hook";
import studioSettings from "common/settings/studioSettings";
import messagePublisher from "common/messagePublisher";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { TaskWithErrors, TransformationWithErrors, TaskCategory } from "../utils/tasksErrorsUtils";
import footer from "common/shell/footer";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type DeleteErrorsModalProps =
    | {
          mode: "task";
          toggle: () => void;
          onRefresh: () => void;
          etlName: string;
          etlType?: StudioEtlType;
          category?: TaskCategory;
          transformations: TransformationWithErrors[];
          errorsCount: number;
      }
    | {
          mode: "all";
          toggle: () => void;
          onRefresh: () => void;
          tasksWithErrors: TaskWithErrors[];
      };

export function DeleteErrorsModal(props: DeleteErrorsModalProps) {
    const { toggle, onRefresh, mode } = props;
    const t = useStudioTranslation("tasksErrors");
    const commonT = useStudioTranslation("common");
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { tasksService } = useServices();

    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteEtlErrors.getValue() ?? true;

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const asyncDeleteErrors = useAsyncCallback(async () => {
        const requestsPerTask: { type: TaskCategory; processNames: string[] }[] = [];

        const addTask = (etlName: string, category: TaskCategory | undefined, transformationNames: string[]) => {
            if (transformationNames.length === 0) {
                return;
            }
            requestsPerTask.push({
                type: category ?? "Etl",
                // CDC Sink tasks have no transformation, so their stored task name is the bare config
                // name - not the "task/transformation" form used by ETL/AI.
                processNames: transformationNames.map((name) => (name ? `${etlName}/${name}` : etlName)),
            });
        };

        if (mode === "task") {
            addTask(
                props.etlName,
                props.category,
                props.transformations.map((t) => t.transformationName)
            );
        } else {
            for (const task of props.tasksWithErrors) {
                addTask(
                    task.etlName,
                    task.category,
                    task.transformations.map((t) => t.transformationName)
                );
            }
        }

        try {
            const locations = DatabaseUtils.getLocations(db);
            await Promise.all(
                locations.flatMap((location) =>
                    requestsPerTask.map((req) =>
                        tasksService.deleteTaskErrors(db.name, {
                            type: req.type,
                            name: req.processNames,
                            nodeTag: location.nodeTag,
                            shardNumber: location.shardNumber,
                        })
                    )
                )
            );
            messagePublisher.reportSuccess(t("deleteModal.deleted"));
            footer.default.refreshStats();
            toggle();
            onRefresh();
        } catch (e) {
            messagePublisher.reportError(t("deleteModal.deleteFailed"), e?.responseText, e?.statusText);
        }
    });

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError(t("deleteModal.loadSettingsFailed"));
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteEtlErrors.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton onCloseClick={toggle} className="pb-0">
                <h3>
                    <Icon icon="trash" color="danger" />
                    <span>
                        {mode === "task"
                            ? t("deleteModal.taskTitle", { taskName: props.etlName })
                            : t("deleteModal.allTitle")}
                    </span>
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                {mode === "task" ? (
                    <>
                        <p>
                            <StudioTrans
                                ns="tasksErrors"
                                i18nKey="deleteModal.taskDescription"
                                components={{ b: <b /> }}
                                options={{ count: props.errorsCount, taskName: props.etlName }}
                            />
                        </p>
                        <RichAlert variant="info" icon="info">
                            <StudioTrans
                                ns="tasksErrors"
                                i18nKey="deleteModal.errorStateInfo"
                                components={{ b: <b /> }}
                            />
                        </RichAlert>
                    </>
                ) : (
                    <p>
                        <StudioTrans
                            ns="tasksErrors"
                            i18nKey="deleteModal.allDescription"
                            components={{ b: <b /> }}
                            options={{ count: props.tasksWithErrors.length }}
                        />
                    </p>
                )}
                {isRequireTypedConfirm && (
                    <FormGroup className="mt-3">
                        <FormLabel className="fw-bold">{t("deleteModal.typeToConfirm")}</FormLabel>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </FormGroup>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch selected={isRequireTypedConfirm} toggleSelection={toggleIsRequireTypedConfirm} color="primary">
                    {t("deleteModal.requireTypedConfirmation")}
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={toggle} className="link-muted">
                        {commonT("cancel")}
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={asyncDeleteErrors.loading}
                        variant="danger"
                        onClick={asyncDeleteErrors.execute}
                        className="rounded-pill"
                        disabled={!isConfirmed || asyncDeleteErrors.loading}
                    >
                        {commonT("delete")}
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}
