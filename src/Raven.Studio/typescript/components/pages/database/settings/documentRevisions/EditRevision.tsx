import { Icon } from "components/common/Icon";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";

import { FormDurationPicker, FormInput, FormLabel, FormSelectCreatable, FormSwitch } from "components/common/Form";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    EditDocumentRevisionsCollectionConfig,
    documentRevisionsCollectionConfigYupResolver,
    documentRevisionsConfigYupResolver,
} from "components/pages/database/settings/documentRevisions/DocumentRevisionsValidation";
import { useDirtyFlag } from "hooks/useDirtyFlag";
import assertUnreachable from "components/utils/assertUnreachable";
import {
    DocumentRevisionsConfig,
    DocumentRevisionsConfigName,
    documentRevisionsConfigNames,
} from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import IconName from "typings/server/icons";
import { useAppSelector } from "components/store";
import { SelectOption } from "components/common/select/Select";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import genUtils from "common/generalUtils";
import generalUtils from "common/generalUtils";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import moment from "moment";
import RichAlert from "components/common/RichAlert";
import useEditRevisionFormSideEffects from "components/pages/database/settings/documentRevisions/useEditRevisionFormSideEffects";
import { useAppUrls } from "hooks/useAppUrls";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";

const revisionsDelta = 100;
const revisionsByAgeDelta = 604800; // 7 days
const minimumRevisionsToKeepDefaultValue = 1000;

export type EditRevisionConfigType = "collectionSpecific" | keyof typeof documentRevisionsConfigNames;
export type EditRevisionTaskType = "edit" | "new";

interface EditRevisionProps {
    toggle: () => void;
    onConfirm: (config: DocumentRevisionsConfig) => void;
    configType: EditRevisionConfigType;
    taskType: EditRevisionTaskType;
    config?: DocumentRevisionsConfig;
}

export default function EditRevision(props: EditRevisionProps) {
    const { toggle, configType, taskType, config, onConfirm } = props;
    const t = useStudioTranslation("documentRevisions");
    const commonT = useStudioTranslation("common");

    const isForNewCollection: boolean = configType === "collectionSpecific" && taskType === "new";

    const revisionsToKeepLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionsToKeep"));
    const revisionsAgeInDaysLimit = useAppSelector(licenseSelectors.statusValue("MaxNumberOfRevisionAgeToKeepInDays"));

    const originalConfig = useAppSelector(documentRevisionsSelectors.originalConfig(config?.Name));
    const collectionConfigsNames = useAppSelector(documentRevisionsSelectors.allConfigsNames);
    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames);

    const newCollectionOptions: SelectOption[] = allCollectionNames
        .filter((name) => !collectionConfigsNames.includes(name))
        .map((name) => ({
            label: name,
            value: name,
        }));

    const { control, formState, setValue, handleSubmit, watch } = useForm<EditDocumentRevisionsCollectionConfig>({
        resolver: isForNewCollection
            ? documentRevisionsCollectionConfigYupResolver
            : documentRevisionsConfigYupResolver,
        mode: "all",
        defaultValues: getInitialValues(config, revisionsToKeepLimit),
    });

    useEditRevisionFormSideEffects(watch, setValue);

    const formValues = useWatch({ control });
    useDirtyFlag(formState.isDirty);

    const onSubmit: SubmitHandler<EditDocumentRevisionsCollectionConfig> = (formData) => {
        onConfirm(mapToDocumentRevisionsConfig(formData, configType));
        toggle();
    };

    const formattedMinimumRevisionAgeToKeep = formValues.minimumRevisionAgeToKeep
        ? generalUtils.formatTimeSpan(formValues.minimumRevisionAgeToKeep * 1000, true)
        : null;

    const isRevisionsToKeepLimitTooLowWarning =
        originalConfig?.MinimumRevisionsToKeep != null &&
        formValues.minimumRevisionsToKeep != null &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        originalConfig.MinimumRevisionsToKeep - formValues.minimumRevisionsToKeep > revisionsDelta;

    const isRevisionsToKeepLimitNotSetWarning =
        !formValues.isMinimumRevisionsToKeepEnabled || !formValues.minimumRevisionsToKeep;

    const isRevisionsToKeepByAgeLimitWarning =
        originalConfig?.MinimumRevisionAgeToKeep != null &&
        formValues.minimumRevisionAgeToKeep != null &&
        !formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled &&
        genUtils.timeSpanToSeconds(originalConfig.MinimumRevisionAgeToKeep) - formValues.minimumRevisionAgeToKeep >
            revisionsByAgeDelta;

    const isDefaultConflicts = config?.Name === documentRevisionsConfigNames.defaultConflicts;

    const minimumRevisionAgeToKeepDays = moment.duration(formValues.minimumRevisionAgeToKeep, "seconds").asDays();

    const isLimitExceeded =
        !isDefaultConflicts &&
        ((revisionsAgeInDaysLimit > 0 && minimumRevisionAgeToKeepDays > revisionsAgeInDaysLimit) ||
            (revisionsToKeepLimit > 0 && formValues.minimumRevisionsToKeep > revisionsToKeepLimit));

    const { appUrl } = useAppUrls();
    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const isAgeRetentionActive =
        formValues.isMinimumRevisionAgeToKeepEnabled && formValues.minimumRevisionAgeToKeep > 0;

    return (
        <Modal size="lg" show onHide={toggle} contentClassName="modal-border bulge-info">
            <Form onSubmit={handleSubmit(onSubmit)} autoComplete="off">
                <Modal.Body className="vstack gap-3">
                    <h4>{getTitle(taskType, configType, t)}</h4>
                    {configType === "collectionSpecific" && (
                        <InputGroup className="gap-1 flex-wrap flex-column">
                            <FormLabel className="mb-0 md-label">{t("editModal.collectionLabel")}</FormLabel>
                            <FormSelectCreatable
                                placeholder={t("editModal.collectionPlaceholder")}
                                control={control}
                                name="collectionName"
                                options={
                                    isForNewCollection
                                        ? newCollectionOptions
                                        : [{ label: config.Name, value: config.Name }]
                                }
                                isDisabled={!isForNewCollection}
                                maxMenuHeight={300}
                            />
                        </InputGroup>
                    )}
                    <FormSwitch control={control} name="isPurgeOnDeleteEnabled">
                        {t("editModal.purgeOnDelete")}
                    </FormSwitch>
                    <FormSwitch control={control} name="isMinimumRevisionsToKeepEnabled">
                        <span>
                            {t("editModal.revisionsToKeepSwitch")}
                            <br />
                            <span className="text-muted">
                                {t("editModal.revisionsToKeepHint", {
                                    context: isAgeRetentionActive ? "minimum" : "maximum",
                                })}
                            </span>
                        </span>
                    </FormSwitch>
                    {formValues.isMinimumRevisionsToKeepEnabled && (
                        <div>
                            <InputGroup className="mb-2">
                                <FormInput
                                    type="number"
                                    control={control}
                                    name="minimumRevisionsToKeep"
                                    placeholder={t("editModal.revisionsToKeepPlaceholder")}
                                />
                                {isRevisionsToKeepLimitTooLowWarning && <LimitTooLowWarning limit={revisionsDelta} />}
                            </InputGroup>
                            {!isDefaultConflicts &&
                            revisionsToKeepLimit > 0 &&
                            formValues.minimumRevisionsToKeep > revisionsToKeepLimit ? (
                                <RichAlert variant="warning" className="mb-2">
                                    {t("editModal.revisionsToKeepLicenseLimit", { count: revisionsToKeepLimit })}
                                </RichAlert>
                            ) : null}
                        </div>
                    )}
                    <FormSwitch control={control} name="isMinimumRevisionAgeToKeepEnabled">
                        {t("editModal.revisionAgeSwitch")}
                    </FormSwitch>
                    {formValues.isMinimumRevisionAgeToKeepEnabled && (
                        <div className="mb-2">
                            <FormDurationPicker
                                control={control}
                                name="minimumRevisionAgeToKeep"
                                showDays
                                showSeconds
                            />
                            {!isDefaultConflicts &&
                            revisionsAgeInDaysLimit > 0 &&
                            minimumRevisionAgeToKeepDays > revisionsAgeInDaysLimit ? (
                                <RichAlert variant="warning" className="my-2">
                                    {t("editModal.retentionLicenseLimit", { count: revisionsAgeInDaysLimit })}
                                </RichAlert>
                            ) : null}
                            {isRevisionsToKeepByAgeLimitWarning && (
                                <LimitTooLowWarning
                                    limit={generalUtils.formatTimeSpan(revisionsByAgeDelta * 1000, true)}
                                />
                            )}
                        </div>
                    )}
                    {(formValues.isMinimumRevisionsToKeepEnabled || formValues.isMinimumRevisionAgeToKeepEnabled) && (
                        <>
                            <FormSwitch control={control} name="isMaximumRevisionsToDeleteUponDocumentUpdateEnabled">
                                {t("editModal.deleteUponUpdateSwitch")}
                            </FormSwitch>
                            {formValues.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled && (
                                <InputGroup className="mb-2">
                                    <FormInput
                                        type="number"
                                        control={control}
                                        name="maximumRevisionsToDeleteUponDocumentUpdate"
                                        placeholder={t("editModal.deleteUponUpdatePlaceholder")}
                                    />
                                </InputGroup>
                            )}
                        </>
                    )}
                    {isRevisionsToKeepLimitNotSetWarning && <LimitNotSetWarning />}
                    <RichAlert variant="primary" title={t("editModal.summaryTitle")} className="mt-2">
                        <ul className="m-0 ps-2 vstack gap-1">
                            <li>
                                {t("editModal.summary.createdItem", {
                                    context: formValues.isPurgeOnDeleteEnabled ? "purge" : "noPurge",
                                })}
                            </li>
                            {formValues.isPurgeOnDeleteEnabled ? (
                                <li>{t("editModal.summary.purgeItem")}</li>
                            ) : (
                                <li>
                                    <StudioTrans
                                        ns="documentRevisions"
                                        i18nKey="editModal.summary.revisionsBinItem"
                                        components={{
                                            a: <a href={appUrl.forRevisionsBin(activeDatabaseName)} target="_blank" />,
                                        }}
                                    />
                                </li>
                            )}
                            {formValues.minimumRevisionsToKeep > 0 && (
                                <>
                                    <li>
                                        {isAgeRetentionActive ? (
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="editModal.summary.keepAtLeast"
                                                options={{ count: formValues.minimumRevisionsToKeep }}
                                                components={{ strong: <strong /> }}
                                            />
                                        ) : (
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="editModal.summary.keepOnlyLatest"
                                                options={{ count: formValues.minimumRevisionsToKeep }}
                                                components={{ strong: <strong /> }}
                                            />
                                        )}
                                    </li>
                                    {isAgeRetentionActive ? (
                                        <li>
                                            <StudioTrans
                                                ns="documentRevisions"
                                                i18nKey="editModal.summary.olderRemovedByAge"
                                                options={{ duration: formattedMinimumRevisionAgeToKeep }}
                                                components={{ strong: <strong /> }}
                                            />
                                        </li>
                                    ) : (
                                        <li>{t("editModal.summary.olderRemoved")}</li>
                                    )}
                                </>
                            )}
                            {!formValues.minimumRevisionsToKeep && formValues.minimumRevisionAgeToKeep > 0 && (
                                <li>
                                    <StudioTrans
                                        ns="documentRevisions"
                                        i18nKey="editModal.summary.exceedingRemoved"
                                        options={{ duration: formattedMinimumRevisionAgeToKeep }}
                                        components={{ strong: <strong /> }}
                                    />
                                </li>
                            )}
                            {formValues.maximumRevisionsToDeleteUponDocumentUpdate > 0 && (
                                <li>
                                    <StudioTrans
                                        ns="documentRevisions"
                                        i18nKey="editModal.summary.deleteUponUpdate"
                                        options={{ count: formValues.maximumRevisionsToDeleteUponDocumentUpdate }}
                                        components={{ strong: <strong /> }}
                                    />
                                </li>
                            )}
                        </ul>
                    </RichAlert>
                </Modal.Body>
                <Modal.Footer>
                    <Button type="button" variant="link" className="link-muted" onClick={toggle}>
                        {commonT("cancel")}
                    </Button>
                    <Button
                        type="submit"
                        variant="success"
                        disabled={isLimitExceeded}
                        title={t("editModal.submitTitle")}
                    >
                        <Icon icon={getSubmitIcon(taskType)} />
                        {t("editModal.submit", { context: taskType })}
                    </Button>
                </Modal.Footer>
            </Form>
        </Modal>
    );
}

interface LimitTooLowWarningProps {
    limit: number | string;
}

function LimitTooLowWarning({ limit }: LimitTooLowWarningProps) {
    return (
        <RichAlert variant="warning" className="mt-1">
            <div>
                <StudioTrans
                    ns="documentRevisions"
                    i18nKey="editModal.limitTooLowWarning"
                    options={{ limit }}
                    components={{ br: <br /> }}
                />
            </div>
        </RichAlert>
    );
}

function LimitNotSetWarning() {
    return (
        <RichAlert variant="warning" className="mt-1">
            <div>
                <StudioTrans
                    ns="documentRevisions"
                    i18nKey="editModal.limitNotSetWarning"
                    components={{ br: <br /> }}
                />
            </div>
        </RichAlert>
    );
}

function getSubmitIcon(taskType: EditRevisionTaskType): IconName {
    switch (taskType) {
        case "new":
            return "plus";
        case "edit":
            return "edit";
        default:
            assertUnreachable(taskType);
    }
}

function getTitle(
    taskType: EditRevisionTaskType,
    configType: EditRevisionConfigType,
    t: StudioTranslate<"documentRevisions">
): string {
    switch (configType) {
        case "collectionSpecific":
            return t("editModal.titleCollectionSpecific", { context: taskType });
        case "defaultDocument":
            return t("editModal.titleDefaultDocument", { context: taskType });
        case "defaultConflicts":
            return t("editModal.titleDefaultConflicts", { context: taskType });
        default:
            assertUnreachable(configType);
    }
}

function getInitialValues(
    config: DocumentRevisionsConfig,
    licenseRevisionsToKeepLimit: number
): EditDocumentRevisionsCollectionConfig {
    if (!config) {
        const minimumRevisionsToKeep =
            licenseRevisionsToKeepLimit && licenseRevisionsToKeepLimit < minimumRevisionsToKeepDefaultValue
                ? licenseRevisionsToKeepLimit
                : minimumRevisionsToKeepDefaultValue;

        return {
            disabled: false,
            collectionName: null,
            isPurgeOnDeleteEnabled: false,
            isMinimumRevisionAgeToKeepEnabled: false,
            minimumRevisionAgeToKeep: null,
            isMinimumRevisionsToKeepEnabled: true,
            minimumRevisionsToKeep,
            isMaximumRevisionsToDeleteUponDocumentUpdateEnabled: false,
            maximumRevisionsToDeleteUponDocumentUpdate: null,
        };
    }

    return {
        disabled: config.Disabled,
        collectionName: config.Name,
        isPurgeOnDeleteEnabled: config.PurgeOnDelete,
        isMinimumRevisionAgeToKeepEnabled: config.MinimumRevisionAgeToKeep != null,
        minimumRevisionAgeToKeep: genUtils.timeSpanToSeconds(config.MinimumRevisionAgeToKeep),
        isMinimumRevisionsToKeepEnabled: config.MinimumRevisionsToKeep != null,
        minimumRevisionsToKeep: config.MinimumRevisionsToKeep,
        isMaximumRevisionsToDeleteUponDocumentUpdateEnabled: config.MaximumRevisionsToDeleteUponDocumentUpdate != null,
        maximumRevisionsToDeleteUponDocumentUpdate: config.MaximumRevisionsToDeleteUponDocumentUpdate,
    };
}

function mapToDocumentRevisionsConfig(
    formData: EditDocumentRevisionsCollectionConfig,
    configType: EditRevisionConfigType
): DocumentRevisionsConfig {
    let name: DocumentRevisionsConfigName = null;

    switch (configType) {
        case "collectionSpecific":
            name = formData.collectionName;
            break;
        case "defaultDocument":
            name = documentRevisionsConfigNames.defaultDocument;
            break;
        case "defaultConflicts":
            name = documentRevisionsConfigNames.defaultConflicts;
            break;
        default:
            assertUnreachable(configType);
    }

    return {
        Name: name,
        Disabled: formData.disabled,
        MaximumRevisionsToDeleteUponDocumentUpdate: formData.isMaximumRevisionsToDeleteUponDocumentUpdateEnabled
            ? formData.maximumRevisionsToDeleteUponDocumentUpdate
            : null,
        MinimumRevisionAgeToKeep: formData.isMinimumRevisionAgeToKeepEnabled
            ? genUtils.formatAsTimeSpan(formData.minimumRevisionAgeToKeep * 1000)
            : null,
        MinimumRevisionsToKeep: formData.isMinimumRevisionsToKeepEnabled ? formData.minimumRevisionsToKeep : null,
        PurgeOnDelete: formData.isPurgeOnDeleteEnabled,
    };
}
