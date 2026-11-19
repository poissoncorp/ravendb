import React from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { Icon } from "components/common/Icon";
import { FormSwitch } from "components/common/Form";
import ImportSection from "./ImportSection";
import RestrictedSwitch from "./RestrictedSwitch";
import {
    connectionStringKeys,
    DatabaseSettingKey,
    databaseSettingKeys,
    ImportFromFileFormData,
    ongoingTaskKeys,
} from "../importFromFileValidation";
import { useImportRestrictions } from "../useImportRestrictions";
import { getTasksMissingConnectionStrings } from "../importFromFileUtils";
import { getLicenseLimitWarning } from "../importLicenseLimits";
import Card from "react-bootstrap/Card";
import { connectionStringLabels, databaseSettingLabels, ongoingTaskLabels } from "../importFromFileLabels";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function ConfigurationToImportSection() {
    const t = useStudioTranslation("importFromFile");
    const { control, setValue, resetField } = useFormContext<ImportFromFileFormData>();
    const {
        databaseSettings: settingRestrictions,
        ongoingTasks: ongoingTaskRestrictions,
        connectionStrings: connectionStringRestrictions,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys,
    } = useImportRestrictions();
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const settingLimitWarnings: Partial<Record<DatabaseSettingKey, string>> = {
        sorters: getLicenseLimitWarning(licenseStatus, "customSorters"),
        analyzers: getLicenseLimitWarning(licenseStatus, "customAnalyzers"),
    };

    const isIncludeTasks = useWatch({ control, name: "configuration.isIncludeConnectionStringsAndOngoingTasks" });
    const isCustomizeTasks = useWatch({ control, name: "configuration.isCustomizeOngoingTasks" });
    const isImportAllSettings = useWatch({ control, name: "configuration.isImportAllSettings" });
    const databaseSettings = useWatch({ control, name: "configuration.databaseSettings" });
    const isIncludeIndexes = useWatch({ control, name: "configuration.isIncludeIndexes" });
    const isIncludeIndexHistory = useWatch({ control, name: "configuration.isIncludeIndexHistory" });
    const isIncludeIdentities = useWatch({ control, name: "configuration.isIncludeIdentities" });

    const forceIndexesOn = (value: boolean) => {
        if (value) {
            setValue("configuration.isIncludeIndexes", true, { shouldDirty: true });
        }
    };

    const selectableSettingKeys = databaseSettingKeys.filter((key) => !settingRestrictions[key]);
    const areAllSettingsSelected = selectableSettingKeys.every((key) => databaseSettings[key]);

    const setAllSettings = (value: boolean) => {
        selectableSettingKeys.forEach((key) =>
            setValue(`configuration.databaseSettings.${key}`, value, { shouldDirty: true })
        );
    };

    const areAllEntitiesSelected = isIncludeIndexes && isIncludeIndexHistory && isIncludeIdentities && isIncludeTasks;

    const ongoingTasks = useWatch({ control, name: "configuration.ongoingTasks" });
    const connectionStrings = useWatch({ control, name: "configuration.connectionStrings" });

    const tasksMissingConnectionStrings = getTasksMissingConnectionStrings(
        {
            isIncludeConnectionStringsAndOngoingTasks: isIncludeTasks,
            isCustomizeOngoingTasks: isCustomizeTasks,
            ongoingTasks,
            connectionStrings,
        },
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    const selectableOngoingTaskKeys = ongoingTaskKeys.filter((key) => !ongoingTaskRestrictions[key]);
    const selectableConnectionStringKeys = connectionStringKeys.filter((key) => !connectionStringRestrictions[key]);

    const areAllOngoingTasksSelected =
        selectableOngoingTaskKeys.length > 0 && selectableOngoingTaskKeys.every((key) => ongoingTasks[key]);
    const areAllConnectionStringsSelected =
        selectableConnectionStringKeys.length > 0 &&
        selectableConnectionStringKeys.every((key) => connectionStrings[key]);

    const setAllOngoingTasks = (value: boolean) => {
        selectableOngoingTaskKeys.forEach((key) =>
            setValue(`configuration.ongoingTasks.${key}`, value, { shouldDirty: true })
        );
    };

    const setAllConnectionStrings = (value: boolean) => {
        selectableConnectionStringKeys.forEach((key) =>
            setValue(`configuration.connectionStrings.${key}`, value, { shouldDirty: true })
        );
    };

    const resetCustomizedTasksToDefault = () => {
        ongoingTaskKeys.forEach((key) => resetField(`configuration.ongoingTasks.${key}`));
        connectionStringKeys.forEach((key) => resetField(`configuration.connectionStrings.${key}`));
        setValue("configuration.isCustomizeOngoingTasks", false, { shouldDirty: true });
    };

    const setAllEntities = (value: boolean) => {
        setValue("configuration.isIncludeIndexes", value, { shouldDirty: true });
        setValue("configuration.isIncludeIndexHistory", value, { shouldDirty: true });
        setValue("configuration.isIncludeIdentities", value, { shouldDirty: true });
        setValue("configuration.isIncludeConnectionStringsAndOngoingTasks", value, { shouldDirty: true });
    };

    return (
        <ImportSection id="configuration-to-import" title={t("sections.configurationToImport")}>
            <div className="d-flex justify-content-between align-items-center mb-2">
                <h4 id="database-entities" className="m-0">
                    {t("configuration.selectEntities")}
                </h4>
                <Button variant="link" size="sm" onClick={() => setAllEntities(!areAllEntitiesSelected)}>
                    {areAllEntitiesSelected ? t("deselectAll") : t("selectAll")}
                </Button>
            </div>
            <div className="card p-4 mb-4">
                <FormSwitch control={control} name="configuration.isIncludeIndexes" className="pb-1">
                    {t("configuration.includeIndexes")}
                </FormSwitch>
                <div className="ms-4 d-flex flex-column gap-1">
                    <FormSwitch
                        control={control}
                        name="configuration.isIncludeIndexHistory"
                        afterChange={forceIndexesOn}
                    >
                        {t("configuration.includeIndexHistory")}
                    </FormSwitch>
                    <FormSwitch control={control} name="configuration.isRemoveAnalyzers" afterChange={forceIndexesOn}>
                        {t("configuration.removeAnalyzers")}
                    </FormSwitch>
                </div>
                <hr className="my-1" />
                <FormSwitch control={control} name="configuration.isIncludeIdentities">
                    {t("configuration.includeIdentities")}
                </FormSwitch>
                <hr className="my-1" />
                <div className="d-flex justify-content-between align-items-start">
                    <FormSwitch control={control} name="configuration.isIncludeConnectionStringsAndOngoingTasks">
                        {t("configuration.includeTasks")}
                    </FormSwitch>
                    <Button
                        variant="link"
                        size="sm"
                        disabled={!isIncludeTasks}
                        onClick={() =>
                            isCustomizeTasks
                                ? resetCustomizedTasksToDefault()
                                : setValue("configuration.isCustomizeOngoingTasks", true, { shouldDirty: true })
                        }
                    >
                        {isCustomizeTasks ? t("configuration.resetToDefault") : t("customize")}
                    </Button>
                </div>
                <Collapse in={isIncludeTasks && isCustomizeTasks}>
                    <div>
                        <div className="row mt-3">
                            <div className="col-md-6">
                                <div className="import-list-header mb-1">
                                    <span className="flex-grow-1 fw-semibold">{t("configuration.ongoingTasks")}</span>
                                    <div className="d-flex align-items-center gap-2">
                                        <span>{t("selectAll")}</span>
                                        <Form.Check
                                            type="switch"
                                            id="select-all-ongoing-tasks"
                                            aria-label={t("configuration.selectAllOngoingTasks")}
                                            label=""
                                            className="m-0"
                                            disabled={selectableOngoingTaskKeys.length === 0}
                                            checked={areAllOngoingTasksSelected}
                                            onChange={(e) => setAllOngoingTasks(e.target.checked)}
                                        />
                                    </div>
                                </div>
                                <div>
                                    {ongoingTaskKeys.map((key) => (
                                        <div key={key} className="import-list-item">
                                            <RestrictedSwitch
                                                control={control}
                                                name={`configuration.ongoingTasks.${key}`}
                                                restriction={ongoingTaskRestrictions[key]}
                                                warning={
                                                    tasksMissingConnectionStrings.includes(key)
                                                        ? t("configuration.missingConnectionString")
                                                        : undefined
                                                }
                                            >
                                                {ongoingTaskLabels[key]}
                                            </RestrictedSwitch>
                                        </div>
                                    ))}
                                </div>
                            </div>
                            <div className="col-md-6">
                                <div className="import-list-header mb-1">
                                    <span className="flex-grow-1 fw-semibold">
                                        {t("configuration.connectionStrings")}
                                    </span>
                                    <div className="d-flex align-items-center gap-2">
                                        <span>{t("selectAll")}</span>
                                        <Form.Check
                                            type="switch"
                                            id="select-all-connection-strings"
                                            aria-label={t("configuration.selectAllConnectionStrings")}
                                            label=""
                                            className="m-0"
                                            disabled={selectableConnectionStringKeys.length === 0}
                                            checked={areAllConnectionStringsSelected}
                                            onChange={(e) => setAllConnectionStrings(e.target.checked)}
                                        />
                                    </div>
                                </div>
                                <div>
                                    {connectionStringKeys.map((key) => (
                                        <div key={key} className="import-list-item">
                                            <RestrictedSwitch
                                                control={control}
                                                name={`configuration.connectionStrings.${key}`}
                                                restriction={connectionStringRestrictions[key]}
                                            >
                                                {connectionStringLabels[key]}
                                            </RestrictedSwitch>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                </Collapse>
                {isIncludeTasks && (
                    <Alert variant="info" className="mt-3 mb-0">
                        <Icon icon="info" color="info" /> {t("configuration.tasksDisabledInfo")}
                    </Alert>
                )}
            </div>

            <h4 id="database-settings" className="mb-2">
                {t("configuration.selectSettings")}
            </h4>
            <Card className="p-4">
                <div className="d-flex gap-2">
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: isImportAllSettings })}
                        onClick={() => setValue("configuration.isImportAllSettings", true, { shouldDirty: true })}
                    >
                        <Icon icon="database" margin="m-0" />
                        {t("configuration.importAllSettings")}
                    </button>
                    <button
                        type="button"
                        className={classNames("import-scope-btn", { active: !isImportAllSettings })}
                        onClick={() => setValue("configuration.isImportAllSettings", false, { shouldDirty: true })}
                    >
                        <Icon icon="settings" addon="edit" margin="m-0" />
                        {t("customize")}
                    </button>
                </div>
                {!isImportAllSettings && (
                    <div className="mt-4">
                        <div className="import-list-header mb-1">
                            <span className="flex-grow-1 fw-semibold">{t("configuration.settingName")}</span>
                            <div className="d-flex align-items-center gap-2">
                                <span>{t("selectAll")}</span>
                                <Form.Check
                                    type="switch"
                                    id="select-all-database-settings"
                                    aria-label={t("configuration.selectAllSettings")}
                                    label=""
                                    className="m-0"
                                    checked={areAllSettingsSelected}
                                    onChange={(e) => setAllSettings(e.target.checked)}
                                />
                            </div>
                        </div>
                        <div>
                            {databaseSettingKeys.map((key) => (
                                <div key={key} className="import-list-item">
                                    <RestrictedSwitch
                                        control={control}
                                        name={`configuration.databaseSettings.${key}`}
                                        restriction={settingRestrictions[key]}
                                        warning={settingLimitWarnings[key]}
                                    >
                                        {databaseSettingLabels[key]}
                                    </RestrictedSwitch>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </Card>
        </ImportSection>
    );
}
