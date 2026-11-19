import {
    FormGroup,
    FormInput,
    FormLabel,
    FormSelect,
    FormSelectAutocomplete,
    FormSwitch,
} from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { useEditCdcSinkTaskSourceTableAutoFill } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskSourceTableAutoFill";
import {
    analyzeRootTables,
    getEmbeddedTableWarningMessagesFromAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import { EmbeddedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type CdcSinkRelationType = Raven.Client.Documents.Operations.CdcSink.CdcSinkRelationType;

export default function EditCdcSinkTaskEmbeddedTableEditor({ path }: { path: EmbeddedTablePath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const caseSensitiveKeys = useWatch({ control, name: `${path}.caseSensitiveKeys` });
    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });
    const embeddedTable = useWatch({ control, name: path });
    const rootTables = useWatch({ control, name: "tables" });
    const { handleSourceTableChange, sourceSchemaOptions, sourceTableOptions } = useEditCdcSinkTaskSourceTableAutoFill(
        path,
        "embedded"
    );

    const hasAdvancedValues = Boolean(caseSensitiveKeys || patch || ignoreDeletes || deletePatch);
    const rootTablesAnalysis = useMemo(() => analyzeRootTables(rootTables), [rootTables]);
    const warningMessages = useMemo(
        () => getEmbeddedTableWarningMessagesFromAnalysis(rootTablesAnalysis, embeddedTable),
        [rootTablesAnalysis, embeddedTable]
    );

    const relationTypeOptions: SelectOption<CdcSinkRelationType>[] = [
        { value: "Array", label: t("tableEditor.embedded.relationTypeArray") },
        { value: "Map", label: t("tableEditor.embedded.relationTypeMap") },
        { value: "Value", label: t("tableEditor.embedded.relationTypeValue") },
    ];

    return (
        <div>
            {warningMessages.map((warning) => (
                <RichAlert key={warning} variant="warning" className="mb-3">
                    {warning}
                </RichAlert>
            ))}
            <div className="grid mb-3">
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>{t("tableEditor.sourceSchema")}</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name={`${path}.sourceTableSchema`}
                        options={sourceSchemaOptions}
                        placeholder={t("tableEditor.sourceSchemaPlaceholder")}
                    />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>{t("tableEditor.sourceTable")}</FormLabel>
                    <FormSelectAutocomplete
                        control={control}
                        name={`${path}.sourceTableName`}
                        options={sourceTableOptions}
                        afterSelect={handleSourceTableChange}
                        placeholder={t("tableEditor.sourceTablePlaceholder")}
                    />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        {t("tableEditor.targetProperty")}
                        <PopoverWithHoverWrapper message={t("tableEditor.embedded.targetPropertyInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        {t("tableEditor.embedded.relationType")}
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="editCdcSinkTask"
                                    i18nKey="tableEditor.embedded.relationTypeInfo"
                                    components={{ br: <br />, strong: <strong /> }}
                                />
                            }
                        >
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormSelect control={control} name={`${path}.type`} options={relationTypeOptions} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        {t("tableEditor.primaryKeyColumns")}
                        <PopoverWithHoverWrapper message={t("tableEditor.embedded.primaryKeyColumnsInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel={t("tableEditor.addPrimaryKeyColumn")}
                control={control}
                name={`${path}.primaryKeyColumns`}
                fieldNameAccessor={(idx) => `${path}.primaryKeyColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <FormStringValueList
                title={
                    <>
                        {t("tableEditor.joinColumns")}
                        <PopoverWithHoverWrapper message={t("tableEditor.embedded.joinColumnsInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel={t("tableEditor.addJoinColumn")}
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
                className="mb-2"
            />
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <FormGroup>
                    <FormSwitch control={control} name={`${path}.caseSensitiveKeys`}>
                        {t("tableEditor.embedded.caseSensitiveKeys")}
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="editCdcSinkTask"
                                    i18nKey="tableEditor.embedded.caseSensitiveKeysInfo"
                                    components={{ strong: <strong /> }}
                                />
                            }
                        >
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                </FormGroup>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}
