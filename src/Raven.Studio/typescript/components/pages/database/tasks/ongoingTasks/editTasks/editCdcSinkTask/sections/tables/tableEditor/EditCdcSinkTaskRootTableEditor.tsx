import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { RootTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useFormContext, useWatch } from "react-hook-form";
import EditCdcSinkTaskAdvancedSettings from "./EditCdcSinkTaskAdvancedSettings";
import EditCdcSinkTaskFieldMapping from "./EditCdcSinkTaskFieldMapping";
import EditCdcSinkTaskOnDeleteFields from "./EditCdcSinkTaskOnDeleteFields";
import EditCdcSinkTaskPatchAdvancedField from "./EditCdcSinkTaskPatchAdvancedField";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function EditCdcSinkTaskRootTableEditor({ path }: { path: RootTablePath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const patch = useWatch({ control, name: `${path}.patch` });
    const ignoreDeletes = useWatch({ control, name: `${path}.onDelete.ignoreDeletes` });
    const deletePatch = useWatch({ control, name: `${path}.onDelete.patch` });

    const hasAdvancedValues = Boolean(patch || ignoreDeletes || deletePatch);

    return (
        <div>
            <div className="grid">
                <FormGroup className="g-col-4">
                    <FormLabel>{t("tableEditor.sourceSchema")}</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableSchema`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>{t("tableEditor.sourceTable")}</FormLabel>
                    <FormInput type="text" control={control} name={`${path}.sourceTableName`} />
                </FormGroup>
                <FormGroup className="g-col-4">
                    <FormLabel>
                        {t("tableEditor.root.targetCollection")}
                        <PopoverWithHoverWrapper message={t("tableEditor.root.targetCollectionInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.collectionName`} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        {t("tableEditor.primaryKeyColumns")}
                        <PopoverWithHoverWrapper message={t("tableEditor.root.primaryKeyColumnsInfo")}>
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
            <EditCdcSinkTaskFieldMapping path={path} />
            <EditCdcSinkTaskAdvancedSettings hasAdvancedValues={hasAdvancedValues}>
                <EditCdcSinkTaskPatchAdvancedField path={path} />
                <EditCdcSinkTaskOnDeleteFields path={path} />
            </EditCdcSinkTaskAdvancedSettings>
        </div>
    );
}
