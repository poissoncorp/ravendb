import { FormGroup, FormInput, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import RichAlert from "components/common/RichAlert";
import { useEditCdcSinkTaskSourceTableAutoFill } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/hooks/useEditCdcSinkTaskSourceTableAutoFill";
import EditCdcSinkTaskWarningMessage from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/partials/EditCdcSinkTaskWarningMessage";
import {
    analyzeRootTables,
    getLinkedTableWarningMessagesFromAnalysis,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTableWarnings";
import { LinkedTablePath } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function EditCdcSinkTaskLinkedTableEditor({ path }: { path: LinkedTablePath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const linkedTable = useWatch({ control, name: path });
    const rootTables = useWatch({ control, name: "tables" });
    const { handleSourceTableChange, sourceSchemaOptions, sourceTableOptions } = useEditCdcSinkTaskSourceTableAutoFill(
        path,
        "linked"
    );

    const rootTablesAnalysis = useMemo(() => analyzeRootTables(rootTables), [rootTables]);
    const warningMessages = useMemo(
        () => getLinkedTableWarningMessagesFromAnalysis(rootTablesAnalysis, linkedTable),
        [rootTablesAnalysis, linkedTable]
    );

    return (
        <div>
            {warningMessages.map((warning) => (
                <RichAlert key={warning} variant="warning" className="mb-3">
                    <EditCdcSinkTaskWarningMessage message={warning} />
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
                        <PopoverWithHoverWrapper message={t("tableEditor.linked.targetPropertyInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.propertyName`} />
                </FormGroup>
                <FormGroup className="g-col-6" marginClass="m-0">
                    <FormLabel>
                        {t("tableEditor.linked.linkedCollection")}
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="editCdcSinkTask"
                                    i18nKey="tableEditor.linked.linkedCollectionInfo"
                                    components={{ br: <br /> }}
                                />
                            }
                        >
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
                    <FormInput type="text" control={control} name={`${path}.linkedCollectionName`} />
                </FormGroup>
            </div>
            <FormStringValueList
                title={
                    <>
                        {t("tableEditor.joinColumns")}
                        <PopoverWithHoverWrapper message={t("tableEditor.linked.joinColumnsInfo")}>
                            <Icon icon="info-new" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </>
                }
                addButtonLabel={t("tableEditor.addJoinColumn")}
                control={control}
                name={`${path}.joinColumns`}
                fieldNameAccessor={(idx) => `${path}.joinColumns.${idx}.value`}
                defaultValue={{ value: "" }}
            />
        </div>
    );
}
