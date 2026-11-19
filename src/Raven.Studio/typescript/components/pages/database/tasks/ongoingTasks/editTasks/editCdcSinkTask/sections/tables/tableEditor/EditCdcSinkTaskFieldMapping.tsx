import { EmptySet } from "components/common/EmptySet";
import ExpandableList from "components/common/ExpandableList";
import { FormErrorIcon, FormGroup, FormInput, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { SelectOption } from "components/common/select/Select";
import useBoolean from "components/hooks/useBoolean";
import {
    RootTablePath,
    EmbeddedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { useFieldArray, useFormContext } from "react-hook-form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type FieldMappingPath = RootTablePath | EmbeddedTablePath;

export default function EditCdcSinkTaskFieldMapping({ path }: { path: FieldMappingPath }) {
    const t = useStudioTranslation("editCdcSinkTask");
    const { control } = useFormContext<EditCdcSinkTaskFormData>();
    const isFieldMappingExpandedByDefault = useAppSelector(editCdcSinkTaskSelectors.isFieldMappingExpandedByDefault);
    const { value: isExpanded, setTrue: expand, setValue: setIsExpanded } = useBoolean(isFieldMappingExpandedByDefault);

    const columnsPath = `${path}.columns` as const;

    const columnsFieldArray = useFieldArray({
        control,
        name: columnsPath,
    });

    const columnTypeOptions: SelectOption<CdcColumnType>[] = [
        { value: "Default", label: t("fieldMapping.typeDefault") },
        { value: "Json", label: "JSON" },
        { value: "Attachment", label: t("fieldMapping.typeAttachment") },
    ];

    return (
        <>
            <div className="hstack justify-content-between mb-1">
                <div className="hstack">
                    {t("fieldMapping.title")}
                    <FormErrorIcon control={control} paths={[columnsPath]} />
                </div>
                <Button
                    variant="link"
                    size="sm"
                    onClick={() => {
                        columnsFieldArray.append({ column: "", name: "", type: "Default" });
                        expand();
                    }}
                >
                    <Icon icon="plus" />
                    {t("fieldMapping.add")}
                </Button>
            </div>
            {columnsFieldArray.fields.length === 0 ? (
                <div className="panel-bg-2 p-1 rounded border border-secondary hstack justify-content-center">
                    <EmptySet compact>{t("fieldMapping.empty")}</EmptySet>
                </div>
            ) : (
                <div>
                    <div className="field-mapping-row panel-bg-2 rounded-top p-1">
                        <div>{t("fieldMapping.sourceColumn")}</div>
                        <div></div>
                        <div>
                            {t("tableEditor.targetProperty")}
                            <PopoverWithHoverWrapper message={t("fieldMapping.targetPropertyInfo")}>
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <div>
                            {t("fieldMapping.type")}
                            <PopoverWithHoverWrapper
                                message={
                                    <StudioTrans
                                        ns="editCdcSinkTask"
                                        i18nKey="fieldMapping.typeInfo"
                                        components={{ br: <br />, strong: <strong /> }}
                                    />
                                }
                            >
                                <Icon icon="info-new" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <div></div>
                    </div>
                    <ExpandableList
                        className="bg-body rounded-bottom p-1"
                        contentClassName="vstack gap-1"
                        itemsCount={columnsFieldArray.fields.length}
                        collapsedItemsCount={6}
                        isExpanded={isExpanded}
                        setIsExpanded={setIsExpanded}
                    >
                        {({ visibleCount }) =>
                            columnsFieldArray.fields.slice(0, visibleCount).map((field, idx) => (
                                <div key={field.id} className="field-mapping-row">
                                    <FormGroup marginClass="m-0">
                                        <FormInput
                                            type="text"
                                            control={control}
                                            name={`${path}.columns.${idx}.column`}
                                        />
                                    </FormGroup>
                                    <Icon icon="arrow-thin-right" margin="m-0" />
                                    <FormGroup marginClass="m-0">
                                        <FormInput type="text" control={control} name={`${path}.columns.${idx}.name`} />
                                    </FormGroup>
                                    <FormSelect
                                        control={control}
                                        name={`${path}.columns.${idx}.type`}
                                        options={columnTypeOptions}
                                    />
                                    <Button
                                        variant="link"
                                        className="text-danger"
                                        size="sm"
                                        onClick={() => columnsFieldArray.remove(idx)}
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                </div>
                            ))
                        }
                    </ExpandableList>
                </div>
            )}
        </>
    );
}
