import { FormInput, FormSwitch, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData, GenAiStartingPoint } from "../../utils/editGenAiTaskValidation";
import TaskUtils from "components/utils/TaskUtils";
import OptionalLabel from "components/common/OptionalLabel";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import tasksCommonContent from "models/database/tasks/tasksCommonContent";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";
import { FormTaskConnectionString } from "components/common/formFields/FormTaskConnectionString";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditGenAiTaskBasicFields() {
    const t = useStudioTranslation("editGenAiTask");
    const isEncrypted = useAppSelector(databaseSelectors.activeDatabase)?.isEncrypted;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

    const stateOptions: SelectOption<OngoingTaskState>[] = [
        { label: t("basic.stateEnabled"), value: "Enabled" },
        { label: t("basic.stateDisabled"), value: "Disabled" },
    ];

    const startingPointTypeOptions: SelectOption<GenAiStartingPoint>[] = [
        { label: t("basic.startingPointBeginningOfTime"), value: "Beginning of Time" },
        { label: t("basic.startingPointLatestDocument"), value: "Latest Document" },
        { label: "Change Vector", value: "Change Vector" },
    ];

    return (
        <>
            <FormGroup>
                <FormLabel>{t("basic.taskName")}</FormLabel>
                <ConditionalPopover
                    conditions={{
                        isActive: isEditTask,
                        message: tasksCommonContent.taskNameLocked,
                    }}
                    className="w-100 flex-column"
                >
                    <FormInput
                        type="text"
                        control={control}
                        name="name"
                        placeholder={t("basic.taskNamePlaceholder")}
                        onBlur={() => {
                            if (!formValues.identifier) {
                                handleGenerateIdentifier();
                            }
                        }}
                        disabled={isEditTask}
                    />
                </ConditionalPopover>
            </FormGroup>
            <FormGroup>
                <FormLabel>
                    {t("basic.identifier")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="basic.identifierInfo"
                                components={{ br: <br /> }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" id="identifier" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput
                    control={control}
                    name="identifier"
                    type="text"
                    placeholder="my-task"
                    addon={
                        <Button
                            variant="link"
                            className="text-reset px-0"
                            onClick={handleGenerateIdentifier}
                            title={t("basic.regenerateIdentifierTitle")}
                        >
                            <Icon icon="refresh" />
                            {t("basic.regenerateIdentifier")}
                        </Button>
                    }
                />
            </FormGroup>
            <FormGroup>
                <FormLabel>{t("basic.taskState")}</FormLabel>
                <FormSelect control={control} name="state" options={stateOptions} />
            </FormGroup>
            {isEncrypted && (
                <div className="vstack gap-2">
                    <RichAlert variant="info">
                        <StudioTrans
                            ns="editGenAiTask"
                            i18nKey="basic.encryptedDatabase"
                            components={{ strong: <strong /> }}
                            options={{ databaseName }}
                        />
                    </RichAlert>
                    <FormGroup>
                        <FormSwitch control={control} name="isAllowEtlOnNonEncryptedChannel">
                            {t("basic.allowNonEncryptedChannel")}
                        </FormSwitch>
                    </FormGroup>
                </div>
            )}
            <FormTaskResponsibleNode
                control={control}
                isSetName="isSetResponsibleNode"
                nodeName="responsibleNode"
                isPinName="isPinResponsibleNode"
            />
            <FormTaskConnectionString control={control} name="connectionStringName" type="Ai" modelType="Chat" />
            <FormGroup>
                <FormLabel>
                    {t("basic.maxConcurrency")} <OptionalLabel />
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="editGenAiTask"
                                i18nKey="basic.maxConcurrencyInfo"
                                components={{ br: <br /> }}
                                options={{ defaultMaxConcurrency: editGenAiTaskUtils.defaultMaxConcurrency }}
                            />
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormInput type="number" control={control} name="maxConcurrency" />
            </FormGroup>
            <FormGroup>
                <FormSwitch control={control} name="isStartingPoint">
                    {t("basic.useStartingPoint")}
                </FormSwitch>
            </FormGroup>
            {formValues.isStartingPoint && (
                <FormGroup>
                    <FormLabel>{t("basic.sendDocumentsFrom")}</FormLabel>
                    <FormSelect control={control} name="startingPointType" options={startingPointTypeOptions} />
                </FormGroup>
            )}
            {formValues.isStartingPoint && formValues.startingPointType === "Change Vector" && (
                <FormGroup>
                    <FormLabel>Change Vector</FormLabel>
                    <FormInput
                        type="textarea"
                        as="textarea"
                        rows={3}
                        control={control}
                        name="startingPointChangeVector"
                        placeholder={t("basic.changeVectorPlaceholder")}
                    />
                </FormGroup>
            )}
            {formValues.nextBatchStartingPoint && (
                <FormGroup>
                    <FormLabel>
                        <StudioTrans
                            ns="editGenAiTask"
                            i18nKey="basic.nextBatchStartingPoint"
                            components={{ small: <small className="text-muted fw-light" /> }}
                        />
                    </FormLabel>
                    <FormInput
                        as="textarea"
                        type="textarea"
                        rows={3}
                        control={control}
                        name="nextBatchStartingPoint"
                        disabled
                    />
                </FormGroup>
            )}
        </>
    );
}
