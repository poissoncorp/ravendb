import CollapseButton from "components/common/CollapseButton";
import { SelectOption } from "components/common/select/Select";
import useBoolean from "components/hooks/useBoolean";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import Collapse from "react-bootstrap/Collapse";
import { FormErrorIcon, FormGroup, FormInput, FormLabel, FormSelect, FormSwitch } from "components/common/Form";
import RichAlert from "components/common/RichAlert";
import { FormTaskResponsibleNode } from "components/common/formFields/FormTaskResponsibleNode";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { Icon } from "components/common/Icon";
import { FormTaskConnectionString } from "components/common/formFields/FormTaskConnectionString";
import { connectionStringSelectors } from "components/pages/database/settings/connectionStrings/store/connectionStringsSlice";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export default function EditCdcSinkTaskBasicSection() {
    const t = useStudioTranslation("editCdcSinkTask");
    const { value: isPanelOpen, setValue: setIsPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const [connectionStringName, postgresPublicationName, postgresSlotName] = useWatch({
        control,
        name: ["connectionStringName", "postgresPublicationName", "postgresSlotName"],
    });
    const sqlConnections = useAppSelector(connectionStringSelectors.connectionsByType("Sql"));
    const selectedConnection = sqlConnections.find((x) => x.name === connectionStringName);

    const hasPostgresSettings =
        (selectedConnection && "factoryName" in selectedConnection && selectedConnection.factoryName === "Npgsql") ||
        Boolean(postgresPublicationName || postgresSlotName);

    const taskStateOptions: SelectOption<OngoingTaskState>[] = [
        { value: "Enabled", label: t("basic.stateEnabled") },
        { value: "Disabled", label: t("basic.stateDisabled") },
    ];

    return (
        <div>
            <div className="hstack align-items-center">
                <h3 className="m-0">{t("basic.title")}</h3>
                <FormErrorIcon
                    control={control}
                    paths={["name", "connectionStringName", "responsibleNode"]}
                    onError={() => setIsPanelOpen(true)}
                />
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">{t("basic.description")}</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary">
                        <FormGroup>
                            <FormLabel>{t("basic.taskName")}</FormLabel>
                            <FormInput
                                type="text"
                                control={control}
                                name="name"
                                placeholder={t("basic.taskNamePlaceholder")}
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>{t("basic.taskState")}</FormLabel>
                            <FormSelect control={control} name="state" options={taskStateOptions} />
                        </FormGroup>
                        <FormTaskConnectionString control={control} name="connectionStringName" type="Sql" />
                        {hasPostgresSettings && (
                            <>
                                <RichAlert variant="info">{t("basic.postgresInfo")}</RichAlert>
                                <FormGroup className="mt-2">
                                    <FormLabel>{t("basic.publicationName")}</FormLabel>
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name="postgresPublicationName"
                                        placeholder="rvn_cdc_p_..."
                                    />
                                </FormGroup>
                                <FormGroup>
                                    <FormLabel>{t("basic.slotName")}</FormLabel>
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name="postgresSlotName"
                                        placeholder="rvn_cdc_s_..."
                                    />
                                </FormGroup>
                            </>
                        )}
                        <FormTaskResponsibleNode
                            control={control}
                            isSetName="isSetResponsibleNode"
                            nodeName="responsibleNode"
                            isPinName="isPinResponsibleNode"
                        />
                        <FormGroup>
                            <FormSwitch control={control} name="skipInitialLoad">
                                {t("basic.skipInitialLoad")}
                                <PopoverWithHoverWrapper
                                    message={
                                        <StudioTrans
                                            ns="editCdcSinkTask"
                                            i18nKey="basic.skipInitialLoadInfo"
                                            components={{ br: <br /> }}
                                        />
                                    }
                                >
                                    <Icon icon="info-new" margin="ms-1" />
                                </PopoverWithHoverWrapper>
                            </FormSwitch>
                        </FormGroup>
                    </div>
                </div>
            </Collapse>
        </div>
    );
}
