import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormCheckbox, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useServices } from "components/hooks/useServices";
import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { logLevelOptions, tryHandleSubmit } from "components/utils/common";
import { SubmitHandler, useForm } from "react-hook-form";
import Table from "react-bootstrap/Table";
import Form from "react-bootstrap/Form";
import * as yup from "yup";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

type MicrosoftLogsConfig = Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult["MicrosoftLogs"];

interface AdminLogsConfigMicrosoftLogsProps {
    targetId: string;
}

export default function AdminLogsConfigMicrosoftLogs({ targetId }: AdminLogsConfigMicrosoftLogsProps) {
    const t = useStudioTranslation("adminLogs");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.MicrosoftLogs;
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { control, formState, handleSubmit, reset } = useForm<FormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const handleSave: SubmitHandler<FormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveAdminLogsConfiguration(mapToDto(data));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <Accordion.Item eventKey={targetId} className="p-1 rounded-3">
            <Accordion.Header>{t("diskSettings.microsoftLogs.title")}</Accordion.Header>
            <Accordion.Body>
                <h5 className="text-center text-muted text-uppercase">{t("diskSettings.microsoftLogs.heading")}</h5>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <FormGroup>
                        <FormLabel>{t("diskSettings.currentMinLevel")}</FormLabel>
                        <FormSelect control={control} name="minLevel" options={logLevelOptions} />
                        {!isCloud && (
                            <FormCheckbox control={control} name="isPersist" className="mt-1">
                                <StudioTrans ns="adminLogs" i18nKey="persistMinLevel" components={{ code: <code /> }} />
                                <AdminLogsPersistInfoIcon />
                            </FormCheckbox>
                        )}
                    </FormGroup>
                    <ButtonWithSpinner
                        type="submit"
                        variant="success"
                        className="ms-auto"
                        icon="save"
                        isSpinning={formState.isSubmitting}
                        disabled={!formState.isDirty}
                    >
                        {commonT("save")}
                    </ButtonWithSpinner>
                </Form>
                <h5 className="text-center text-muted text-uppercase">
                    {t("diskSettings.readOnly")}
                    <PopoverWithHoverWrapper message={t("diskSettings.readOnlyInfoSingle")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </h5>
                <Table className="m-0">
                    <tbody>
                        <tr>
                            <td>{t("diskSettings.table.minLevel")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MinLevel} />
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </Accordion.Body>
        </Accordion.Item>
    );
}

const schema = yup.object({
    minLevel: yup.string<Sparrow.Logging.LogLevel>().nullable().required(),
    isPersist: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(config: MicrosoftLogsConfig): FormData {
    return {
        minLevel: config.CurrentMinLevel,
        isPersist: false,
    };
}

function mapToDto(
    data: FormData
): Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters> {
    return {
        MicrosoftLogs: {
            MinLevel: data.minLevel,
        },
        Persist: data.isPersist,
    };
}
