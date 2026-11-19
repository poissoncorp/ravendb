import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import { FormCheckbox, FormGroup, FormLabel, FormSelect } from "components/common/Form";
import Row from "react-bootstrap/Row";
import * as yup from "yup";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { logFilterActionOptions, logLevelOptions, tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import { Icon } from "components/common/Icon";
import { yupResolver } from "@hookform/resolvers/yup";
import AdminLogsFilterField from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsFilterField";
import AdminLogsConfigTableValue from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsConfigTableValue";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import {
    adminLogsActions,
    adminLogsSelectors,
} from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AdminLogsPersistInfoIcon from "components/pages/resources/manageServer/adminLogs/bits/AdminLogsPersistInfoIcon";
import { adminLogsUtils } from "components/pages/resources/manageServer/adminLogs/common/adminLogsUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import Accordion from "react-bootstrap/Accordion";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function AdminLogsConfigLogs({ targetId }: { targetId: string }) {
    const t = useStudioTranslation("adminLogs");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();

    const config = useAppSelector(adminLogsSelectors.configs).adminLogsConfig.Logs;
    const isCloud = useAppSelector(licenseSelectors.statusValue("IsCloud"));

    const { control, formState, handleSubmit, reset } = useForm<AdminLogsConfigLogsFormData>({
        defaultValues: mapToFormDefaultValues(config),
        resolver: yupResolver(schema),
    });

    useDirtyFlag(formState.isDirty);

    const filterFieldArray = useFieldArray({
        control,
        name: "filters",
    });

    const handleSave: SubmitHandler<AdminLogsConfigLogsFormData> = (data) => {
        return tryHandleSubmit(async () => {
            await manageServerService.saveAdminLogsConfiguration(mapToDto(data));
            reset(data);
            dispatch(adminLogsActions.fetchConfigs());
            dispatch(adminLogsActions.isDiscSettingOpenToggled());
        });
    };

    return (
        <Accordion.Item eventKey={targetId} className="p-1 rounded-3">
            <Accordion.Header>{t("diskSettings.logs.title")}</Accordion.Header>
            <Accordion.Body>
                <h5 className="text-center text-muted text-uppercase">{t("diskSettings.logs.heading")}</h5>
                <Form onSubmit={handleSubmit(handleSave)} key={targetId}>
                    <Row>
                        <Col>
                            <FormGroup>
                                <FormLabel>
                                    {t("diskSettings.currentMinLevel")}
                                    <PopoverWithHoverWrapper message={t("diskSettings.logs.minLevelInfo")}>
                                        <Icon icon="info" color="info" margin="ms-1" />
                                    </PopoverWithHoverWrapper>
                                </FormLabel>
                                <FormSelect control={control} name="minLevel" options={logLevelOptions} />
                                {!isCloud && (
                                    <FormCheckbox control={control} name="isPersist" className="mt-1">
                                        <StudioTrans
                                            ns="adminLogs"
                                            i18nKey="persistMinLevel"
                                            components={{ code: <code /> }}
                                        />
                                        <AdminLogsPersistInfoIcon />
                                    </FormCheckbox>
                                )}
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <FormLabel>
                                    {t("defaultFilterAction.label")}
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                <p className="mb-1">
                                                    <StudioTrans
                                                        ns="adminLogs"
                                                        i18nKey="defaultFilterAction.info"
                                                        components={{ strong: <strong />, br: <br /> }}
                                                    />
                                                </p>
                                                <ul className="mb-0">
                                                    <li className="mb-1">{t("defaultFilterAction.noMatchItem")}</li>
                                                    <li>
                                                        <StudioTrans
                                                            ns="adminLogs"
                                                            i18nKey="defaultFilterAction.neutralItem"
                                                            components={{ code: <code /> }}
                                                        />
                                                    </li>
                                                </ul>
                                            </>
                                        }
                                    >
                                        <Icon icon="info" color="info" margin="ms-1" />
                                    </PopoverWithHoverWrapper>
                                </FormLabel>
                                <FormSelect
                                    control={control}
                                    name="logFilterDefaultAction"
                                    options={logFilterActionOptions}
                                    isDisabled={filterFieldArray.fields.length === 0}
                                />
                            </FormGroup>
                        </Col>
                    </Row>
                    <FormGroup className="vstack">
                        <FormLabel>{t("filters.label")}</FormLabel>
                        <div className="vstack gap-1 mb-1">
                            {filterFieldArray.fields.map((field, idx) => (
                                <AdminLogsFilterField
                                    key={field.id}
                                    control={control}
                                    idx={idx}
                                    remove={() => filterFieldArray.remove(idx)}
                                />
                            ))}
                        </div>
                        <Button
                            type="button"
                            variant="info"
                            className="w-fit-content"
                            onClick={() => filterFieldArray.append(adminLogsUtils.initialFilter)}
                        >
                            <Icon icon="plus" />
                            {t("filters.addButton")}
                        </Button>
                    </FormGroup>
                    <ButtonWithSpinner
                        variant="success"
                        type="submit"
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
                    <PopoverWithHoverWrapper message={t("diskSettings.readOnlyInfo")}>
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </h5>
                <Table className="m-0">
                    <tbody>
                        <tr>
                            <td>{t("diskSettings.table.path")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.Path} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.minLevel")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MinLevel} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.archiveAboveSize")}</td>
                            <td>
                                <AdminLogsConfigTableValue
                                    value={`${config.ArchiveAboveSizeInMb?.toLocaleString()} MB`}
                                />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.maxArchivedDays")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveDays} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.maxArchivedFiles")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.MaxArchiveFiles} />
                            </td>
                        </tr>
                        <tr>
                            <td>{t("diskSettings.table.archiveFileCompression")}</td>
                            <td>
                                <AdminLogsConfigTableValue value={config.EnableArchiveFileCompression} />
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
    logFilterDefaultAction: yup.string<Sparrow.Logging.LogFilterAction>().nullable().required(),
    filters: adminLogsUtils.filtersSchema,
});

export type AdminLogsConfigLogsFormData = yup.InferType<typeof schema>;

function mapToFormDefaultValues(
    config: Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult["Logs"]
): AdminLogsConfigLogsFormData {
    return {
        minLevel: config.CurrentMinLevel,
        isPersist: false,
        filters: config.CurrentFilters.map((x) => ({
            minLevel: x.MinLevel,
            maxLevel: x.MaxLevel,
            condition: x.Condition,
            action: x.Action,
        })),
        logFilterDefaultAction: config.CurrentLogFilterDefaultAction,
    };
}

function mapToDto(
    data: AdminLogsConfigLogsFormData
): Partial<Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters> {
    return {
        Logs: {
            Filters: data.filters.map((x) => ({
                MinLevel: x.minLevel,
                MaxLevel: x.maxLevel,
                Condition: x.condition,
                Action: x.action,
            })),
            MinLevel: data.minLevel,
            LogFilterDefaultAction: data.logFilterDefaultAction,
        },
        Persist: data.isPersist,
    };
}
