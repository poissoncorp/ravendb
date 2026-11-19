import { yupResolver } from "@hookform/resolvers/yup";
import messagePublisher from "common/messagePublisher";
import { FormDatePicker, FormGroup, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { adminLogsActions } from "components/pages/resources/manageServer/adminLogs/store/adminLogsSlice";
import { useAppDispatch } from "components/store";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Modal from "components/common/Modal";
import Row from "react-bootstrap/Row";
import * as yup from "yup";
import endpoints from "endpoints";
import { useAppUrls } from "components/hooks/useAppUrls";
import moment from "moment";
import genUtils from "common/generalUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export default function AdminLogsDiskDownloadModal() {
    const t = useStudioTranslation("adminLogs");
    const dispatch = useAppDispatch();

    const { control, handleSubmit, reset } = useForm<FormData>({
        defaultValues: {
            isUsingMinimumDate: false,
            startDate: null,
            isUsingMaximumDate: false,
            endDate: null,
        },
        resolver: yupResolver(schema),
    });

    const { isUsingMinimumDate, isUsingMaximumDate, startDate, endDate } = useWatch({ control: control });

    const { appUrl } = useAppUrls();

    const handleDownload: SubmitHandler<FormData> = (data) => {
        messagePublisher.reportSuccess(t("diskDownload.downloadStarted"));

        const $form = $("#downloadLogsForm");
        const url = endpoints.global.adminLogs.adminLogsDownload;

        $form.attr("action", appUrl.forServer() + url);

        $("[name=from]", $form).val(
            data.isUsingMinimumDate ? null : moment(data.startDate).utc().format(genUtils.utcFullDateFormat)
        );
        $("[name=to]", $form).val(
            data.isUsingMaximumDate ? null : moment(data.endDate).utc().format(genUtils.utcFullDateFormat)
        );

        $form.trigger("submit");

        reset(data);
        dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled());
    };

    return (
        <Modal show size="lg" onHide={() => dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled())}>
            <Form onSubmit={handleSubmit(handleDownload)}>
                <Modal.Header onCloseClick={() => dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled())}>
                    <h3>
                        <Icon icon="storage" addon="download" />
                        {t("diskDownload.title")}
                    </h3>
                </Modal.Header>
                <Modal.Body>
                    <Row>
                        <Col>
                            <FormGroup>
                                <FormDatePicker
                                    control={control}
                                    name="startDate"
                                    placeholderText={t("diskDownload.startDatePlaceholder")}
                                    addon={t("diskDownload.localAddon")}
                                    showTimeSelect
                                    maxDate={endDate}
                                    disabled={isUsingMinimumDate}
                                    title={isUsingMaximumDate && t("diskDownload.minimumDateTitle")}
                                />
                                <FormSwitch control={control} name="isUsingMinimumDate" className="mt-1">
                                    {t("diskDownload.useMinimumDate")}
                                </FormSwitch>
                            </FormGroup>
                        </Col>
                        <Col>
                            <FormGroup>
                                <FormDatePicker
                                    control={control}
                                    name="endDate"
                                    placeholderText={t("diskDownload.endDatePlaceholder")}
                                    addon={t("diskDownload.localAddon")}
                                    showTimeSelect
                                    minDate={startDate}
                                    disabled={isUsingMaximumDate}
                                    title={isUsingMaximumDate && t("diskDownload.maximumDateTitle")}
                                />
                                <FormSwitch control={control} name="isUsingMaximumDate" className="mt-1">
                                    {t("diskDownload.useMaximumDate")}
                                </FormSwitch>
                            </FormGroup>
                        </Col>
                    </Row>
                </Modal.Body>
                <Modal.Footer>
                    <Button
                        variant="secondary"
                        type="button"
                        onClick={() => dispatch(adminLogsActions.isDownloadDiskLogsOpenToggled())}
                    >
                        <Icon icon="cancel" />
                        {t("close")}
                    </Button>
                    <Button type="submit" variant="success">
                        <Icon icon="download" />
                        {t("download")}
                    </Button>
                </Modal.Footer>
            </Form>
            <div className="hidden">
                <form method="get" target="hidden-form" id="downloadLogsForm">
                    <input type="hidden" name="from" />
                    <input type="hidden" name="to" />
                </form>
            </div>
        </Modal>
    );
}

const schema = yup.object({
    isUsingMinimumDate: yup.boolean(),
    startDate: yup
        .date()
        .nullable()
        .when("isUsingMinimumDate", {
            is: false,
            then: (schema) => schema.required(),
        }),
    isUsingMaximumDate: yup.boolean(),
    endDate: yup
        .date()
        .nullable()
        .when("isUsingMaximumDate", {
            is: false,
            then: (schema) => schema.required(),
        }),
});

type FormData = yup.InferType<typeof schema>;
