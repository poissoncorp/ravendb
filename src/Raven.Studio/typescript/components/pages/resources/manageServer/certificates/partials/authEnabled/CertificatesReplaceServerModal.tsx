import { yupResolver } from "@hookform/resolvers/yup";
import { FormInput, FormSwitch, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import * as yup from "yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import CertificatesFileField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesFileField";
import RichAlert from "components/common/RichAlert";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import classNames from "classnames";
import Modal from "components/common/Modal";
import useUniqueId from "components/hooks/useUniqueId";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

export default function CertificatesReplaceServerModal() {
    const t = useStudioTranslation("certificates");
    const commonT = useStudioTranslation("common");
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const titleId = useUniqueId("replace-server-certificate-title");

    const close = () => dispatch(certificatesActions.isReplaceServerModalOpenToggled());

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            certificateAsBase64: "",
            certificatePassphrase: "",
            isReplaceImmediately: false,
        },
    });

    const { control, formState, handleSubmit, reset } = form;

    const formValues = useWatch({ control });

    const asyncGetClusterDomains = useAsync(manageServerService.getClusterDomains, []);

    const handleReplace: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "replace");
            await manageServerService.replaceClusterCertificate(
                certificatesUtils.mapReplaceServerToDto(formData),
                formData.isReplaceImmediately
            );
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.isReplaceServerModalOpenToggled());
        });
    };

    return (
        <Modal
            show
            size="lg"
            centered
            contentClassName="modal-border bulge-warning"
            backdrop="static"
            onHide={close}
            aria-labelledby={titleId}
        >
            <FormProvider {...form}>
                <form onSubmit={handleSubmit(handleReplace)}>
                    <Modal.Header className="pt-3 pb-0" onCloseClick={close}>
                        <Icon icon="refresh" size="lg" color="warning" margin="me-3" />
                        <h3 id={titleId} className="mb-0">
                            {t("replace.title")}
                        </h3>
                    </Modal.Header>
                    <Modal.Body>
                        <LazyLoad active={asyncGetClusterDomains.loading}>
                            <RichAlert title="" variant="info" className="my-2">
                                <span>{t("replace.info")}</span>
                                <div className={classNames(asyncGetClusterDomains.result?.length ? "" : "d-none")}>
                                    {t("replace.domainsInfo")}
                                    <ul className="mt-2">
                                        {asyncGetClusterDomains.result?.map((domain, idx) => (
                                            <li key={idx}>{domain}</li>
                                        ))}
                                    </ul>
                                </div>
                            </RichAlert>
                        </LazyLoad>
                        <CertificatesFileField infoPopoverBody={t("replace.fileInfo")} />
                        <FormGroup>
                            <FormLabel>{t("form.passphrase")}</FormLabel>
                            <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                        </FormGroup>
                        <FormGroup>
                            <FormSwitch control={control} name="isReplaceImmediately">
                                {t("replace.immediately")}
                            </FormSwitch>
                        </FormGroup>
                        {formValues.isReplaceImmediately && (
                            <RichAlert variant="warning">
                                <StudioTrans
                                    ns="certificates"
                                    i18nKey="replace.immediatelyWarning"
                                    components={{ strong: <strong /> }}
                                />
                            </RichAlert>
                        )}
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={close} className="link-muted">
                            {commonT("cancel")}
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="warning"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            {t("replace.submit")}
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    certificateAsBase64: yup.string().required(),
    certificatePassphrase: yup.string(),
    isReplaceImmediately: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesReplaceServerFormData = FormData;
