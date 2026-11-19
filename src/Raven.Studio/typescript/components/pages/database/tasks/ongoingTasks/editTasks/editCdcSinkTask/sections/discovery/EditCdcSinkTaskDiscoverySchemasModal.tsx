import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import InnerForm from "components/common/InnerForm";
import Modal from "components/common/Modal";
import RichAlert from "components/common/RichAlert";
import FormStringValueList from "components/common/formFields/FormStringValueList";
import { SubmitHandler, useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import { UseAsyncReturn } from "react-async-hook";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface EditCdcSinkTaskDiscoverySchemasModalProps {
    asyncGetSchema: UseAsyncReturn<
        Raven.Client.Documents.Operations.CdcSink.Schema.CdcSinkSourceSchema,
        [schemas: string[]]
    >;
    onClose: () => void;
}

export default function EditCdcSinkTaskDiscoverySchemasModal({
    asyncGetSchema,
    onClose,
}: EditCdcSinkTaskDiscoverySchemasModalProps) {
    const t = useStudioTranslation("editCdcSinkTask");
    const commonT = useStudioTranslation("common");
    const { control, handleSubmit } = useForm<DiscoverySchemasFormData>({
        defaultValues: {
            schemas: [],
        },
        resolver: yupResolver(schema),
    });

    const handleDiscover: SubmitHandler<DiscoverySchemasFormData> = async (formData) => {
        const schemas = formData.schemas.map((x) => x.value);
        await asyncGetSchema.execute(schemas.length > 0 ? schemas : null);
        onClose();
    };

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-primary">
            <Modal.Header onCloseClick={onClose} className="pb-0">
                <h3 className="m-0">{t("discovery.discoverTables")}</h3>
            </Modal.Header>
            <Modal.Body>
                <InnerForm onSubmit={handleSubmit(handleDiscover)} className="vstack gap-3">
                    <RichAlert variant="info">
                        <StudioTrans ns="editCdcSinkTask" i18nKey="discovery.schemasInfo" components={{ br: <br /> }} />
                    </RichAlert>
                    <FormStringValueList
                        title={t("discovery.schemas")}
                        addButtonLabel={t("discovery.addSchema")}
                        control={control}
                        name="schemas"
                        fieldNameAccessor={(idx) => `schemas.${idx}.value`}
                        defaultValue={{ value: "" }}
                        emptyMessage={t("discovery.noSchemas")}
                    />
                </InnerForm>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={onClose}>
                    {commonT("cancel")}
                </Button>
                <ButtonWithSpinner
                    variant="primary"
                    onClick={handleSubmit(handleDiscover)}
                    isSpinning={asyncGetSchema.loading}
                    className="rounded-pill"
                    icon="search"
                >
                    {t("discovery.discover")}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}

const schema = yup.object({
    schemas: yup.array().of(
        yup.object({
            value: yup.string().trim().strict().required(),
        })
    ),
});

export type DiscoverySchemasFormData = yup.InferType<typeof schema>;
