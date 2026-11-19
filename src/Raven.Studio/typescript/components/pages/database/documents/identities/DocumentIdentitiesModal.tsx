import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import Form from "react-bootstrap/Form";
import { FormLabel } from "components/common/Form";
import React from "react";
import { Icon } from "components/common/Icon";
import { FormInput } from "components/common/Form";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    AddIdentitiesFormData,
    DocumentIdentitiesPrefixTestContext,
    documentIdentitiesSchema,
} from "components/pages/database/documents/identities/DocumentIdentitiesValidation";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { tryHandleSubmit } from "components/utils/common";
import RichAlert from "components/common/RichAlert";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { yupResolver } from "@hookform/resolvers/yup";
import { useEventsCollector } from "hooks/useEventsCollector";
import Modal from "components/common/Modal";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";

interface DocumentIdentitiesModalProps {
    onHide: () => void;
    defaultValues?: AddIdentitiesFormData;
    identities?: AddIdentitiesFormData[];
    refetch: () => void;
    show: boolean;
}

export default function DocumentIdentitiesModal({
    defaultValues,
    refetch,
    identities,
    onHide,
    ...props
}: DocumentIdentitiesModalProps) {
    const t = useStudioTranslation("identities");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const isEditing = !!defaultValues;
    const eventsCollector = useEventsCollector();
    const {
        reset,
        handleSubmit,
        control,
        formState: { isSubmitting },
    } = useForm<AddIdentitiesFormData, DocumentIdentitiesPrefixTestContext>({
        context: {
            identities,
            isEditing,
        },
        resolver: yupResolver(documentIdentitiesSchema),
        defaultValues,
    });

    const formValues = useWatch({ control });

    const onSubmit: SubmitHandler<AddIdentitiesFormData> = ({ prefix, value }) => {
        return tryHandleSubmit(async () => {
            await databasesService.seedIdentity(databaseName, prefix, value);
            if (!isEditing) {
                eventsCollector.reportEvent("identity", "new");
            }
            refetch();
            onHide();
            reset();
        });
    };

    return (
        <Modal centered contentClassName="modal-border bulge-primary" size="lg" {...props}>
            <form onSubmit={handleSubmit(onSubmit)}>
                <Modal.Header className="vstack gap-3" onCloseClick={onHide}>
                    <div className="text-center">
                        <Icon icon="identities" color="primary" margin="me-0" className="fs-1" />
                    </div>
                    <div className="text-center lead">{t("modal.title", { context: isEditing ? "edit" : "new" })}</div>
                </Modal.Header>
                <Modal.Body className="pb-0 vstack gap-3">
                    <DocumentIdentitiesModalForm isEditing={isEditing} control={control} />
                    <InformationBadge isEditing={isEditing} {...formValues} />
                </Modal.Body>
                <Modal.Footer className="mt-4">
                    <Button className="link-muted" variant="link" onClick={onHide} type="button">
                        {t("modal.close")}
                    </Button>
                    <ButtonWithSpinner
                        className="rounded-pill"
                        variant="success"
                        icon="save"
                        isSpinning={isSubmitting}
                        type="submit"
                        title={t("modal.saveButton")}
                    >
                        {t("modal.saveButton")}
                    </ButtonWithSpinner>
                </Modal.Footer>
            </form>
        </Modal>
    );
}

interface InformationBadgeProps extends AddIdentitiesFormData {
    isEditing: boolean;
}

function InformationBadge({ prefix: formPrefix, value, isEditing }: InformationBadgeProps) {
    const t = useStudioTranslation("identities");
    const prefix = formPrefix === undefined ? t("modal.prefixExample") : formPrefix;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { manageServerService } = useServices();
    const { result: identityPartsSeparator, loading } = useAsync(async () => {
        try {
            const clientConfiguration = manageServerService.getClientConfiguration(databaseName);
            const globalClientConfiguration = manageServerService.getGlobalClientConfiguration();

            const [clientConfig, globalConfig] = await Promise.all([clientConfiguration, globalClientConfiguration]);

            if (!clientConfig?.Disabled && clientConfig?.IdentityPartsSeparator != null) {
                return clientConfig.IdentityPartsSeparator;
            }

            if (!globalConfig?.Disabled && globalConfig?.IdentityPartsSeparator != null) {
                return globalConfig.IdentityPartsSeparator;
            }

            return "/";
        } catch (error) {
            console.error("Error fetching configurations:", error);
            return "/";
        }
    }, []);

    const formattedPrefix = isEditing ? prefix.slice(0, -1) : prefix;

    return (
        <RichAlert icon="info" variant="info">
            <div className="word-break">
                <p className="mb-0">
                    {t("modal.separatorInfo")}{" "}
                    <code>
                        <LazyLoad active={loading}>{identityPartsSeparator}</LazyLoad>
                    </code>
                </p>
                <p className="mb-0">
                    <StudioTrans
                        ns="identities"
                        i18nKey="modal.nextDocumentInfo"
                        components={{ strong: <strong />, code: <code /> }}
                        options={{
                            prefix: formattedPrefix,
                            separator: identityPartsSeparator ?? "",
                            nextValue: value ? value + 1 : t("modal.nextValueExample"),
                        }}
                    />
                </p>
            </div>
        </RichAlert>
    );
}

interface DocumentIdentitiesModalFormProps {
    control: Control<AddIdentitiesFormData>;
    isEditing?: boolean;
}

function DocumentIdentitiesModalForm({ control, isEditing }: DocumentIdentitiesModalFormProps) {
    const t = useStudioTranslation("identities");

    return (
        <Form className="vstack gap-3">
            <InputGroup className="vstack">
                <FormLabel>{t("modal.prefixLabel")}</FormLabel>
                <FormInput
                    name="prefix"
                    type="text"
                    control={control}
                    placeholder={t("modal.prefixPlaceholder")}
                    disabled={isEditing}
                />
            </InputGroup>
            <InputGroup className="vstack">
                <FormLabel>{t("modal.valueLabel")}</FormLabel>
                <FormInput name="value" type="number" control={control} placeholder={t("modal.valuePlaceholder")} />
            </InputGroup>
        </Form>
    );
}
