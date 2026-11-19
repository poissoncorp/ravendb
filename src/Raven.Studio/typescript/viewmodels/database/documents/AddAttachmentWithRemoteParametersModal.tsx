import Modal from "components/common/Modal";
import { Controller, ControllerRenderProps, FieldValues, FormProvider, useForm, useWatch } from "react-hook-form";
import * as yup from "yup";
import { Icon } from "components/common/Icon";
import { yupResolver } from "@hookform/resolvers/yup";
import { FormDatePicker, FormGroup, FormLabel, FormSelectAutocomplete } from "components/common/Form";
import FileUploadPanel from "components/common/FileUploadPanel";
import useAttachmentUpload from "components/hooks/useAttachmentUpload";
import messagePublisher from "common/messagePublisher";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import React from "react";
import document from "models/database/documents/document";
import Button from "react-bootstrap/Button";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "hooks/useServices";
import moment from "moment";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import RichAlert from "components/common/RichAlert";
import { components, GroupBase, OptionProps } from "react-select";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import type { StudioTranslate } from "common/i18n/resources";
import RemoteAttachmentParameters = Raven.Client.Documents.Operations.Attachments.RemoteAttachmentParameters;

type AddAttachmentWithRemoteParametersModalProps = {
    document: KnockoutObservable<document>;
    onUploaded: () => void;
    onClose: () => void;
};

export default function AddAttachmentWithRemoteParametersModal({
    document,
    onClose,
    onUploaded,
}: AddAttachmentWithRemoteParametersModalProps) {
    const t = useStudioTranslation("editDocument");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const asyncGetRemoteAttachmentParametersConfig = useAsyncCallback(() =>
        databasesService.getRemoteAttachmentsDestinations(databaseName)
    );

    const form = useForm({
        resolver: yupResolver(getSchema(t)),
        defaultValues: async () => {
            const config = await asyncGetRemoteAttachmentParametersConfig.execute();
            return getDefaultValues(config);
        },
    });

    const { control, formState } = form;
    const { uploadItems, batchProgress, upload, abortCurrent } = useAttachmentUpload(
        document,
        databaseName,
        onUploaded
    );

    const selectedDestination = useWatch({
        control,
        name: "identifier",
    });

    const selectedFiles = useWatch({ control, name: "files" }) as File[];
    const selectedUploadDate = useWatch({ control, name: "uploadDate" });

    const handleSubmit = async (formData: AttachmentWithRemoteParametersFormData) => {
        if (await upload(formData.files as File[], mapToDto(formData))) {
            onClose();
        }
    };

    const onFileDropzoneChange = (files: File[], field: ControllerRenderProps<FieldValues, "files">) => {
        if (files.some((file) => !file.name.trim())) {
            messagePublisher.reportError(t("attachmentUpload.loadFileFailed"));
            form.setError("files", { type: "manual", message: t("attachmentUpload.loadFileFailed") });
            return;
        }

        const current: File[] = field.value ?? [];
        const replaced = current.filter((x) => !files.some((file) => file.name === x.name));
        field.onChange([...replaced, ...files]);
    };

    const removeFile = (file: File) => {
        form.setValue(
            "files",
            selectedFiles.filter((x) => x !== file),
            { shouldValidate: true, shouldDirty: true }
        );
    };

    const clearFiles = () => {
        form.setValue("files", [], { shouldValidate: true, shouldDirty: true });
    };

    const attachmentsCount = selectedFiles?.length || 1;

    return (
        <Modal size="lg" show contentClassName="modal-border bulge-info">
            <Modal.Header className="pb-0" onCloseClick={onClose}>
                <h3>
                    <Icon icon="remote-attachment" color="info" />
                    {t("addRemoteAttachment.title", { count: attachmentsCount })}
                </h3>
            </Modal.Header>
            <FormProvider {...form}>
                <Modal.Body>
                    <form onSubmit={form.handleSubmit(handleSubmit)}>
                        <FormGroup>
                            <Controller
                                name="files"
                                render={({ field }) => (
                                    <FileUploadPanel
                                        items={uploadItems(selectedFiles ?? [])}
                                        onChange={(files) => onFileDropzoneChange(files, field)}
                                        onRemove={removeFile}
                                        onCancel={abortCurrent}
                                        onClearAll={clearFiles}
                                    />
                                )}
                            ></Controller>
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>{t("addRemoteAttachment.destinationLabel")}</FormLabel>
                            <FormSelectAutocomplete
                                placeholder={t("addRemoteAttachment.destinationPlaceholder")}
                                isLoading={asyncGetRemoteAttachmentParametersConfig.loading}
                                options={getRemoteAttachmentsDestinationsOptions(
                                    asyncGetRemoteAttachmentParametersConfig.result
                                )}
                                name="identifier"
                                control={control}
                                components={{ Option: RemoteAttachmentDestinationOption }}
                            />
                            <RemoteAttachmentWarning
                                config={asyncGetRemoteAttachmentParametersConfig.result}
                                selectedDestination={selectedDestination}
                            />
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>{t("addRemoteAttachment.uploadTimeLabel")}</FormLabel>
                            <FormDatePicker
                                placeholderText={t("addRemoteAttachment.uploadTimePlaceholder")}
                                showTimeSelect
                                minDate={new Date()}
                                minTime={getMinUploadTime(selectedUploadDate)}
                                maxTime={moment().endOf("day").toDate()}
                                name="uploadDate"
                                control={control}
                            />
                        </FormGroup>
                    </form>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="link" className="text-muted" onClick={onClose}>
                        {t("close")}
                    </Button>
                    <ButtonWithSpinner
                        className="rounded-pill"
                        variant="info"
                        isSpinning={formState.isSubmitting}
                        onClick={form.handleSubmit(handleSubmit)}
                        disabled={!formState.isValid}
                    >
                        {batchProgress
                            ? t("attachmentUpload.uploading", {
                                  position: batchProgress.position,
                                  total: batchProgress.count,
                              })
                            : t("addRemoteAttachment.save", { count: attachmentsCount })}
                    </ButtonWithSpinner>
                </Modal.Footer>
            </FormProvider>
        </Modal>
    );
}

interface RemoteAttachmentWarningProps {
    config: RemoteAttachmentsStudioConfiguration;
    selectedDestination: string;
}

function RemoteAttachmentWarning({ config, selectedDestination }: RemoteAttachmentWarningProps) {
    if (config?.Disabled) {
        return (
            <RichAlert className="mt-2" icon="warning" variant="warning">
                <StudioTrans
                    ns="editDocument"
                    i18nKey="addRemoteAttachment.featureDisabled"
                    components={{ b: <b /> }}
                />
            </RichAlert>
        );
    }

    if (config?.Destinations[selectedDestination]?.Disabled) {
        return (
            <RichAlert className="mt-2" icon="warning" variant="warning">
                <StudioTrans
                    ns="editDocument"
                    i18nKey="addRemoteAttachment.destinationDisabled"
                    components={{ b: <b /> }}
                />
            </RichAlert>
        );
    }

    return null;
}

function getMinUploadTime(selectedDate: Date): Date {
    const isToday = !selectedDate || moment(selectedDate).isSame(moment(), "day");
    return isToday ? moment().subtract(29, "minutes").toDate() : moment().startOf("day").toDate();
}

function getSchema(t: StudioTranslate<"editDocument">) {
    return yup.object({
        files: yup
            .array()
            .of(yup.mixed())
            .min(1, t("addRemoteAttachment.fileRequired"))
            .required(t("addRemoteAttachment.fileRequired")),
        identifier: yup.string().required(t("addRemoteAttachment.identifierRequired")),
        uploadDate: yup.date().required(t("addRemoteAttachment.uploadDateRequired")),
    });
}

type AttachmentWithRemoteParametersFormData = yup.InferType<ReturnType<typeof getSchema>>;

const mapToDto = (values: AttachmentWithRemoteParametersFormData): RemoteAttachmentParameters => {
    return {
        At: values.uploadDate.toISOString(),
        Identifier: values.identifier,
    };
};

const getRemoteAttachmentsDestinationsOptions = (
    remoteAttachmentsConfiguration?: RemoteAttachmentsStudioConfiguration
) => {
    if (!remoteAttachmentsConfiguration) {
        return [];
    }
    return Object.keys(remoteAttachmentsConfiguration.Destinations).map((destination) => {
        const destinationKey = remoteAttachmentsConfiguration.Destinations[destination];
        return { label: destination, value: destination, disabled: destinationKey.Disabled };
    });
};

function getDefaultValues(configResult: RemoteAttachmentsStudioConfiguration): AttachmentWithRemoteParametersFormData {
    const options = getRemoteAttachmentsDestinationsOptions(configResult);
    // if there is only one destination, use it as default
    if (options.length === 1) {
        return {
            identifier: options[0].value,
            uploadDate: new Date(),
            files: [],
        };
    }

    return {
        identifier: null,
        uploadDate: new Date(),
        files: [],
    };
}

interface RemoteAttachmentDestination {
    value: string;
    label: string;
    disabled?: boolean;
}

type RemoteAttachmentDestinationOptionProps = OptionProps<
    RemoteAttachmentDestination,
    false,
    GroupBase<RemoteAttachmentDestination>
>;

function RemoteAttachmentDestinationOption(props: RemoteAttachmentDestinationOptionProps) {
    const t = useStudioTranslation("editDocument");
    const { data, label } = props;
    return (
        <components.Option {...props}>
            {label}
            {data.disabled && (
                <PopoverWithHoverWrapper message={t("addRemoteAttachment.destinationDisabledPopover")}>
                    <Icon icon="warning" color="warning" margin="ms-1" />
                </PopoverWithHoverWrapper>
            )}
        </components.Option>
    );
}
