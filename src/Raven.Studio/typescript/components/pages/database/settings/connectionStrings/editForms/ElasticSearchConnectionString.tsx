import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";

import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm, UseFormTrigger, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    EditConnectionStringFormProps,
    ElasticSearchAuthenticationMethod,
    ElasticSearchConnection,
} from "../connectionStringsTypes";
import { yupObjectSchema } from "components/utils/yupUtils";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import fileImporter from "common/fileImporter";
import certificateUtils from "common/certificateUtils";
import messagePublisher from "common/messagePublisher";
import { mapElasticSearchAuthenticationToDto } from "../store/connectionStringsMapsToDto";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import ElasticSearchCertificate from "./ElasticSearchCertificate";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import forge = require("node-forge");
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<ElasticSearchConnection>;

export interface ElasticSearchStringProps extends EditConnectionStringFormProps {
    initialConnection: ElasticSearchConnection;
}

export default function ElasticSearchConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: ElasticSearchStringProps) {
    const t = useStudioTranslation("connectionStrings");
    const usedNames = useAppSelector(connectionStringSelectors.connections)["ElasticSearch"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const { control, formState, handleSubmit, setValue, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
        context: {
            isForNewConnection,
            usedNames,
        } satisfies ConnectionStringsNameContext,
    });

    const urlFieldArray = useFieldArray({
        name: "nodes",
        control,
    });

    const formValues = useWatch({ control });

    const onCertificateUploaded = (data: string) => {
        const currentCertificates = formValues.certificatesBase64 ?? [];

        try {
            // First detect the data format, pfx (binary) or crt/cer (text)
            // The line bellow will throw if data is not pfx
            forge.asn1.fromDer(data);

            // *** Handle pfx ***
            try {
                const certAsBase64 = forge.util.encode64(data);
                const extractBase64s = certificateUtils
                    .extractCertificatesFromPkcs12(certAsBase64, undefined)
                    .map((x) => certificateUtils.extractBase64(x));

                setValue("certificatesBase64", [...currentCertificates, ...extractBase64s]);
            } catch ($ex1) {
                messagePublisher.reportError(t("elasticSearch.uploadCertificateError"), $ex1);
            }
        } catch {
            // *** Handle crt/cer ***
            try {
                setValue("certificatesBase64", [...currentCertificates, certificateUtils.extractBase64(data)]);
            } catch ($ex2) {
                messagePublisher.reportError(t("elasticSearch.uploadCertificateError"), $ex2);
            }
        }
    };

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "ElasticSearch",
            ...formData,
        } satisfies ElasticSearchConnection);
    };

    const deleteCertificate = (cert: string) => {
        setValue(
            "certificatesBase64",
            formValues.certificatesBase64.filter((x) => x !== cert)
        );
    };

    const isUploadCertificateVisible = !formValues.certificatesBase64 || formValues.certificatesBase64.length === 0;

    return (
        <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-3">
            <div className="mb-2">
                <FormLabel>{t("form.nameLabel")}</FormLabel>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder={t("form.namePlaceholder")}
                    disabled={!isForNewConnection}
                    autoComplete="off"
                />
            </div>
            <div className="mb-2">
                <FormLabel>{t("elasticSearch.nodesUrlsLabel")}</FormLabel>
                <div className="vstack gap-3">
                    {formState.errors?.nodes?.message && (
                        <div className="text-danger small">{formState.errors.nodes.message}</div>
                    )}
                    {urlFieldArray.fields.map((urlField, idx) => (
                        <NodeUrl
                            key={urlField.id}
                            idx={idx}
                            control={control}
                            formValues={formValues}
                            isDeleteButtonVisible={urlFieldArray.fields.length > 1}
                            onDelete={() => urlFieldArray.remove(idx)}
                            trigger={trigger}
                        />
                    ))}
                </div>
                <Button variant="info" className="mt-4" onClick={() => urlFieldArray.append({ url: null })}>
                    <Icon icon="plus" />
                    {t("elasticSearch.addUrl")}
                </Button>
            </div>
            <div className="mb-2">
                <FormLabel>{t("form.authenticationLabel")}</FormLabel>
                <FormSelect
                    name="authMethodUsed"
                    control={control}
                    placeholder={t("form.authenticationPlaceholder")}
                    options={getAuthenticationOptions(t)}
                    isSearchable={false}
                />
            </div>
            {formValues.authMethodUsed === "Basic" && (
                <div className="vstack gap-3">
                    <div className="mb-2">
                        <FormLabel>{t("elasticSearch.usernameLabel")}</FormLabel>
                        <FormInput
                            control={control}
                            name="username"
                            type="text"
                            placeholder={t("elasticSearch.usernamePlaceholder")}
                            autoComplete="off"
                        />
                    </div>
                    <div className="mb-2">
                        <FormLabel>{t("elasticSearch.passwordLabel")}</FormLabel>
                        <FormInput
                            control={control}
                            name="password"
                            type="password"
                            placeholder={t("elasticSearch.passwordPlaceholder")}
                            autoComplete="off"
                        />
                    </div>
                </div>
            )}
            {formValues.authMethodUsed === "API Key" && (
                <div className="vstack gap-3">
                    <div className="mb-2">
                        <FormLabel>{t("elasticSearch.apiKeyIdLabel")}</FormLabel>
                        <FormInput
                            control={control}
                            name="apiKeyId"
                            type="text"
                            placeholder={t("elasticSearch.apiKeyIdPlaceholder")}
                            autoComplete="off"
                        />
                    </div>
                    <div className="mb-2">
                        <FormLabel>{t("elasticSearch.apiKeyLabel")}</FormLabel>
                        <FormInput
                            control={control}
                            name="apiKey"
                            type="text"
                            placeholder={t("elasticSearch.apiKeyPlaceholder")}
                            autoComplete="off"
                        />
                    </div>
                </div>
            )}
            {formValues.authMethodUsed === "Encoded API Key" && (
                <div className="mb-2">
                    <FormLabel>{t("elasticSearch.encodedApiKeyLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="encodedApiKey"
                        type="text"
                        placeholder={t("elasticSearch.encodedApiKeyPlaceholder")}
                        autoComplete="off"
                    />
                </div>
            )}
            {formValues.authMethodUsed === "Certificate" && (
                <div className="mb-2">
                    <FormLabel>{t("elasticSearch.certificateFileLabel")}</FormLabel>
                    {isUploadCertificateVisible && (
                        <div>
                            <FormLabel className="btn btn-primary">
                                <Icon icon="upload" />
                                {t("elasticSearch.uploadCertificate")}
                                <input
                                    type="file"
                                    className="d-none"
                                    id="elasticCertificateFilePicker"
                                    onChange={(e) =>
                                        fileImporter.readAsBinaryString(e.currentTarget, (x) =>
                                            onCertificateUploaded(x)
                                        )
                                    }
                                />
                            </FormLabel>
                        </div>
                    )}
                    {formValues.certificatesBase64?.map((cert) => (
                        <ElasticSearchCertificate
                            key={cert}
                            certBase64={cert}
                            onDelete={() => deleteCertificate(cert)}
                        />
                    ))}
                    {formState.errors?.certificatesBase64 && (
                        <div className="text-danger small">{formState.errors.certificatesBase64.message}</div>
                    )}
                </div>
            )}
            <ConnectionStringUsedByTasks tasks={initialConnection.usedBy} connectionType={initialConnection.type} />
            {isServerWide && (
                <ExcludedDatabasesFormSelect
                    control={control}
                    name="excludedDatabases"
                    usedBy={initialConnection.usedBy}
                />
            )}
        </Form>
    );
}

interface NodeUrlProps {
    idx: number;
    control: any;
    formValues: FormData;
    isDeleteButtonVisible: boolean;
    onDelete: () => void;
    trigger: UseFormTrigger<FormData>;
}

function NodeUrl({ idx, control, formValues, isDeleteButtonVisible, onDelete, trigger }: NodeUrlProps) {
    const t = useStudioTranslation("connectionStrings");
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(`nodes.${idx}`);
        if (!isValid) {
            return;
        }

        const url = formValues.nodes[idx].url;
        const authDto = mapElasticSearchAuthenticationToDto(formValues);
        return isServerWide
            ? tasksService.testServerWideElasticSearchNodeConnection(url, authDto)
            : tasksService.testElasticSearchNodeConnection(databaseName, url, authDto);
    });

    return (
        <div className="vstack mb-2 gap-1">
            <FormLabel className="mb-0 d-flex align-items-center gap-1">
                <span className="small-label mb-0">{t("form.urlNumber", { number: idx + 1 })}</span>
                {asyncTest.result?.Success ? (
                    <Badge bg="success" pill>
                        <Icon icon="check" />
                        {t("form.successfullyConnected")}
                    </Badge>
                ) : asyncTest.result?.Error ? (
                    <Badge bg="danger" pill>
                        <Icon icon="warning" />
                        {t("form.failedConnection")}
                    </Badge>
                ) : null}
            </FormLabel>
            <div className="input-group">
                <FormInput
                    type="text"
                    control={control}
                    name={`nodes.${idx}.url`}
                    placeholder="http(s)://hostName"
                    autoComplete="off"
                />
                {isDeleteButtonVisible && (
                    <Button variant="danger" onClick={onDelete} disabled={asyncTest.loading}>
                        <Icon icon="trash" margin="m-0" title={t("form.deleteUrl")} />
                    </Button>
                )}
                <ButtonWithSpinner
                    variant="secondary"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                    icon={{
                        icon: "rocket",
                        title: t("form.testConnection"),
                    }}
                >
                    {t("form.testConnection")}
                </ButtonWithSpinner>
            </div>
            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}

function getAuthenticationOptions(t: StudioTranslate<"connectionStrings">): SelectOption[] {
    const labels: Record<ElasticSearchAuthenticationMethod, string> = {
        "No authentication": t("elasticSearch.auth.none"),
        Basic: t("elasticSearch.auth.basic"),
        "API Key": t("elasticSearch.auth.apiKey"),
        "Encoded API Key": t("elasticSearch.auth.encodedApiKey"),
        Certificate: t("elasticSearch.auth.certificate"),
    };

    return exhaustiveStringTuple<ElasticSearchAuthenticationMethod>()(
        "No authentication",
        "Basic",
        "API Key",
        "Encoded API Key",
        "Certificate"
    ).map((type: ElasticSearchAuthenticationMethod) => ({
        value: type,
        label: labels[type],
    }));
}

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    authMethodUsed: yup.string<ElasticSearchAuthenticationMethod>(),
    apiKey: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "API Key",
            then: (schema) => schema.required(),
        }),
    apiKeyId: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "API Key",
            then: (schema) => schema.required(),
        }),
    encodedApiKey: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Encoded API Key",
            then: (schema) => schema.required(),
        }),
    password: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Basic",
            then: (schema) => schema.required(),
        }),
    username: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Basic",
            then: (schema) => schema.required(),
        }),
    certificatesBase64: yup
        .array()
        .nullable()
        .of(yup.string())
        .when("authMethodUsed", {
            is: "Certificate",
            then: (schema) => schema.min(1),
        }),
    nodes: yup
        .array()
        .of(yup.object({ url: yup.string().basicUrl().nullable().required() }))
        .min(1),
    excludedDatabases: yup.array().of(yup.string()).optional(),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: any, isForNewConnection: any): FormData {
    if (isForNewConnection) {
        return {
            authMethodUsed: "No authentication",
            apiKey: null,
            apiKeyId: null,
            encodedApiKey: null,
            password: null,
            username: null,
            certificatesBase64: [],
            nodes: [{ url: null }],
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
