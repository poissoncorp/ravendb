import React, { useEffect } from "react";
import {
    AzureQueueStorageConnection,
    ConnectionFormData,
    EditConnectionStringFormProps,
} from "../connectionStringsTypes";
import { SelectOption } from "components/common/select/Select";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { yupObjectSchema } from "components/utils/yupUtils";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionStringUsedByTasks from "components/pages/database/settings/connectionStrings/editForms/shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "components/pages/database/settings/connectionStrings/editForms/shared/ExcludedDatabasesFormSelect";
import { useServices } from "components/hooks/useServices";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { mapAzureQueueStorageConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<AzureQueueStorageConnection>;

export interface AzureQueueStorageConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AzureQueueStorageConnection;
}

export default function AzureQueueStorageConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AzureQueueStorageConnectionStringProps) {
    const t = useStudioTranslation("connectionStrings");
    const usedNames = useAppSelector(connectionStringSelectors.connections)["AzureQueueStorage"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(getSchema(t))(
                data,
                {
                    authType: data.authType,
                    isForNewConnection,
                    usedNames,
                } satisfies ConnectionStringsNameContext & { authType: FormData["authType"] },
                options
            ),
    });

    const formValues = useWatch({ control });
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(`settings.${formValues.authType}`);
        if (!isValid) {
            return;
        }

        const dto = mapAzureQueueStorageConnectionStringSettingsToDto(formValues);
        return isServerWide
            ? tasksService.testServerWideAzureQueueStorageServerConnection(dto)
            : tasksService.testAzureQueueStorageServerConnection(databaseName, dto);
    });

    // Clear test result after changing auth type
    useEffect(() => {
        asyncTest.set(null);
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AzureQueueStorage",
            ...formData,
        } satisfies AzureQueueStorageConnection);
    };

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
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("form.authenticationLabel")}{" "}
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
                <FormSelect
                    name="authType"
                    control={control}
                    placeholder={t("form.authenticationPlaceholder")}
                    options={getAuthenticationOptions(t)}
                    isSearchable={false}
                />
            </div>
            <SelectedAuthFields control={control} authMethod={formValues.authType} />

            <div className="mb-2">
                <ButtonWithSpinner
                    variant="secondary"
                    icon="rocket"
                    title={t("form.testConnection")}
                    className="mb-2"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                    disabled={asyncTest.loading}
                >
                    {t("form.testConnection")}
                </ButtonWithSpinner>
            </div>
            {asyncTest.result?.Error && (
                <div className="mb-2">
                    <ConnectionTestResult testResult={asyncTest.result} />
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

interface SelectedAuthFieldsProps {
    control: Control<FormData>;
    authMethod: AzureQueueStorageAuthenticationType;
}

function SelectedAuthFields({ control, authMethod }: SelectedAuthFieldsProps) {
    const t = useStudioTranslation("connectionStrings");

    if (authMethod === "connectionString") {
        return (
            <div className="mb-2">
                <div className="d-flex flex-grow align-items-baseline justify-content-between">
                    <FormLabel>{t("form.connectionStringLabel")}</FormLabel>
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="form.example"
                                options={{ example: exampleConnectionString }}
                                components={{ code: <code /> }}
                            />
                        }
                    >
                        <small className="text-primary">
                            {t("form.syntax")} <Icon icon="help" margin="m-0" />
                        </small>
                    </PopoverWithHoverWrapper>
                </div>
                <FormInput
                    control={control}
                    name="settings.connectionString.connectionStringValue"
                    type="textarea"
                    as="textarea"
                    placeholder={t("form.connectionStringPlaceholder")}
                    rows={5}
                />
            </div>
        );
    }

    if (authMethod === "entraId") {
        return (
            <div className="vstack gap-3">
                <div className="mb-2">
                    <FormLabel>{t("azure.clientIdLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientId"
                        type="text"
                        placeholder={t("azure.clientIdPlaceholder")}
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>{t("azure.clientSecretLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientSecret"
                        type="password"
                        placeholder={t("azure.clientSecretPlaceholder")}
                        passwordPreview
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>{t("azureQueueStorage.storageAccountNameLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.storageAccountName"
                        type="text"
                        placeholder={t("azureQueueStorage.storageAccountNamePlaceholder")}
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>{t("azure.tenantIdLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.tenantId"
                        type="text"
                        placeholder={t("azure.tenantIdPlaceholder")}
                    />
                </div>
            </div>
        );
    }

    if (authMethod === "passwordless") {
        return (
            <div className="mb-2">
                <FormLabel>{t("azureQueueStorage.storageAccountNameLabel")}</FormLabel>
                <FormInput
                    control={control}
                    name="settings.passwordless.storageAccountName"
                    type="text"
                    placeholder={t("azureQueueStorage.storageAccountNamePlaceholder")}
                />
            </div>
        );
    }

    assertUnreachable(authMethod);
}

function getAuthenticationOptions(
    t: StudioTranslate<"connectionStrings">
): SelectOption<AzureQueueStorageAuthenticationType>[] {
    return [
        {
            value: "connectionString",
            label: t("azure.authConnectionString"),
        },
        {
            value: "entraId",
            label: "Entra ID",
        },
        {
            value: "passwordless",
            label: t("azure.authPasswordless"),
        },
    ];
}

function getStringRequiredSchema(authType: AzureQueueStorageAuthenticationType) {
    return yup
        .string()
        .nullable()
        .when("$authType", {
            is: authType,
            then: (schema) => schema.required(),
        });
}

function getSchema(t: StudioTranslate<"connectionStrings">) {
    return yupObjectSchema<FormData>({
        name: connectionStringsUtils.nameSchema,
        authType: yup.string<AzureQueueStorageAuthenticationType>(),
        excludedDatabases: yup.array().of(yup.string()).optional(),
        settings: yupObjectSchema<FormData["settings"]>({
            connectionString: yupObjectSchema<FormData["settings"]["connectionString"]>({
                connectionStringValue: yup
                    .string()
                    .nullable()
                    .when("$authType", {
                        is: "connectionString",
                        then: (schema) =>
                            schema
                                .required()
                                .test(
                                    "connectionString",
                                    t("azureQueueStorage.connectionStringRequiredFields"),
                                    (value) => {
                                        return (
                                            value.includes("DefaultEndpointsProtocol") &&
                                            value.includes("AccountName") &&
                                            value.includes("AccountKey")
                                        );
                                    }
                                ),
                    }),
            }),
            entraId: yupObjectSchema<FormData["settings"]["entraId"]>({
                clientId: getStringRequiredSchema("entraId"),
                clientSecret: getStringRequiredSchema("entraId"),
                storageAccountName: getStringRequiredSchema("entraId"),
                tenantId: getStringRequiredSchema("entraId"),
            }),
            passwordless: yupObjectSchema<FormData["settings"]["passwordless"]>({
                storageAccountName: getStringRequiredSchema("passwordless"),
            }),
        }),
    });
}

function getDefaultValues(initialConnection: AzureQueueStorageConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "connectionString",
            settings: {
                connectionString: {
                    connectionStringValue: null,
                },
                entraId: {
                    clientId: null,
                    clientSecret: null,
                    storageAccountName: null,
                    tenantId: null,
                },
                passwordless: {
                    storageAccountName: null,
                },
            },
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}

const exampleConnectionString =
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;";
