import React, { useEffect } from "react";
import {
    AzureServiceBusConnection,
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
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { useServices } from "components/hooks/useServices";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { mapAzureServiceBusConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<AzureServiceBusConnection>;

export interface AzureServiceBusConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AzureServiceBusConnection;
}

export default function AzureServiceBusConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AzureServiceBusConnectionStringProps) {
    const t = useStudioTranslation("connectionStrings");
    const usedNames = useAppSelector(connectionStringSelectors.connections)["AzureServiceBus"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(schema)(
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

        const dto = mapAzureServiceBusConnectionStringSettingsToDto(formValues);
        return isServerWide
            ? tasksService.testServerWideAzureServiceBusServerConnection(dto)
            : tasksService.testAzureServiceBusServerConnection(databaseName, dto);
    });

    useEffect(() => {
        asyncTest.set(null);
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AzureServiceBus",
            ...formData,
        } satisfies AzureServiceBusConnection);
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
    authMethod: AzureServiceBusAuthenticationType;
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
                    <div className="d-flex flex-grow align-items-baseline justify-content-between">
                        <FormLabel>{t("azureServiceBus.namespaceLabel")}</FormLabel>
                        <PopoverWithHoverWrapper
                            message={
                                <StudioTrans
                                    ns="connectionStrings"
                                    i18nKey="form.example"
                                    options={{ example: "mynamespace.servicebus.windows.net" }}
                                    components={{ code: <code /> }}
                                />
                            }
                        >
                            <small className="text-primary">
                                {t("azureServiceBus.namespaceFormat")} <Icon icon="help" margin="m-0" />
                            </small>
                        </PopoverWithHoverWrapper>
                    </div>
                    <FormInput
                        control={control}
                        name="settings.entraId.namespace"
                        type="text"
                        placeholder={t("azureServiceBus.namespacePlaceholder")}
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
            </div>
        );
    }

    if (authMethod === "passwordless") {
        return (
            <div className="mb-2">
                <div className="d-flex flex-grow align-items-baseline justify-content-between">
                    <FormLabel>{t("azureServiceBus.namespaceLabel")}</FormLabel>
                    <PopoverWithHoverWrapper
                        message={
                            <StudioTrans
                                ns="connectionStrings"
                                i18nKey="form.example"
                                options={{ example: "mynamespace.servicebus.windows.net" }}
                                components={{ code: <code /> }}
                            />
                        }
                    >
                        <small className="text-primary">
                            {t("azureServiceBus.namespaceFormat")} <Icon icon="help" margin="m-0" />
                        </small>
                    </PopoverWithHoverWrapper>
                </div>
                <FormInput
                    control={control}
                    name="settings.passwordless.namespace"
                    type="text"
                    placeholder={t("azureServiceBus.namespacePlaceholder")}
                />
            </div>
        );
    }

    assertUnreachable(authMethod);
}

function getAuthenticationOptions(
    t: StudioTranslate<"connectionStrings">
): SelectOption<AzureServiceBusAuthenticationType>[] {
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

function getStringRequiredSchema(authType: AzureServiceBusAuthenticationType) {
    return yup
        .string()
        .nullable()
        .when("$authType", {
            is: authType,
            then: (schema) => schema.required(),
        });
}

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    authType: yup.string<AzureServiceBusAuthenticationType>(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
    settings: yupObjectSchema<FormData["settings"]>({
        connectionString: yupObjectSchema<FormData["settings"]["connectionString"]>({
            connectionStringValue: yup
                .string()
                .nullable()
                .when("$authType", {
                    is: "connectionString",
                    then: (schema) => schema.required(),
                }),
        }),
        entraId: yupObjectSchema<FormData["settings"]["entraId"]>({
            namespace: getStringRequiredSchema("entraId"),
            tenantId: getStringRequiredSchema("entraId"),
            clientId: getStringRequiredSchema("entraId"),
            clientSecret: getStringRequiredSchema("entraId"),
        }),
        passwordless: yupObjectSchema<FormData["settings"]["passwordless"]>({
            namespace: getStringRequiredSchema("passwordless"),
        }),
    }),
});

function getDefaultValues(initialConnection: AzureServiceBusConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "connectionString",
            settings: {
                connectionString: {
                    connectionStringValue: null,
                },
                entraId: {
                    namespace: null,
                    tenantId: null,
                    clientId: null,
                    clientSecret: null,
                },
                passwordless: {
                    namespace: null,
                },
            },
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}

const exampleConnectionString =
    "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=stub";
