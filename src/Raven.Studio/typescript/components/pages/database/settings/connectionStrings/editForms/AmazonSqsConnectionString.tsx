import React, { useEffect } from "react";
import { AmazonSqsConnection, ConnectionFormData, EditConnectionStringFormProps } from "../connectionStringsTypes";
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
import { Icon } from "components/common/Icon";
import { mapAmazonSqsConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import type { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<AmazonSqsConnection>;

export interface AmazonSqsConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AmazonSqsConnection;
}

export default function AmazonSqsConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AmazonSqsConnectionStringProps) {
    const t = useStudioTranslation("connectionStrings");
    const usedNames = useAppSelector(connectionStringSelectors.connections)["AmazonSqs"].map((x) => x.name);
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

        const dto = mapAmazonSqsConnectionStringSettingsToDto(formValues);
        return isServerWide
            ? tasksService.testServerWideAmazonSqsServerConnection(dto)
            : tasksService.testAmazonSqsServerConnection(databaseName, dto);
    });

    // Clear test result after changing auth type
    useEffect(() => {
        asyncTest.set(null);
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AmazonSqs",
            ...formData,
        } satisfies AmazonSqsConnection);
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
    authMethod: AmazonSqsAuthenticationType;
}

function SelectedAuthFields({ control, authMethod }: SelectedAuthFieldsProps) {
    const t = useStudioTranslation("connectionStrings");

    if (authMethod === "basic") {
        return (
            <div className="vstack gap-3">
                <div className="mb-2">
                    <FormLabel>{t("amazonSqs.accessKeyLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.accessKey"
                        type="text"
                        placeholder={t("amazonSqs.accessKeyPlaceholder")}
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>{t("amazonSqs.secretKeyLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.secretKey"
                        type="password"
                        placeholder={t("amazonSqs.secretKeyPlaceholder")}
                        passwordPreview
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>{t("amazonSqs.regionNameLabel")}</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.regionName"
                        type="text"
                        placeholder={t("amazonSqs.regionNamePlaceholder")}
                    />
                </div>
            </div>
        );
    }

    return null;
}

function getAuthenticationOptions(
    t: StudioTranslate<"connectionStrings">
): SelectOption<AmazonSqsAuthenticationType>[] {
    return [
        {
            value: "basic",
            label: t("amazonSqs.authBasic"),
        },
        {
            value: "passwordless",
            label: t("amazonSqs.authPasswordless"),
        },
    ];
}

function getStringRequiredSchema(authType: AmazonSqsAuthenticationType) {
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
    authType: yup.string<AmazonSqsAuthenticationType>(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
    settings: yupObjectSchema<FormData["settings"]>({
        basic: yupObjectSchema<FormData["settings"]["basic"]>({
            accessKey: getStringRequiredSchema("basic"),
            secretKey: getStringRequiredSchema("basic"),
            regionName: getStringRequiredSchema("basic"),
        }),
        passwordless: yup.boolean(),
    }),
});

function getDefaultValues(initialConnection: AmazonSqsConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "basic",
            settings: {
                basic: {
                    accessKey: null,
                    secretKey: null,
                    regionName: null,
                },
                passwordless: false,
            },
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
