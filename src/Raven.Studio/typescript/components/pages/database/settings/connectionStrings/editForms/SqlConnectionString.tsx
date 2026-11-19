import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";

import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { OptionWithWarning, SelectOptionWithWarning } from "components/common/select/Select";
import { ConnectionFormData, EditConnectionStringFormProps, SqlConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { useAsyncCallback } from "react-async-hook";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import { Icon } from "components/common/Icon";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { yupObjectSchema } from "components/utils/yupUtils";
import { FlexGrow } from "components/common/FlexGrow";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import RichAlert from "components/common/RichAlert";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { withNestedSubmit } from "components/utils/common";
import { useStudioTranslation } from "hooks/useStudioTranslation";
import { StudioTrans } from "components/common/i18n/StudioTrans";
import { StudioTranslate } from "common/i18n/resources";

type FormData = ConnectionFormData<SqlConnection>;

export interface SqlConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: SqlConnection;
}

export default function SqlConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: SqlConnectionStringProps) {
    const t = useStudioTranslation("connectionStrings");
    const usedNames = useAppSelector(connectionStringSelectors.connections)["Sql"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
        context: {
            isForNewConnection,
            usedNames,
        } satisfies ConnectionStringsNameContext,
    });

    const formValues = useWatch({ control });
    const { tasksService } = useServices();
    const [syntaxHelpElement, setSyntaxHelpElement] = useState<HTMLElement>();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(["connectionString", "factoryName"]);
        if (!isValid) {
            return;
        }

        return isServerWide
            ? tasksService.testServerWideSqlConnectionString(formValues.connectionString, formValues.factoryName)
            : tasksService.testSqlConnectionString(databaseName, formValues.connectionString, formValues.factoryName);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Sql",
            ...formData,
        } satisfies SqlConnection);
    };

    return (
        <Form
            id="connection-string-form"
            onSubmit={withNestedSubmit(handleSubmit(handleSave))}
            className="vstack gap-3"
        >
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
                <div className="d-flex flex-grow align-items-baseline justify-content-between">
                    <FormLabel>{t("sql.factoryLabel")}</FormLabel>
                    {formValues.factoryName && (
                        <>
                            <small ref={setSyntaxHelpElement} className="text-primary">
                                {t("form.syntax")} <Icon icon="help" margin="m-0" />
                            </small>
                            <PopoverWithHover target={syntaxHelpElement}>
                                <div className="p-2">{getSyntaxHelp(formValues.factoryName)}</div>
                            </PopoverWithHover>
                        </>
                    )}
                </div>
                <FormSelect
                    control={control}
                    name="factoryName"
                    options={getSqlFactoryOptions(t)}
                    placeholder={t("sql.factoryPlaceholder")}
                    isSearchable={false}
                    components={{ Option: OptionWithWarning }}
                />
                {formValues.factoryName === "MySql.Data.MySqlClient" && (
                    <RichAlert variant="warning" className="mt-1">
                        {t("sql.mySqlDeprecated")}
                    </RichAlert>
                )}
                {formValues.factoryName === "System.Data.SqlClient" && (
                    <RichAlert variant="warning" className="mt-1">
                        {t("sql.sqlClientDeprecated")}
                    </RichAlert>
                )}
            </div>
            <div className="mb-2">
                <FormLabel className="d-flex align-items-center gap-1">
                    {t("form.connectionStringLabel")}{" "}
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
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    as="textarea"
                    placeholder={getConnectionStringPlaceholder(formValues.factoryName, t)}
                    rows={3}
                    autoComplete="off"
                />
                <div className="d-flex mt-4">
                    <FlexGrow />
                    <ButtonWithSpinner
                        variant="secondary"
                        icon="rocket"
                        onClick={asyncTest.execute}
                        isSpinning={asyncTest.loading}
                    >
                        {t("form.testConnection")}
                    </ButtonWithSpinner>
                </div>
            </div>
            <ConnectionStringUsedByTasks tasks={initialConnection.usedBy} connectionType={initialConnection.type} />
            {isServerWide && (
                <ExcludedDatabasesFormSelect
                    control={control}
                    name="excludedDatabases"
                    usedBy={initialConnection.usedBy}
                />
            )}
            {asyncTest.result?.Error && <ConnectionTestResult testResult={asyncTest.result} />}
        </Form>
    );
}

function getSqlFactoryOptions(
    t: StudioTranslate<"connectionStrings">
): SelectOptionWithWarning<SqlConnectionStringFactoryName>[] {
    return [
        { value: "Microsoft.Data.SqlClient", label: "Microsoft SQL Server (System.Data.Microsoft)" },
        { value: "MySqlConnector.MySqlConnectorFactory", label: "MySQL Server (MySqlConnector.MySqlConnectorFactory)" },
        { value: "Npgsql", label: "PostgreSQL (Npgsql)" },
        { value: "Oracle.ManagedDataAccess.Client", label: "Oracle Database (Oracle.ManagedDataAccess.Client)" },
        {
            value: "MySql.Data.MySqlClient",
            label: t("sql.deprecatedFactory", { factory: "MySQL Server (MySql.Data.MySqlClient)" }),
            isWarning: true,
        },
        {
            value: "System.Data.SqlClient",
            label: t("sql.deprecatedFactory", { factory: "Microsoft SQL Server (System.Data.SqlClient)" }),
            isWarning: true,
        },
    ];
}

function getSyntaxHelp(factory: SqlConnectionStringFactoryName) {
    switch (factory) {
        case "Microsoft.Data.SqlClient":
        case "System.Data.SqlClient":
            return (
                <SyntaxHelp
                    example="Data Source=10.0.0.107;Database=SourceDB;User ID=sa;Password=secret;"
                    link="https://ravendb.net/l/38S9OQ"
                />
            );
        case "MySql.Data.MySqlClient":
        case "MySqlConnector.MySqlConnectorFactory":
            return (
                <SyntaxHelp
                    example="server=10.0.0.103;port=3306;userid=root;password=secret;"
                    link="https://ravendb.net/l/BSS8YH"
                />
            );
        case "Npgsql":
            return (
                <SyntaxHelp
                    example="Host=10.0.0.105;Port=5432;Username=postgres;Password=secret"
                    link="https://ravendb.net/l/FWEBWD"
                />
            );
        case "Oracle.ManagedDataAccess.Client":
            return (
                <SyntaxHelp
                    example="Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.101)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)));User Id=SYS;DBA Privilege=SYSDBA;password=secret;"
                    link="https://ravendb.net/l/TG851N"
                />
            );
        default:
            return null;
    }
}

function SyntaxHelp({ example, link }: { example: string; link: string }) {
    return (
        <span>
            <StudioTrans
                ns="connectionStrings"
                i18nKey="form.example"
                options={{ example }}
                components={{ code: <code /> }}
            />
            <br />
            <span>
                <StudioTrans
                    ns="connectionStrings"
                    i18nKey="sql.moreExamples"
                    components={{
                        a: <a href={link} target="_blank" />,
                        icon: <Icon icon="link" margin="ms-1" />,
                    }}
                />
            </span>
        </span>
    );
}

function getConnectionStringPlaceholder(
    factoryName: SqlConnectionStringFactoryName,
    t: StudioTranslate<"connectionStrings">
) {
    const optionLabel = getSqlFactoryOptions(t).find((x) => x.value === factoryName)?.label;

    return optionLabel
        ? t("sql.connectionStringForFactoryPlaceholder", { factory: optionLabel })
        : t("sql.connectionStringPlaceholder");
}

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    connectionString: yup.string().nullable().required(),
    factoryName: yup.string<SqlConnectionStringFactoryName>().nullable().required(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: SqlConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            factoryName: null,
            connectionString: null,
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
