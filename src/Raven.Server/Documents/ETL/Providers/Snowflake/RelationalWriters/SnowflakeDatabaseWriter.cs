﻿using System;
using System.Data;
using System.Data.Common;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Server.Documents.ETL.Relational;
using Raven.Server.Documents.ETL.Relational.Metrics;
using Raven.Server.Documents.ETL.Relational.RelationalWriters;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Snowflake.Data.Client;
using Sparrow.Json;
using DbCommandBuilder = Raven.Server.Documents.ETL.Relational.RelationalWriters.DbCommandBuilder;

namespace Raven.Server.Documents.ETL.Providers.Snowflake.RelationalWriters;

public class SnowflakeDatabaseWriter: RelationalDatabaseWriterBase<SnowflakeConnectionString, SnowflakeEtlConfiguration>
{
    private readonly string SnowflakeEtlTag = "Snowflake ETL";
    
    private readonly SnowflakeEtlConfiguration _snowflakeEtlConfiguration;
    public SnowflakeDatabaseWriter(DocumentDatabase database, SnowflakeEtlConfiguration configuration, RelationalDatabaseEtlMetricsCountersManager sqlMetrics, EtlProcessStatistics statistics) : base(database, configuration, sqlMetrics, statistics)
    {
        _snowflakeEtlConfiguration = configuration;
        ParametrizeDeletes = configuration.ParameterizeDeletes; // todo: remove
    }

    public override bool ParametrizeDeletes { get; }

    protected override string GetConnectionString(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return configuration.Connection.ConnectionString;
    }

    protected override DbCommandBuilder GetInitializedCommandBuilder()
    {
        return new DbCommandBuilder { Start =  string.Empty, End = string.Empty };
    }

    protected override DbProviderFactory GetDbProviderFactory(EtlConfiguration<SnowflakeConnectionString> configuration)
    {
        return SnowflakeDbFactory.Instance;
    }

    protected override void CreateAlertCannotOpenConnection(DocumentDatabase database, string etlConfigurationName, string connectionStringName, Exception e)
    {
        database.NotificationCenter.Add(AlertRaised.Create(
            database.Name,
            SnowflakeEtlTag,
            $"[{etlConfigurationName}] Could not open connection using '{connectionStringName}' connection string",
            AlertType.SnowflakeEtl_ConnectionError,
            NotificationSeverity.Error,
            key: $"{etlConfigurationName}/{connectionStringName}",
            details: new ExceptionDetails(e)));

    }

    protected override int? GetCommandTimeout()
    {
        return _snowflakeEtlConfiguration.CommandTimeout;
    }

    protected override bool ShouldQuoteTables()
    {
        return false;
    }

    protected override string GetParameterNameForDbParameter(string paramName)
    {
        return paramName;
    }

    protected override string GetParameterNameForCommandString(string targetParamName, bool parseJson)
    {
        return parseJson ? $"PARSE_JSON(:{targetParamName})" : $":{targetParamName}";
    }

    protected override void EnsureParamTypeSupportedByDbProvider(DbParameter parameter)
    {
        if (parameter.DbType == DbType.AnsiString)
            parameter.DbType = DbType.String;
    }

    protected override void SetPrimaryKeyParamValue(ToRelationalDatabaseItem itemToReplicate, DbParameter pkParam)
    {
        pkParam.Value = itemToReplicate.DocumentId.ToString();
        EnsureParamTypeSupportedByDbProvider(pkParam);
    }

    protected override string GetPostInsertIntoStartSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return "SELECT ";
    }

    protected override string GetPostInsertIntoEndSyntax(ToRelationalDatabaseItem itemToReplicate)
    {
        return string.Empty;
    }

    protected override string GetPostDeleteSyntax(ToRelationalDatabaseItem itemToDelete)
    {
        return string.Empty;
    }

    protected override void HandleCustomDbTypeObject(DbParameter colParam, RelationalDatabaseColumn column, object dbType, object fieldValue, BlittableJsonReaderObject objectValue)
    {
        column.IsArrayOrObject = true;
        var dbTypeString = dbType.ToString() ?? string.Empty;
        colParam.Value = dbTypeString switch
        {
            "Array" when fieldValue is BlittableJsonReaderArray bjrav => bjrav.ToString(),
            "Object" when fieldValue is BlittableJsonReaderObject bjro => bjro.ToString(),
            _ => throw new NotSupportedException($"Type {dbTypeString} isn't currently supported by Snowflake ETL.")
        };
    }
}